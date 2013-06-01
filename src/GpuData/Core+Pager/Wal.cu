// wal.c
#include "Core+Pager.cu.h"
#include <stddef.h>
using namespace Core;

namespace Core
{

#if defined(TEST) && defined(_DEBUG)
	bool _walTrace = false;
#define WALTRACE(X)  if (_walTrace) printf(X)
#else
#define WALTRACE(X)
#endif

#define WAL_MAX_VERSION      3007000
#define WALINDEX_MAX_VERSION 3007000
#define WAL_WRITE_LOCK         0
#define WAL_ALL_BUT_WRITE      1
#define WAL_CKPT_LOCK          1
#define WAL_RECOVER_LOCK       2
#define WAL_READ_LOCK(I)       (3 + (I))
#define WAL_NREADER            (VFile::SHM::SHM_MAX - 3)

	typedef struct WalIndexHeader WalIndexHeader;
	typedef struct WalIterator WalIterator;
	typedef struct WalCheckpointInfo WalCheckpointInfo;

	struct WalIndexHeader
	{
		uint32 Version;                 // Wal-index version
		uint32 Unused;					// Unused (padding) field
		uint32 Change;                  // Counter incremented each transaction
		bool IsInit;					// 1 when initialized
		bool BigEndianChecksum;			// True if checksums in WAL are big-endian
		uint16 SizePage;                // Database page size in bytes. 1==64K
		uint32 MaxFrame;                // Index of last valid frame in the WAL
		uint32 Pages;                   // Size of database in pages
		uint32 FrameChecksum[2];		// Checksum of last frame in log
		uint32 Salt[2];					// Two salt values copied from WAL header
		uint32 Checksum[2];				// Checksum over all prior fields
	};

	struct WalCheckpointInfo
	{
		uint32 Backfills;               // Number of WAL frames backfilled into DB
		uint32 ReadMarks[WAL_NREADER];  // Reader marks
	};

#define READMARK_NOT_USED 0xffffffff
#define WALINDEX_LOCK_OFFSET (sizeof(WalIndexHeader)*2 + sizeof(WalCheckpointInfo))
#define WALINDEX_LOCK_RESERVED 16
#define WALINDEX_HDR_SIZE (WALINDEX_LOCK_OFFSET+WALINDEX_LOCK_RESERVED)
#define WAL_FRAME_HDRSIZE 24
#define WAL_HDRSIZE 32
#define WAL_MAGIC 0x377f0682
#define walFrameOffset(Frame, SizePage) (WAL_HDRSIZE + ((Frame) - 1) * (int64)((SizePage) + WAL_FRAME_HDRSIZE))

	struct Wal
	{
		VFileSystem *Vfs;				// The VFS used to create pDbFd
		VFile *DBFile;					// File handle for the database file
		VFile *WalFile;					// File handle for WAL file
		uint32 Callback;				// Value to pass to log callback (or 0)
		int64 MaxWalSize;				// Truncate WAL to this size upon reset
		int SizeFirstBlock;				// Size of first block written to WAL file
		//int nWiData;					// Size of array apWiData
		volatile uint32 **WiData;		// Pointer to wal-index content in memory */
		uint32 SizePage;                // Database page size
		int16 ReadLock;					// Which read lock is being held.  -1 for none
		uint8 SyncFlags;				// Flags to use to sync header writes
		uint8 ExclusiveMode;			// Non-zero if connection is in exclusive mode
		uint8 WriteLock;				// True if in a write transaction
		uint8 CheckpointLock;           // True if holding a checkpoint lock
		uint8 ReadOnly;					// WAL_RDWR, WAL_RDONLY, or WAL_SHM_RDONLY
		uint8 TruncateOnCommit;			// True to truncate WAL file on commit
		uint8 SyncHeader;				// Fsync the WAL header if true
		uint8 PadToSectorBoundary;		// Pad transactions out to the next sector
		WalIndexHeader Header;			// Wal-index header for current transaction
		const char *WalName;			// Name of WAL file
		uint32 Checkpoints;				// Checkpoint sequence counter in the wal-header
#ifdef _DEBUG
		uint8 LockError;				// True if a locking error has occurred
#endif
	};

#define WAL_NORMAL_MODE     0
#define WAL_EXCLUSIVE_MODE  1     
#define WAL_HEAPMEMORY_MODE 2

#define WAL_RDWR        0    // Normal read/write connection
#define WAL_RDONLY      1    // The WAL file is readonly
#define WAL_SHM_RDONLY  2    // The SHM file is readonly

	// Each page of the wal-index mapping contains a hash-table made up of an array of HASHTABLE_NSLOT elements of the following type.
	typedef uint16 ht_slot;

	//
	// This structure is used to implement an iterator that loops through all frames in the WAL in database page order. Where two or more frames
	// correspond to the same database page, the iterator visits only the frame most recently written to the WAL (in other words, the frame with
	// the largest index).
	//
	// The internals of this structure are only accessed by:
	//
	//   walIteratorInit() - Create a new iterator,
	//   walIteratorNext() - Step an iterator,
	//   walIteratorFree() - Free an iterator.
	//
	// This functionality is used by the checkpoint code (see walCheckpoint()).
	struct WalIterator
	{
		int Prior;						// Last result returned from the iterator
		int SegmentsLength;             // Number of entries in aSegment[]
		struct WalSegment
		{
			int Next;					// Next slot in aIndex[] not yet returned
			ht_slot *Indexs;            // i0, i1, i2... such that aPgno[iN] ascend
			uint32 *IDs;				// Array of page numbers.
			int Entrys;                 // Nr. of entries in aPgno[] and aIndex[]
			int Zero;					// Frame number associated with aPgno[0]
		} Segments[1];					// One for every 32KB page in the wal-index
	};

	//
	// Define the parameters of the hash tables in the wal-index file. There is a hash-table following every HASHTABLE_NPAGE page numbers in the
	// wal-index.
	//
	// Changing any of these constants will alter the wal-index format and create incompatibilities.
#define HASHTABLE_NPAGE      4096                 // Must be power of 2
#define HASHTABLE_HASH_1     383                  // Should be prime
#define HASHTABLE_NSLOT      (HASHTABLE_NPAGE*2)  // Must be a power of 2

	// The block of page numbers associated with the first hash-table in a wal-index is smaller than usual. This is so that there is a complete
	// hash-table on each aligned 32KB page of the wal-index.
#define HASHTABLE_NPAGE_ONE (HASHTABLE_NPAGE - (WALINDEX_HDR_SIZE / sizeof(uint32)))

	// The wal-index is divided into pages of WALINDEX_PGSZ bytes each.
#define WALINDEX_PGSZ   (sizeof(ht_slot) * HASHTABLE_NSLOT + HASHTABLE_NPAGE * sizeof(uint32))

	static RC walIndexPage(Wal *wal, Pid id, volatile Pid **idOut)
	{
		// Enlarge the pWal->apWiData[] array if required
		if (__arrayLength(wal->WiData) <= id)
		{
			int bytes = sizeof(uint32 *) * (id + 1);
			volatile uint32 **newWiData = (volatile uint32 **)SysEx::Realloc((void *)wal->WiData, bytes);
			if (!newWiData)
			{
				*idOut = nullptr;
				return RC::NOMEM;
			}
			_memset((void *)&newWiData[__arrayLength(wal->WiData)], 0, sizeof(uint32 *) * (id + 1 - __arrayLength(wal->WiData)));
			__arraySet(wal->WiData, newWiData, id + 1);
		}

		// Request a pointer to the required page from the VFS
		RC rc = RC::OK;
		if (wal->WiData[id] == 0)
		{
			if (wal->ExclusiveMode == WAL_HEAPMEMORY_MODE)
			{
				wal->WiData[id] = (uint32 volatile *)SysEx::Alloc(WALINDEX_PGSZ, true);
				if (!wal->WiData[id]) rc = RC::NOMEM;
			}
			else
			{
				rc = wal->DBFile->ShmMap(id, WALINDEX_PGSZ, wal->WriteLock, (void volatile **)&wal->WiData[id]);
				if (rc == RC::READONLY)
				{
					wal->ReadOnly |= WAL_SHM_RDONLY;
					rc = RC::OK;
				}
			}
		}

		*idOut = wal->WiData[id];
		_assert(id == 0 || *idOut || rc != RC::OK);
		return rc;
	}

	static volatile WalCheckpointInfo *walCkptInfo(Wal *wal)
	{
		_assert(__arrayLength(wal->WiData) > 0 && wal->WiData[0]);
		return (volatile WalCheckpointInfo *) & (wal->WiData[0][sizeof(WalIndexHeader) / 2]);
	}

	static volatile WalIndexHeader *walIndexHeader(Wal *wal)
	{
		_assert(__arrayLength(wal->WiData) > 0 && wal->WiData[0]);
		return (volatile WalIndexHeader *)wal->WiData[0];
	}

#define BYTESWAP32(x) ((((x)&0x000000FF)<<24) + (((x)&0x0000FF00)<<8) + (((x)&0x00FF0000)>>8)  + (((x)&0xFF000000)>>24))

	static void walChecksumBytes(bool nativeChecksum,  uint8 *b, int length, const uint32 *checksum, uint32 *checksumOut)
	{
		uint32 s1, s2;
		if (checksum)
		{
			s1 = checksum[0];
			s2 = checksum[1];
		}
		else
			s1 = s2 = 0;

		_assert(length >= 8);
		_assert((length & 0x00000007) == 0);

		uint32 *data = (uint32 *)b;
		uint32 *end = (uint32 *)&b[length];
		if (nativeChecksum)
		{
			do
			{
				s1 += *data++ + s2;
				s2 += *data++ + s1;
			} while (data < end);
		}
		else
		{
			do
			{
				s1 += BYTESWAP32(data[0]) + s2;
				s2 += BYTESWAP32(data[1]) + s1;
				data += 2;
			} while (data < end);
		}

		checksumOut[0] = s1;
		checksumOut[1] = s2;
	}

	static void walShmBarrier(Wal *wal)
	{
		if (wal->ExclusiveMode != WAL_HEAPMEMORY_MODE)
			wal->DBFile->ShmBarrier();
	}

	static void walIndexWriteHdr(Wal *wal)
	{
		volatile WalIndexHeader *header = walIndexHeader(wal);
		const int checksumIdx = offsetof(WalIndexHeader, Checksum);

		_assert(wal->WriteLock);
		wal->Header.IsInit = true;
		wal->Header.Version = WALINDEX_MAX_VERSION;
		walChecksumBytes(1, (uint8 *)&wal->Header, checksumIdx, 0, wal->Header.Checksum);
		_memcpy((void *)&header[1], (void *)&wal->Header, sizeof(WalIndexHeader));
		walShmBarrier(wal);
		_memcpy((void *)&header[0], (void *)&wal->Header, sizeof(WalIndexHeader));
	}

	static void walEncodeFrame(Wal *wal, Pid id, uint32 truncate, uint8 *data, uint8 *frame)
	{
		uint32 *checksum = wal->Header.FrameChecksum;
		_assert(WAL_FRAME_HDRSIZE == 24);
		ConvertEx::Put4(&frame[0], id);
		ConvertEx::Put4(&frame[4], truncate);
		_memcpy(&frame[8], wal->Header.Salt, 8);

		bool nativeChecksum = (wal->Header.BigEndianChecksum == TYPE_BIGENDIAN); // True for native byte-order checksums
		walChecksumBytes(nativeChecksum, frame, 8, checksum, checksum);
		walChecksumBytes(nativeChecksum, data, wal->SizePage, checksum, checksum);

		ConvertEx::Put4(&frame[16], checksum[0]);
		ConvertEx::Put4(&frame[20], checksum[1]);
	}

	static int walDecodeFrame(Wal *wal, Pid *idOut, uint32 *truncateOut, uint8 *data, uint8 *frame)
	{
		uint32 *checksum = wal->Header.FrameChecksum;
		_assert(WAL_FRAME_HDRSIZE == 24);

		// A frame is only valid if the salt values in the frame-header match the salt values in the wal-header. 
		if (_memcmp(&wal->Header.Salt, &frame[8], 8) != 0)
			return false;

		// A frame is only valid if the page number is creater than zero.
		Pid id = ConvertEx::Get4(&frame[0]); // Page number of the frame
		if (id == 0)
			return false;

		// A frame is only valid if a checksum of the WAL header, all prior frams, the first 16 bytes of this frame-header, 
		// and the frame-data matches the checksum in the last 8 bytes of this frame-header.
		bool nativeChecksum = (wal->Header.BigEndianChecksum == TYPE_BIGENDIAN); // True for native byte-order checksums
		walChecksumBytes(nativeChecksum, frame, 8, checksum, checksum);
		walChecksumBytes(nativeChecksum, data, wal->SizePage, checksum, checksum);
		if (checksum[0] != ConvertEx::Get4(&frame[16]) || checksum[1]!=ConvertEx::Get4(&frame[20])) // Checksum failed.
			return false;

		// If we reach this point, the frame is valid.  Return the page number and the new database size.
		*idOut = id;
		*truncateOut = ConvertEx::Get4(&frame[4]);
		return true;
	}

#if defined(TEST) && defined(_DEBUG)
	static const char *walLockName(int lockIdx)
	{
		if (lockIdx == WAL_WRITE_LOCK)
			return "WRITE-LOCK";
		else if (lockIdx == WAL_CKPT_LOCK)
			return "CKPT-LOCK";
		else if (lockIdx == WAL_RECOVER_LOCK)
			return "RECOVER-LOCK";
		else
		{
			static char name[15];
			_snprintf(name, sizeof(name), "READ-LOCK[%d]", lockIdx - WAL_READ_LOCK(0));
			return name;
		}
	}
#endif

	static RC walLockShared(Wal *wal, int lockIdx)
	{
		if (wal->ExclusiveMode) return RC::OK;
		RC rc = wal->DBFile->ShmLock(lockIdx, 1, (VFile::SHM)(VFile::SHM::LOCK | VFile::SHM::SHARED));
		WALTRACE("WAL%p: acquire SHARED-%s %s\n", wal, walLockName(lockIdx), rc ? "failed" : "ok");
		return rc;
	}

	static void walUnlockShared(Wal *wal, int lockIdx)
	{
		if (wal->ExclusiveMode) return;
		wal->DBFile->ShmLock(lockIdx, 1, (VFile::SHM)(VFile::SHM::UNLOCK | VFile::SHM::SHARED));
		WALTRACE("WAL%p: release SHARED-%s\n", wal, walLockName(lockIdx));
	}

	static RC walLockExclusive(Wal *wal, int lockIdx, int n)
	{
		if (wal->ExclusiveMode) return RC::OK;
		RC rc = wal->DBFile->ShmLock(lockIdx, n, (VFile::SHM)(VFile::SHM::LOCK | VFile::SHM::EXCLUSIVE));
		WALTRACE("WAL%p: acquire EXCLUSIVE-%s cnt=%d %s\n", wal, walLockName(lockIdx), n, rc ? "failed" : "ok");
		return rc;
	}

	static void walUnlockExclusive(Wal *wal, int lockIdx, int n)
	{
		if (wal->ExclusiveMode) return;
		wal->DBFile->ShmLock(lockIdx, n, (VFile::SHM)(VFile::SHM::UNLOCK | VFile::SHM::EXCLUSIVE));
		WALTRACE("WAL%p: release EXCLUSIVE-%s cnt=%d\n", wal, walLockName(lockIdx), n);
	}

	static int walHash(uint id)
	{
		_assert(id > 0);
		_assert((HASHTABLE_NSLOT & (HASHTABLE_NSLOT-1)) == 0);
		return (id * HASHTABLE_HASH_1) & (HASHTABLE_NSLOT-1);
	}

	static int walNextHash(int priorHash)
	{
		return (priorHash + 1) & (HASHTABLE_NSLOT - 1);
	}

	static RC walHashGet(Wal *wal, int id, volatile ht_slot **hashOut, volatile Pid **idsOut, uint32 *zeroOut)
	{
		volatile Pid *ids;
		RC rc = walIndexPage(wal, id, &ids);
		_assert(rc == RC::OK || id > 0);

		if (rc == RC::OK)
		{
			Pid zero;
			volatile ht_slot *hash = (volatile ht_slot *)&ids[HASHTABLE_NPAGE];
			if (id == 0)
			{
				ids = &ids[WALINDEX_HDR_SIZE / sizeof(Pid)];
				zero = 0;
			}
			else
				zero = HASHTABLE_NPAGE_ONE + (id - 1) * HASHTABLE_NPAGE;

			*idsOut = &ids[-1];
			*hashOut = hash;
			*zeroOut = zero;
		}
		return rc;
	}

	static int walFramePage(uint32 frame)
	{
		int hash = (frame + HASHTABLE_NPAGE-HASHTABLE_NPAGE_ONE-1) / HASHTABLE_NPAGE;
		_assert((hash==0 || frame > HASHTABLE_NPAGE_ONE) && 
			(hash>=1 || frame <= HASHTABLE_NPAGE_ONE) && 
			(hash<=1 || frame > (HASHTABLE_NPAGE_ONE + HASHTABLE_NPAGE)) && 
			(hash>=2 || frame <= HASHTABLE_NPAGE_ONE + HASHTABLE_NPAGE) && 
			(hash<=2 || frame > (HASHTABLE_NPAGE_ONE + 2 * HASHTABLE_NPAGE)));
		return hash;
	}

	static uint32 walFramePgno(Wal *wal, uint32 frame)
	{
		int hash = walFramePage(frame);
		if (hash == 0)
			return wal->WiData[0][WALINDEX_HDR_SIZE / sizeof(uint32) + frame - 1];
		return wal->WiData[hash][(frame - 1 - HASHTABLE_NPAGE_ONE) % HASHTABLE_NPAGE];
	}

	static void walCleanupHash(Wal *wal)
	{
		_assert(wal->WriteLock);
		ASSERTCOVERAGE(wal->Header.MaxFrame == HASHTABLE_NPAGE_ONE - 1);
		ASSERTCOVERAGE(wal->Header.MaxFrame == HASHTABLE_NPAGE_ONE);
		ASSERTCOVERAGE(wal->Header.MaxFrame == HASHTABLE_NPAGE_ONE + 1);

		if (wal->Header.MaxFrame == 0) return;

		// Obtain pointers to the hash-table and page-number array containing the entry that corresponds to frame pWal->hdr.mxFrame. It is guaranteed
		// that the page said hash-table and array reside on is already mapped.
		_assert(__arrayLength(wal->WiData) > walFramePage(wal->Header.MaxFrame));
		_assert(wal->WiData[walFramePage(wal->Header.MaxFrame)] != 0);
		volatile ht_slot *hash = nullptr; // Pointer to hash table to clear
		volatile Pid *ids = nullptr; // Page number array for hash table
		int zero = 0; // frame == (aHash[x]+iZero)
		walHashGet(wal, walFramePage(wal->Header.MaxFrame), &hash, &ids, &zero);

		// Zero all hash-table entries that correspond to frame numbers greater than pWal->hdr.mxFrame.
		int limit = wal->Header.MaxFrame - zero; // Zero values greater than this
		_assert(limit > 0);
		for (int i = 0; i < HASHTABLE_NSLOT; i++)
			if (hash[i] > limit)
				hash[i] = 0;

		// Zero the entries in the aPgno array that correspond to frames with frame numbers greater than pWal->hdr.mxFrame. 
		int bytes = (int)((char *)hash - (char *)&ids[limit + 1]); // Number of bytes to zero in aPgno[]
		_memset((void *)&ids[limit + 1], 0, bytes);

#ifdef ENABLE_EXPENSIVE_ASSERT
		// Verify that the every entry in the mapping region is still reachable via the hash table even after the cleanup.
		int key; // Hash key
		if (limit)
			for (int i = 1; i <= limit; i++)
			{
				for (key = walHash(ids[i]); hash[key]; key = walNextHash(key))
					if (hash[key] == i) break;
				_assert(hash[key] == i);
			}
#endif
	}

	static RC walIndexAppend(Wal *wal, uint32 frame, Pid id)
	{
		volatile ht_slot *hash = nullptr; // Hash table
		volatile Pid *ids = nullptr; // Page number array
		uint zero = 0; // One less than frame number of aPgno[1]
		RC rc = walHashGet(wal, walFramePage(frame), &hash, &ids, &zero);

		// Assuming the wal-index file was successfully mapped, populate the page number array and hash table entry.
		if (rc == RC::OK)
		{
			int idx = frame - zero; // Value to write to hash-table slot
			_assert(idx <= HASHTABLE_NSLOT / 2 + 1 );

			// If this is the first entry to be added to this hash-table, zero the entire hash table and aPgno[] array before proceding. 
			if (idx == 1)
			{
				int bytes = (int)((uint8 *)&hash[HASHTABLE_NSLOT] - (uint8 *)&ids[1]);
				_memset((void*)&ids[1], 0, bytes);
			}

			// If the entry in aPgno[] is already set, then the previous writer must have exited unexpectedly in the middle of a transaction (after
			// writing one or more dirty pages to the WAL to free up memory). Remove the remnants of that writers uncommitted transaction from 
			// the hash-table before writing any new entries.
			if (ids[idx])
			{
				walCleanupHash(wal);
				_assert(!ids[idx]);
			}

			// Write the aPgno[] array entry and the hash-table slot.
			int collide = idx; // Number of hash collisions
			int key; // Hash table key
			for (key = walHash(id); hash[key]; key = walNextHash(key))
				if ((collide--) == 0) return SysEx_CORRUPT_BKPT;
			ids[idx] = id;
			hash[key] = (ht_slot)idx;

#ifdef ENABLE_EXPENSIVE_ASSERT
			// Verify that the number of entries in the hash table exactly equals the number of entries in the mapping region.
			{
				int entry = 0; // Number of entries in the hash table
				for (int i = 0; i < HASHTABLE_NSLOT; i++) { if (hash[i]) entry++; }
				_assert(entry == idx);
			}

			// Verify that the every entry in the mapping region is reachable via the hash table.  This turns out to be a really, really expensive
			// thing to check, so only do this occasionally - not on every iteration.
			if ((idx & 0x3ff) == 0)
				for (int i = 1; i <= idx; i++)
				{
					for(key = walHash(ids[i]); hash[key]; key = walNextHash(key))
						if (hash[key] == i) break;
					_assert(hash[key] == i);
				}
#endif
		}

		return rc;
	}

	static int walIndexRecover(Wal *wal)
	{
		uint32 frameChecksum[2] = {0, 0};

		// Obtain an exclusive lock on all byte in the locking range not already locked by the caller. The caller is guaranteed to have locked the
		// WAL_WRITE_LOCK byte, and may have also locked the WAL_CKPT_LOCK byte. If successful, the same bytes that are locked here are unlocked before
		// this function returns.
		_assert(wal->CheckpointLock == 1 || wal->CheckpointLock == 0);
		_assert(WAL_ALL_BUT_WRITE == WAL_WRITE_LOCK + 1);
		_assert(WAL_CKPT_LOCK == WAL_ALL_BUT_WRITE);
		_assert(wal->WriteLock);
		int lockIdx = WAL_ALL_BUT_WRITE + wal->CheckpointLock; // Lock offset to lock for checkpoint
		int locks = VFile::SHM::SHM_MAX - lockIdx; // Number of locks to hold
		RC rc = walLockExclusive(wal, lockIdx, locks);
		if (rc)
			return rc;
		WALTRACE("WAL%p: recovery begin...\n", wal);

		_memset(&wal->Header, 0, sizeof(WalIndexHeader));

		int64 size; // Size of log file
		rc = wal->WalFile->get_FileSize(&size);
		if (rc != RC::OK)
			goto recovery_error;

		if (size > WAL_HDRSIZE)
		{
			// Read in the WAL header.
			uint8 buf[WAL_HDRSIZE]; // Buffer to load WAL header into
			rc = wal->WalFile->Read(buf, WAL_HDRSIZE, 0);
			if (rc != RC::OK)
				goto recovery_error;

			// If the database page size is not a power of two, or is greater than SQLITE_MAX_PAGE_SIZE, conclude that the WAL file contains no valid 
			// data. Similarly, if the 'magic' value is invalid, ignore the whole WAL file.
			uint32 magic = ConvertEx::Get4(&buf[0]); // Magic value read from WAL header
			int sizePage = ConvertEx::Get4(&buf[8]); // Page size according to the log
			if ((magic & 0xFFFFFFFE) != WAL_MAGIC ||
				sizePage & (sizePage - 1) ||
				sizePage > MAX_PAGE_SIZE ||
				sizePage < 512)
				goto finished;
			wal->Header.BigEndianChecksum = (uint8)(magic & 0x00000001);
			wal->SizePage = sizePage;
			wal->Checkpoints = ConvertEx::Get4(&buf[12]);
			_memcpy(&wal->Header.Salt, &buf[16], 8);

			// Verify that the WAL header checksum is correct
			walChecksumBytes(wal->Header.BigEndianChecksum == TYPE_BIGENDIAN, buf, WAL_HDRSIZE - 2 * 4, 0, wal->Header.FrameChecksum);
			if (wal->Header.FrameChecksum[0] != ConvertEx::Get4(&buf[24]) || wal->Header.FrameChecksum[1] != ConvertEx::Get4(&buf[28]))
				goto finished;

			// Verify that the version number on the WAL format is one that are able to understand
			uint32 version = ConvertEx::Get4(&buf[4]); // Magic value read from WAL header
			if (version != WAL_MAX_VERSION)
			{
				rc = SysEx_CANTOPEN_BKPT;
				goto finished;
			}

			// Malloc a buffer to read frames into.
			int sizeFrame = sizePage + WAL_FRAME_HDRSIZE; // Number of bytes in buffer aFrame[]
			uint8 *frames = (uint8 *)SysEx::Alloc(sizeFrame); // Malloc'd buffer to load entire frame
			if (!frames)
			{
				rc = RC::NOMEM;
				goto recovery_error;
			}
			uint8 *data = &frames[WAL_FRAME_HDRSIZE]; // Pointer to data part of aFrame buffer

			// Read all frames from the log file.
			int frameIdx = 0; // Index of last frame read
			for (int64 offset = WAL_HDRSIZE; (offset + sizeFrame) <= size; offset += sizeFrame) // Next offset to read from log file
			{ 
				// Read and decode the next log frame.
				frameIdx++;
				rc = wal->WalFile->Read(frames, sizeFrame, offset);
				if (rc != RC::OK) break;
				Pid id; // Database page number for frame
				uint32 truncate; // dbsize field from frame header
				bool isValid = walDecodeFrame(wal, &id, &truncate, data, frames); // True if this frame is valid
				if (!isValid) break;
				rc = walIndexAppend(wal, frameIdx, id);
				if (rc != RC::OK) break;

				// If nTruncate is non-zero, this is a commit record.
				if (truncate)
				{
					wal->Header.MaxFrame = frameIdx;
					wal->Header.Pages = truncate;
					wal->Header.SizePage = (uint16)((sizePage & 0xff00) | (sizePage >> 16));
					ASSERTCOVERAGE(sizePage <= 32768);
					ASSERTCOVERAGE(sizePage >= 65536);
					frameChecksum[0] = wal->Header.FrameChecksum[0];
					frameChecksum[1] = wal->Header.FrameChecksum[1];
				}
			}

			SysEx::Free(frames);
		}

finished:
		if (rc == RC::OK)
		{
			volatile WalCheckpointInfo *info;
			wal->Header.FrameChecksum[0] = frameChecksum[0];
			wal->Header.FrameChecksum[1] = frameChecksum[1];
			walIndexWriteHdr(wal);

			// Reset the checkpoint-header. This is safe because this thread is currently holding locks that exclude all other readers, writers and checkpointers.
			info = walCkptInfo(wal);
			info->Backfills = 0;
			info->ReadMarks[0] = 0;
			for (int i = 1; i < WAL_NREADER; i++) info->ReadMarks[i] = READMARK_NOT_USED;
			if (wal->Header.MaxFrame) info->ReadMarks[1] = wal->Header.MaxFrame;

			// If more than one frame was recovered from the log file, report an event via sqlite3_log(). This is to help with identifying performance
			// problems caused by applications routinely shutting down without checkpointing the log file.
			if (wal->Header.Pages)
				SysEx_log(RC::OK, "Recovered %d frames from WAL file %s", wal->Header.Pages, wal->WalName);
		}

recovery_error:
		WALTRACE("WAL%p: recovery %s\n", wal, rc ? "failed" : "ok");
		walUnlockExclusive(wal, lockIdx, locks);
		return rc;
	}

	static void walIndexClose(Wal *wal, int isDelete)
	{
		if (wal->ExclusiveMode == WAL_HEAPMEMORY_MODE)
			for (int i = 0; i < __arrayLength(wal->WiData); i++)
			{
				SysEx::Free((void *)wal->WiData[i]);
				wal->WiData[i] = nullptr;
			}
		else
			wal->DBFile->ShmUnmap(isDelete);
	}

	RC Wal::Open(VFileSystem *vfs, VFile *dbFile, const char *walName, bool noShm, int64 maxWalSize, Wal **walOut)
	{
		_assert(walName && walName[0]);
		_assert(dbFile != nullptr);

		// In the amalgamation, the os_unix.c and os_win.c source files come before this source file.  Verify that the #defines of the locking byte offsets
		// in os_unix.c and os_win.c agree with the WALINDEX_LOCK_OFFSET value.
#ifdef WIN_SHM_BASE
		_assert(WIN_SHM_BASE == WALINDEX_LOCK_OFFSET);
#endif
#ifdef UNIX_SHM_BASE
		_assert(UNIX_SHM_BASE == WALINDEX_LOCK_OFFSET);
#endif

		// Allocate an instance of struct Wal to return.
		*walOut = nullptr;
		Wal *r = (Wal *)SysEx::Alloc(sizeof(Wal) + vfs->SizeOsFile, true); // Object to allocate and return
		if (!r)
			return RC::NOMEM;

		r->Vfs = vfs;
		r->WalFile = (VFile *)&r[1];
		r->DBFile = dbFile;
		r->ReadLock = -1;
		r->MaxWalSize = maxWalSize;
		r->WalName = walName;
		r->SyncHeader = 1;
		r->PadToSectorBoundary = 1;
		r->ExclusiveMode = (noShm ? WAL_HEAPMEMORY_MODE : WAL_NORMAL_MODE);

		// Open file handle on the write-ahead log file.
		VFileSystem::OPEN flags = (VFileSystem::OPEN)(VFileSystem::OPEN::OREADWRITE | VFileSystem::OPEN::CREATE | VFileSystem::OPEN::WAL);
		RC rc = vfs->Open(walName, r->WalFile, flags, &flags);
		if (rc == RC::OK && flags & VFileSystem::OPEN::READONLY)
			r->ReadOnly = WAL_RDONLY;

		if (rc != RC::OK)
		{
			walIndexClose(r, 0);
			r->WalFile->Close();
			SysEx::Free(r);
		}
		else
		{
			int dc = r->WalFile->get_DeviceCharacteristics();
			if (dc & VFile::IOCAP::SEQUENTIAL) { r->SyncHeader = 0; }
			if (dc & VFile::IOCAP::POWERSAFE_OVERWRITE)
				r->PadToSectorBoundary = 0;
			*walOut = r;
			WALTRACE("WAL%d: opened\n", r);
		}
		return rc;
	}

	void Wal::Limit(Wal *wal, int64 limit)
	{
		if (wal) wal->MaxWalSize = limit;
	}

	static int walIteratorNext(WalIterator *p, uint32 *page, uint32 *frame)
	{
		uint32 r = 0xFFFFFFFF; // 0xffffffff is never a valid page number
		uint32 min = p->Prior; // Result pgno must be greater than iMin
		_assert(min < 0xffffffff);
		for (int i = __arrayLength(p->Segments) - 1; i >= 0; i--)
		{
			struct WalSegment *segment = &p->Segments[i];
			while (segment->Next < segment->Entrys)
			{
				uint32 id = segment->IDs[segment->Indexs[segment->Next]];
				if (id > min)
				{
					if (id < r)
					{
						r = id;
						*frame = segment->Zero + segment->Indexs[segment->Next];
					}
					break;
				}
				segment->Next++;
			}
		}

		*page = p->Prior = r;
		return (r == 0xFFFFFFFF);
	}

	static void walMerge(const uint32 *content, ht_slot *lefts, int leftsLength, ht_slot **rightsOut, int *rightsLengthOut, ht_slot *tmp)
	{
		int left = 0; // Current index in aLeft
		int right = 0; // Current index in aRight
		int out = 0; // Current index in output buffer */
		int rightsLength = *rightsLengthOut;
		ht_slot *rights = *rightsOut;

		_assert(leftsLength > 0 && rightsLength > 0);
		while (right < rightsLength || left < leftsLength)
		{
			ht_slot logpage;
			if (left < leftsLength && (right >= rightsLength || content[lefts[left]] < content[rights[right]]))
				logpage = lefts[left++];
			else
				logpage = rights[right++];
			Pid dbpage = content[logpage];

			tmp[out++] = logpage;
			if (left < leftsLength && content[lefts[left]] == dbpage) left++;

			_assert(left >= leftsLength || content[lefts[left]] > dbpage);
			_assert(right >= rightsLength || content[rights[right]] > dbpage);
		}

		*rightsOut = lefts;
		*rightsLengthOut = out;
		_memcpy(lefts, tmp, sizeof(tmp[0]) * out);
	}

	static void walMergesort(const uint32 *content, ht_slot *buffer, ht_slot *list, int *listLengthRef)
	{
		struct Sublist
		{
			int ListLength; // Number of elements in aList
			ht_slot *List; // Pointer to sub-list content
		};

		const int listLength = *listLengthRef; // Size of input list
		int mergeLength = 0; // Number of elements in list aMerge
		ht_slot *merge = nullptr; // List to be merged
		int subIdx = 0; // Index into aSub array
		struct Sublist subs[13]; // Array of sub-lists

		_memset(subs, 0, sizeof(subs));
		_assert(listLength <= HASHTABLE_NPAGE && listLength > 0);
		_assert(HASHTABLE_NPAGE == (1 << (ArraySize(subs) - 1)));

		for (int listIdx = 0; listIdx < listLength; listIdx++) // Index into input list
		{
			mergeLength = 1;
			merge = &list[listIdx];
			for (subIdx = 0; listIdx & (1 << subIdx); subIdx++)
			{
				struct Sublist *p = &subs[subIdx];
				_assert(p->List && p->ListLength <= (1 << subIdx));
				_assert(p->List == &list[listIdx & ~((2 << subIdx) - 1)]);
				walMerge(content, p->List, p->ListLength, &merge, &mergeLength, buffer);
			}
			subs[subIdx].List = merge;
			subs[subIdx].ListLength = mergeLength;
		}

		for (subIdx++; subIdx < ArraySize(subs); subIdx++)
		{
			if (listLength & (1 << subIdx))
			{
				struct Sublist *p = &subs[subIdx];
				_assert(p->ListLength <= (1 << subIdx));
				_assert(p->List == &list[listLength & ~((2 << subIdx) - 1)]);
				walMerge(content, p->List, p->ListLength, &merge, &mergeLength, buffer);
			}
		}
		_assert(merge == list);
		*listLengthRef = mergeLength;

#ifdef _DEBUG
		for (int i = 1; i < *listLengthRef; i++)
			_assert(content[list[i]] > content[list[i - 1]]);
#endif
	}

	static void walIteratorFree(WalIterator *p)
	{
		sqlite3ScratchFree(p);
	}

	static RC walIteratorInit(Wal *wal, WalIterator **iteratorOut)
	{
		// This routine only runs while holding the checkpoint lock. And it only runs if there is actually content in the log (mxFrame>0).
		_assert(wal->CheckpointLock && wal->Header.MaxFrame > 0);
		uint32 lastFrame = wal->Header.MaxFrame; // Last frame in log

		// Allocate space for the WalIterator object.
		int segments = walFramePage(lastFrame) + 1; // Number of segments to merge
		int bytes = sizeof(WalIterator) + (segments - 1) * sizeof(struct WalSegment) + lastFrame * sizeof(ht_slot); // Number of bytes to allocate
		WalIterator *p = (WalIterator *)SysEx::ScratchAlloc(bytes); // Return value
		if (!p)
			return RC::NOMEM;
		_memset(p, 0, bytes);
		p->SegmentsLength = segments;

		// Allocate temporary space used by the merge-sort routine. This block of memory will be freed before this function returns.
		RC rc = RC::OK;
		ht_slot *tmp = (ht_slot *)SysEx::ScratchAlloc(sizeof(ht_slot) * (lastFrame > HASHTABLE_NPAGE ? HASHTABLE_NPAGE : lastFrame)); // Temp space used by merge-sort
		if (!tmp)
			rc = RC::NOMEM;

		for (int i = 0; rc == RC::OK && i < segments; i++)
		{
			volatile ht_slot *hash;
			volatile uint32 *ids;
			uint32 zero;
			rc = walHashGet(wal, i, &hash, &ids, &zero);
			if (rc == RC::OK)
			{
				ids++;
				int entrys; // Number of entries in this segment
				if ((i + 1) == segments)
					entrys = (int)(lastFrame - zero);
				else
					entrys = (int)((uint32 *)hash - (Pid *)ids);
				ht_slot *indexs = &((ht_slot *)&p->Segments[p->SegmentsLength])[zero]; // Sorted index for this segment
				zero++;

				for (int j = 0; j < entrys; j++)
					indexs[j] = (ht_slot)j;
				walMergesort((Pid *)ids, tmp, indexs, &entrys);
				p->Segments[i].Zero = zero;
				p->Segments[i].Entrys = entrys;
				p->Segments[i].Indexs = indexs;
				p->Segments[i].IDs = (Pid *)ids;
			}
		}
		SysEx::ScratchFree(tmp);

		if (rc != RC::OK)
			walIteratorFree(p);
		*iteratorOut = p;
		return rc;
	}

	static RC walBusyLock(Wal *wal, int (*busy)(void *), void *busyArg, int lockIdx, int n)
	{
		RC rc;
		do
		{
			rc = walLockExclusive(wal, lockIdx, n);
		} while (busy && rc == RC::BUSY && busy(busyArg));
		return rc;
	}

	static int walPagesize(Wal *wal)
	{
		return (wal->Header.SizePage & 0xfe00) + ((wal->Header.SizePage & 0x0001) << 16);
	}

	static int walCheckpoint(
		Wal *pWal,                      /* Wal connection */
		int eMode,                      /* One of PASSIVE, FULL or RESTART */
		int (*xBusyCall)(void*),        /* Function to call when busy */
		void *pBusyArg,                 /* Context argument for xBusyHandler */
		int sync_flags,                 /* Flags for OsSync() (or 0) */
		u8 *zBuf                        /* Temporary buffer to use */
		){
			int rc;                         /* Return code */
			int szPage;                     /* Database page-size */
			WalIterator *pIter = 0;         /* Wal iterator context */
			u32 iDbpage = 0;                /* Next database page to write */
			u32 iFrame = 0;                 /* Wal frame containing data for iDbpage */
			u32 mxSafeFrame;                /* Max frame that can be backfilled */
			u32 mxPage;                     /* Max database page to write */
			int i;                          /* Loop counter */
			volatile WalCkptInfo *pInfo;    /* The checkpoint status information */
			int (*xBusy)(void*) = 0;        /* Function to call when waiting for locks */

			szPage = walPagesize(pWal);
			testcase( szPage<=32768 );
			testcase( szPage>=65536 );
			pInfo = walCkptInfo(pWal);
			if( pInfo->nBackfill>=pWal->hdr.mxFrame ) return SQLITE_OK;

			/* Allocate the iterator */
			rc = walIteratorInit(pWal, &pIter);
			if( rc!=SQLITE_OK ){
				return rc;
			}
			assert( pIter );

			if( eMode!=SQLITE_CHECKPOINT_PASSIVE ) xBusy = xBusyCall;

			/* Compute in mxSafeFrame the index of the last frame of the WAL that is
			** safe to write into the database.  Frames beyond mxSafeFrame might
			** overwrite database pages that are in use by active readers and thus
			** cannot be backfilled from the WAL.
			*/
			mxSafeFrame = pWal->hdr.mxFrame;
			mxPage = pWal->hdr.nPage;
			for(i=1; i<WAL_NREADER; i++){
				u32 y = pInfo->aReadMark[i];
				if( mxSafeFrame>y ){
					assert( y<=pWal->hdr.mxFrame );
					rc = walBusyLock(pWal, xBusy, pBusyArg, WAL_READ_LOCK(i), 1);
					if( rc==SQLITE_OK ){
						pInfo->aReadMark[i] = (i==1 ? mxSafeFrame : READMARK_NOT_USED);
						walUnlockExclusive(pWal, WAL_READ_LOCK(i), 1);
					}else if( rc==SQLITE_BUSY ){
						mxSafeFrame = y;
						xBusy = 0;
					}else{
						goto walcheckpoint_out;
					}
				}
			}

			if( pInfo->nBackfill<mxSafeFrame
				&& (rc = walBusyLock(pWal, xBusy, pBusyArg, WAL_READ_LOCK(0), 1))==SQLITE_OK
				){
					i64 nSize;                    /* Current size of database file */
					u32 nBackfill = pInfo->nBackfill;

					/* Sync the WAL to disk */
					if( sync_flags ){
						rc = sqlite3OsSync(pWal->pWalFd, sync_flags);
					}

					/* If the database file may grow as a result of this checkpoint, hint
					** about the eventual size of the db file to the VFS layer. 
					*/
					if( rc==SQLITE_OK ){
						i64 nReq = ((i64)mxPage * szPage);
						rc = sqlite3OsFileSize(pWal->pDbFd, &nSize);
						if( rc==SQLITE_OK && nSize<nReq ){
							sqlite3OsFileControlHint(pWal->pDbFd, SQLITE_FCNTL_SIZE_HINT, &nReq);
						}
					}

					/* Iterate through the contents of the WAL, copying data to the db file. */
					while( rc==SQLITE_OK && 0==walIteratorNext(pIter, &iDbpage, &iFrame) ){
						i64 iOffset;
						assert( walFramePgno(pWal, iFrame)==iDbpage );
						if( iFrame<=nBackfill || iFrame>mxSafeFrame || iDbpage>mxPage ) continue;
						iOffset = walFrameOffset(iFrame, szPage) + WAL_FRAME_HDRSIZE;
						/* testcase( IS_BIG_INT(iOffset) ); // requires a 4GiB WAL file */
						rc = sqlite3OsRead(pWal->pWalFd, zBuf, szPage, iOffset);
						if( rc!=SQLITE_OK ) break;
						iOffset = (iDbpage-1)*(i64)szPage;
						testcase( IS_BIG_INT(iOffset) );
						rc = sqlite3OsWrite(pWal->pDbFd, zBuf, szPage, iOffset);
						if( rc!=SQLITE_OK ) break;
					}

					/* If work was actually accomplished... */
					if( rc==SQLITE_OK ){
						if( mxSafeFrame==walIndexHdr(pWal)->mxFrame ){
							i64 szDb = pWal->hdr.nPage*(i64)szPage;
							testcase( IS_BIG_INT(szDb) );
							rc = sqlite3OsTruncate(pWal->pDbFd, szDb);
							if( rc==SQLITE_OK && sync_flags ){
								rc = sqlite3OsSync(pWal->pDbFd, sync_flags);
							}
						}
						if( rc==SQLITE_OK ){
							pInfo->nBackfill = mxSafeFrame;
						}
					}

					/* Release the reader lock held while backfilling */
					walUnlockExclusive(pWal, WAL_READ_LOCK(0), 1);
			}

			if( rc==SQLITE_BUSY ){
				/* Reset the return code so as not to report a checkpoint failure
				** just because there are active readers.  */
				rc = SQLITE_OK;
			}

			/* If this is an SQLITE_CHECKPOINT_RESTART operation, and the entire wal
			** file has been copied into the database file, then block until all
			** readers have finished using the wal file. This ensures that the next
			** process to write to the database restarts the wal file.
			*/
			if( rc==SQLITE_OK && eMode!=SQLITE_CHECKPOINT_PASSIVE ){
				assert( pWal->writeLock );
				if( pInfo->nBackfill<pWal->hdr.mxFrame ){
					rc = SQLITE_BUSY;
				}else if( eMode==SQLITE_CHECKPOINT_RESTART ){
					assert( mxSafeFrame==pWal->hdr.mxFrame );
					rc = walBusyLock(pWal, xBusy, pBusyArg, WAL_READ_LOCK(1), WAL_NREADER-1);
					if( rc==SQLITE_OK ){
						walUnlockExclusive(pWal, WAL_READ_LOCK(1), WAL_NREADER-1);
					}
				}
			}

walcheckpoint_out:
			walIteratorFree(pIter);
			return rc;
	}

	static void walLimitSize(Wal *pWal, i64 nMax){
		i64 sz;
		int rx;
		sqlite3BeginBenignMalloc();
		rx = sqlite3OsFileSize(pWal->pWalFd, &sz);
		if( rx==SQLITE_OK && (sz > nMax ) ){
			rx = sqlite3OsTruncate(pWal->pWalFd, nMax);
		}
		sqlite3EndBenignMalloc();
		if( rx ){
			sqlite3_log(rx, "cannot limit WAL size: %s", pWal->zWalName);
		}
	}

	int sqlite3WalClose(
		Wal *pWal,                      /* Wal to close */
		int sync_flags,                 /* Flags to pass to OsSync() (or 0) */
		int nBuf,
		u8 *zBuf                        /* Buffer of at least nBuf bytes */
		){
			int rc = SQLITE_OK;
			if( pWal ){
				int isDelete = 0;             /* True to unlink wal and wal-index files */

				/* If an EXCLUSIVE lock can be obtained on the database file (using the
				** ordinary, rollback-mode locking methods, this guarantees that the
				** connection associated with this log file is the only connection to
				** the database. In this case checkpoint the database and unlink both
				** the wal and wal-index files.
				**
				** The EXCLUSIVE lock is not released before returning.
				*/
				rc = sqlite3OsLock(pWal->pDbFd, SQLITE_LOCK_EXCLUSIVE);
				if( rc==SQLITE_OK ){
					if( pWal->exclusiveMode==WAL_NORMAL_MODE ){
						pWal->exclusiveMode = WAL_EXCLUSIVE_MODE;
					}
					rc = sqlite3WalCheckpoint(
						pWal, SQLITE_CHECKPOINT_PASSIVE, 0, 0, sync_flags, nBuf, zBuf, 0, 0
						);
					if( rc==SQLITE_OK ){
						int bPersist = -1;
						sqlite3OsFileControlHint(
							pWal->pDbFd, SQLITE_FCNTL_PERSIST_WAL, &bPersist
							);
						if( bPersist!=1 ){
							/* Try to delete the WAL file if the checkpoint completed and
							** fsyned (rc==SQLITE_OK) and if we are not in persistent-wal
							** mode (!bPersist) */
							isDelete = 1;
						}else if( pWal->mxWalSize>=0 ){
							/* Try to truncate the WAL file to zero bytes if the checkpoint
							** completed and fsynced (rc==SQLITE_OK) and we are in persistent
							** WAL mode (bPersist) and if the PRAGMA journal_size_limit is a
							** non-negative value (pWal->mxWalSize>=0).  Note that we truncate
							** to zero bytes as truncating to the journal_size_limit might
							** leave a corrupt WAL file on disk. */
							walLimitSize(pWal, 0);
						}
					}
				}

				walIndexClose(pWal, isDelete);
				sqlite3OsClose(pWal->pWalFd);
				if( isDelete ){
					sqlite3BeginBenignMalloc();
					sqlite3OsDelete(pWal->pVfs, pWal->zWalName, 0);
					sqlite3EndBenignMalloc();
				}
				WALTRACE(("WAL%p: closed\n", pWal));
				sqlite3_free((void *)pWal->apWiData);
				sqlite3_free(pWal);
			}
			return rc;
	}

	static int walIndexTryHdr(Wal *pWal, int *pChanged){
		u32 aCksum[2];                  /* Checksum on the header content */
		WalIndexHdr h1, h2;             /* Two copies of the header content */
		WalIndexHdr volatile *aHdr;     /* Header in shared memory */

		/* The first page of the wal-index must be mapped at this point. */
		assert( pWal->nWiData>0 && pWal->apWiData[0] );

		/* Read the header. This might happen concurrently with a write to the
		** same area of shared memory on a different CPU in a SMP,
		** meaning it is possible that an inconsistent snapshot is read
		** from the file. If this happens, return non-zero.
		**
		** There are two copies of the header at the beginning of the wal-index.
		** When reading, read [0] first then [1].  Writes are in the reverse order.
		** Memory barriers are used to prevent the compiler or the hardware from
		** reordering the reads and writes.
		*/
		aHdr = walIndexHdr(pWal);
		memcpy(&h1, (void *)&aHdr[0], sizeof(h1));
		walShmBarrier(pWal);
		memcpy(&h2, (void *)&aHdr[1], sizeof(h2));

		if( memcmp(&h1, &h2, sizeof(h1))!=0 ){
			return 1;   /* Dirty read */
		}  
		if( h1.isInit==0 ){
			return 1;   /* Malformed header - probably all zeros */
		}
		walChecksumBytes(1, (u8*)&h1, sizeof(h1)-sizeof(h1.aCksum), 0, aCksum);
		if( aCksum[0]!=h1.aCksum[0] || aCksum[1]!=h1.aCksum[1] ){
			return 1;   /* Checksum does not match */
		}

		if( memcmp(&pWal->hdr, &h1, sizeof(WalIndexHdr)) ){
			*pChanged = 1;
			memcpy(&pWal->hdr, &h1, sizeof(WalIndexHdr));
			pWal->szPage = (pWal->hdr.szPage&0xfe00) + ((pWal->hdr.szPage&0x0001)<<16);
			testcase( pWal->szPage<=32768 );
			testcase( pWal->szPage>=65536 );
		}

		/* The header was successfully read. Return zero. */
		return 0;
	}

	static int walIndexReadHdr(Wal *pWal, int *pChanged){
		int rc;                         /* Return code */
		int badHdr;                     /* True if a header read failed */
		volatile u32 *page0;            /* Chunk of wal-index containing header */

		/* Ensure that page 0 of the wal-index (the page that contains the 
		** wal-index header) is mapped. Return early if an error occurs here.
		*/
		assert( pChanged );
		rc = walIndexPage(pWal, 0, &page0);
		if( rc!=SQLITE_OK ){
			return rc;
		};
		assert( page0 || pWal->writeLock==0 );

		/* If the first page of the wal-index has been mapped, try to read the
		** wal-index header immediately, without holding any lock. This usually
		** works, but may fail if the wal-index header is corrupt or currently 
		** being modified by another thread or process.
		*/
		badHdr = (page0 ? walIndexTryHdr(pWal, pChanged) : 1);

		/* If the first attempt failed, it might have been due to a race
		** with a writer.  So get a WRITE lock and try again.
		*/
		assert( badHdr==0 || pWal->writeLock==0 );
		if( badHdr ){
			if( pWal->readOnly & WAL_SHM_RDONLY ){
				if( SQLITE_OK==(rc = walLockShared(pWal, WAL_WRITE_LOCK)) ){
					walUnlockShared(pWal, WAL_WRITE_LOCK);
					rc = SQLITE_READONLY_RECOVERY;
				}
			}else if( SQLITE_OK==(rc = walLockExclusive(pWal, WAL_WRITE_LOCK, 1)) ){
				pWal->writeLock = 1;
				if( SQLITE_OK==(rc = walIndexPage(pWal, 0, &page0)) ){
					badHdr = walIndexTryHdr(pWal, pChanged);
					if( badHdr ){
						/* If the wal-index header is still malformed even while holding
						** a WRITE lock, it can only mean that the header is corrupted and
						** needs to be reconstructed.  So run recovery to do exactly that.
						*/
						rc = walIndexRecover(pWal);
						*pChanged = 1;
					}
				}
				pWal->writeLock = 0;
				walUnlockExclusive(pWal, WAL_WRITE_LOCK, 1);
			}
		}

		/* If the header is read successfully, check the version number to make
		** sure the wal-index was not constructed with some future format that
		** this version of SQLite cannot understand.
		*/
		if( badHdr==0 && pWal->hdr.iVersion!=WALINDEX_MAX_VERSION ){
			rc = SQLITE_CANTOPEN_BKPT;
		}

		return rc;
	}

#define WAL_RETRY  (-1)

	static int walTryBeginRead(Wal *pWal, int *pChanged, int useWal, int cnt){
		volatile WalCkptInfo *pInfo;    /* Checkpoint information in wal-index */
		u32 mxReadMark;                 /* Largest aReadMark[] value */
		int mxI;                        /* Index of largest aReadMark[] value */
		int i;                          /* Loop counter */
		int rc = SQLITE_OK;             /* Return code  */

		assert( pWal->readLock<0 );     /* Not currently locked */

		/* Take steps to avoid spinning forever if there is a protocol error.
		**
		** Circumstances that cause a RETRY should only last for the briefest
		** instances of time.  No I/O or other system calls are done while the
		** locks are held, so the locks should not be held for very long. But 
		** if we are unlucky, another process that is holding a lock might get
		** paged out or take a page-fault that is time-consuming to resolve, 
		** during the few nanoseconds that it is holding the lock.  In that case,
		** it might take longer than normal for the lock to free.
		**
		** After 5 RETRYs, we begin calling sqlite3OsSleep().  The first few
		** calls to sqlite3OsSleep() have a delay of 1 microsecond.  Really this
		** is more of a scheduler yield than an actual delay.  But on the 10th
		** an subsequent retries, the delays start becoming longer and longer, 
		** so that on the 100th (and last) RETRY we delay for 21 milliseconds.
		** The total delay time before giving up is less than 1 second.
		*/
		if( cnt>5 ){
			int nDelay = 1;                      /* Pause time in microseconds */
			if( cnt>100 ){
				VVA_ONLY( pWal->lockError = 1; )
					return SQLITE_PROTOCOL;
			}
			if( cnt>=10 ) nDelay = (cnt-9)*238;  /* Max delay 21ms. Total delay 996ms */
			sqlite3OsSleep(pWal->pVfs, nDelay);
		}

		if( !useWal ){
			rc = walIndexReadHdr(pWal, pChanged);
			if( rc==SQLITE_BUSY ){
				/* If there is not a recovery running in another thread or process
				** then convert BUSY errors to WAL_RETRY.  If recovery is known to
				** be running, convert BUSY to BUSY_RECOVERY.  There is a race here
				** which might cause WAL_RETRY to be returned even if BUSY_RECOVERY
				** would be technically correct.  But the race is benign since with
				** WAL_RETRY this routine will be called again and will probably be
				** right on the second iteration.
				*/
				if( pWal->apWiData[0]==0 ){
					/* This branch is taken when the xShmMap() method returns SQLITE_BUSY.
					** We assume this is a transient condition, so return WAL_RETRY. The
					** xShmMap() implementation used by the default unix and win32 VFS 
					** modules may return SQLITE_BUSY due to a race condition in the 
					** code that determines whether or not the shared-memory region 
					** must be zeroed before the requested page is returned.
					*/
					rc = WAL_RETRY;
				}else if( SQLITE_OK==(rc = walLockShared(pWal, WAL_RECOVER_LOCK)) ){
					walUnlockShared(pWal, WAL_RECOVER_LOCK);
					rc = WAL_RETRY;
				}else if( rc==SQLITE_BUSY ){
					rc = SQLITE_BUSY_RECOVERY;
				}
			}
			if( rc!=SQLITE_OK ){
				return rc;
			}
		}

		pInfo = walCkptInfo(pWal);
		if( !useWal && pInfo->nBackfill==pWal->hdr.mxFrame ){
			/* The WAL has been completely backfilled (or it is empty).
			** and can be safely ignored.
			*/
			rc = walLockShared(pWal, WAL_READ_LOCK(0));
			walShmBarrier(pWal);
			if( rc==SQLITE_OK ){
				if( memcmp((void *)walIndexHdr(pWal), &pWal->hdr, sizeof(WalIndexHdr)) ){
					/* It is not safe to allow the reader to continue here if frames
					** may have been appended to the log before READ_LOCK(0) was obtained.
					** When holding READ_LOCK(0), the reader ignores the entire log file,
					** which implies that the database file contains a trustworthy
					** snapshoT. Since holding READ_LOCK(0) prevents a checkpoint from
					** happening, this is usually correct.
					**
					** However, if frames have been appended to the log (or if the log 
					** is wrapped and written for that matter) before the READ_LOCK(0)
					** is obtained, that is not necessarily true. A checkpointer may
					** have started to backfill the appended frames but crashed before
					** it finished. Leaving a corrupt image in the database file.
					*/
					walUnlockShared(pWal, WAL_READ_LOCK(0));
					return WAL_RETRY;
				}
				pWal->readLock = 0;
				return SQLITE_OK;
			}else if( rc!=SQLITE_BUSY ){
				return rc;
			}
		}

		/* If we get this far, it means that the reader will want to use
		** the WAL to get at content from recent commits.  The job now is
		** to select one of the aReadMark[] entries that is closest to
		** but not exceeding pWal->hdr.mxFrame and lock that entry.
		*/
		mxReadMark = 0;
		mxI = 0;
		for(i=1; i<WAL_NREADER; i++){
			u32 thisMark = pInfo->aReadMark[i];
			if( mxReadMark<=thisMark && thisMark<=pWal->hdr.mxFrame ){
				assert( thisMark!=READMARK_NOT_USED );
				mxReadMark = thisMark;
				mxI = i;
			}
		}
		/* There was once an "if" here. The extra "{" is to preserve indentation. */
		{
			if( (pWal->readOnly & WAL_SHM_RDONLY)==0
				&& (mxReadMark<pWal->hdr.mxFrame || mxI==0)
				){
					for(i=1; i<WAL_NREADER; i++){
						rc = walLockExclusive(pWal, WAL_READ_LOCK(i), 1);
						if( rc==SQLITE_OK ){
							mxReadMark = pInfo->aReadMark[i] = pWal->hdr.mxFrame;
							mxI = i;
							walUnlockExclusive(pWal, WAL_READ_LOCK(i), 1);
							break;
						}else if( rc!=SQLITE_BUSY ){
							return rc;
						}
					}
			}
			if( mxI==0 ){
				assert( rc==SQLITE_BUSY || (pWal->readOnly & WAL_SHM_RDONLY)!=0 );
				return rc==SQLITE_BUSY ? WAL_RETRY : SQLITE_READONLY_CANTLOCK;
			}

			rc = walLockShared(pWal, WAL_READ_LOCK(mxI));
			if( rc ){
				return rc==SQLITE_BUSY ? WAL_RETRY : rc;
			}
			/* Now that the read-lock has been obtained, check that neither the
			** value in the aReadMark[] array or the contents of the wal-index
			** header have changed.
			**
			** It is necessary to check that the wal-index header did not change
			** between the time it was read and when the shared-lock was obtained
			** on WAL_READ_LOCK(mxI) was obtained to account for the possibility
			** that the log file may have been wrapped by a writer, or that frames
			** that occur later in the log than pWal->hdr.mxFrame may have been
			** copied into the database by a checkpointer. If either of these things
			** happened, then reading the database with the current value of
			** pWal->hdr.mxFrame risks reading a corrupted snapshot. So, retry
			** instead.
			**
			** This does not guarantee that the copy of the wal-index header is up to
			** date before proceeding. That would not be possible without somehow
			** blocking writers. It only guarantees that a dangerous checkpoint or 
			** log-wrap (either of which would require an exclusive lock on
			** WAL_READ_LOCK(mxI)) has not occurred since the snapshot was valid.
			*/
			walShmBarrier(pWal);
			if( pInfo->aReadMark[mxI]!=mxReadMark
				|| memcmp((void *)walIndexHdr(pWal), &pWal->hdr, sizeof(WalIndexHdr))
				){
					walUnlockShared(pWal, WAL_READ_LOCK(mxI));
					return WAL_RETRY;
			}else{
				assert( mxReadMark<=pWal->hdr.mxFrame );
				pWal->readLock = (i16)mxI;
			}
		}
		return rc;
	}

	int sqlite3WalBeginReadTransaction(Wal *pWal, int *pChanged){
		int rc;                         /* Return code */
		int cnt = 0;                    /* Number of TryBeginRead attempts */

		do{
			rc = walTryBeginRead(pWal, pChanged, 0, ++cnt);
		}while( rc==WAL_RETRY );
		testcase( (rc&0xff)==SQLITE_BUSY );
		testcase( (rc&0xff)==SQLITE_IOERR );
		testcase( rc==SQLITE_PROTOCOL );
		testcase( rc==SQLITE_OK );
		return rc;
	}

	void sqlite3WalEndReadTransaction(Wal *pWal){
		sqlite3WalEndWriteTransaction(pWal);
		if( pWal->readLock>=0 ){
			walUnlockShared(pWal, WAL_READ_LOCK(pWal->readLock));
			pWal->readLock = -1;
		}
	}

	int sqlite3WalRead(
		Wal *pWal,                      /* WAL handle */
		Pgno pgno,                      /* Database page number to read data for */
		int *pInWal,                    /* OUT: True if data is read from WAL */
		int nOut,                       /* Size of buffer pOut in bytes */
		u8 *pOut                        /* Buffer to write page data to */
		){
			u32 iRead = 0;                  /* If !=0, WAL frame to return data from */
			u32 iLast = pWal->hdr.mxFrame;  /* Last page in WAL for this reader */
			int iHash;                      /* Used to loop through N hash tables */

			/* This routine is only be called from within a read transaction. */
			assert( pWal->readLock>=0 || pWal->lockError );

			/* If the "last page" field of the wal-index header snapshot is 0, then
			** no data will be read from the wal under any circumstances. Return early
			** in this case as an optimization.  Likewise, if pWal->readLock==0, 
			** then the WAL is ignored by the reader so return early, as if the 
			** WAL were empty.
			*/
			if( iLast==0 || pWal->readLock==0 ){
				*pInWal = 0;
				return SQLITE_OK;
			}

			/* Search the hash table or tables for an entry matching page number
			** pgno. Each iteration of the following for() loop searches one
			** hash table (each hash table indexes up to HASHTABLE_NPAGE frames).
			**
			** This code might run concurrently to the code in walIndexAppend()
			** that adds entries to the wal-index (and possibly to this hash 
			** table). This means the value just read from the hash 
			** slot (aHash[iKey]) may have been added before or after the 
			** current read transaction was opened. Values added after the
			** read transaction was opened may have been written incorrectly -
			** i.e. these slots may contain garbage data. However, we assume
			** that any slots written before the current read transaction was
			** opened remain unmodified.
			**
			** For the reasons above, the if(...) condition featured in the inner
			** loop of the following block is more stringent that would be required 
			** if we had exclusive access to the hash-table:
			**
			**   (aPgno[iFrame]==pgno): 
			**     This condition filters out normal hash-table collisions.
			**
			**   (iFrame<=iLast): 
			**     This condition filters out entries that were added to the hash
			**     table after the current read-transaction had started.
			*/
			for(iHash=walFramePage(iLast); iHash>=0 && iRead==0; iHash--){
				volatile ht_slot *aHash;      /* Pointer to hash table */
				volatile u32 *aPgno;          /* Pointer to array of page numbers */
				u32 iZero;                    /* Frame number corresponding to aPgno[0] */
				int iKey;                     /* Hash slot index */
				int nCollide;                 /* Number of hash collisions remaining */
				int rc;                       /* Error code */

				rc = walHashGet(pWal, iHash, &aHash, &aPgno, &iZero);
				if( rc!=SQLITE_OK ){
					return rc;
				}
				nCollide = HASHTABLE_NSLOT;
				for(iKey=walHash(pgno); aHash[iKey]; iKey=walNextHash(iKey)){
					u32 iFrame = aHash[iKey] + iZero;
					if( iFrame<=iLast && aPgno[aHash[iKey]]==pgno ){
						/* assert( iFrame>iRead ); -- not true if there is corruption */
						iRead = iFrame;
					}
					if( (nCollide--)==0 ){
						return SQLITE_CORRUPT_BKPT;
					}
				}
			}

#ifdef SQLITE_ENABLE_EXPENSIVE_ASSERT
			/* If expensive assert() statements are available, do a linear search
			** of the wal-index file content. Make sure the results agree with the
			** result obtained using the hash indexes above.  */
			{
				u32 iRead2 = 0;
				u32 iTest;
				for(iTest=iLast; iTest>0; iTest--){
					if( walFramePgno(pWal, iTest)==pgno ){
						iRead2 = iTest;
						break;
					}
				}
				assert( iRead==iRead2 );
			}
#endif

			/* If iRead is non-zero, then it is the log frame number that contains the
			** required page. Read and return data from the log file.
			*/
			if( iRead ){
				int sz;
				i64 iOffset;
				sz = pWal->hdr.szPage;
				sz = (sz&0xfe00) + ((sz&0x0001)<<16);
				testcase( sz<=32768 );
				testcase( sz>=65536 );
				iOffset = walFrameOffset(iRead, sz) + WAL_FRAME_HDRSIZE;
				*pInWal = 1;
				/* testcase( IS_BIG_INT(iOffset) ); // requires a 4GiB WAL */
				return sqlite3OsRead(pWal->pWalFd, pOut, (nOut>sz ? sz : nOut), iOffset);
			}

			*pInWal = 0;
			return SQLITE_OK;
	}

	Pgno sqlite3WalDbsize(Wal *pWal){
		if( pWal && ALWAYS(pWal->readLock>=0) ){
			return pWal->hdr.nPage;
		}
		return 0;
	}

	int sqlite3WalBeginWriteTransaction(Wal *pWal){
		int rc;

		/* Cannot start a write transaction without first holding a read
		** transaction. */
		assert( pWal->readLock>=0 );

		if( pWal->readOnly ){
			return SQLITE_READONLY;
		}

		/* Only one writer allowed at a time.  Get the write lock.  Return
		** SQLITE_BUSY if unable.
		*/
		rc = walLockExclusive(pWal, WAL_WRITE_LOCK, 1);
		if( rc ){
			return rc;
		}
		pWal->writeLock = 1;

		/* If another connection has written to the database file since the
		** time the read transaction on this connection was started, then
		** the write is disallowed.
		*/
		if( memcmp(&pWal->hdr, (void *)walIndexHdr(pWal), sizeof(WalIndexHdr))!=0 ){
			walUnlockExclusive(pWal, WAL_WRITE_LOCK, 1);
			pWal->writeLock = 0;
			rc = SQLITE_BUSY;
		}

		return rc;
	}

	int sqlite3WalEndWriteTransaction(Wal *pWal){
		if( pWal->writeLock ){
			walUnlockExclusive(pWal, WAL_WRITE_LOCK, 1);
			pWal->writeLock = 0;
			pWal->truncateOnCommit = 0;
		}
		return SQLITE_OK;
	}

	int sqlite3WalUndo(Wal *pWal, int (*xUndo)(void *, Pgno), void *pUndoCtx){
		int rc = SQLITE_OK;
		if( ALWAYS(pWal->writeLock) ){
			Pgno iMax = pWal->hdr.mxFrame;
			Pgno iFrame;

			/* Restore the clients cache of the wal-index header to the state it
			** was in before the client began writing to the database. 
			*/
			memcpy(&pWal->hdr, (void *)walIndexHdr(pWal), sizeof(WalIndexHdr));

			for(iFrame=pWal->hdr.mxFrame+1; 
				ALWAYS(rc==SQLITE_OK) && iFrame<=iMax; 
				iFrame++
				){
					/* This call cannot fail. Unless the page for which the page number
					** is passed as the second argument is (a) in the cache and 
					** (b) has an outstanding reference, then xUndo is either a no-op
					** (if (a) is false) or simply expels the page from the cache (if (b)
					** is false).
					**
					** If the upper layer is doing a rollback, it is guaranteed that there
					** are no outstanding references to any page other than page 1. And
					** page 1 is never written to the log until the transaction is
					** committed. As a result, the call to xUndo may not fail.
					*/
					assert( walFramePgno(pWal, iFrame)!=1 );
					rc = xUndo(pUndoCtx, walFramePgno(pWal, iFrame));
			}
			if( iMax!=pWal->hdr.mxFrame ) walCleanupHash(pWal);
		}
		assert( rc==SQLITE_OK );
		return rc;
	}

	void sqlite3WalSavepoint(Wal *pWal, u32 *aWalData){
		assert( pWal->writeLock );
		aWalData[0] = pWal->hdr.mxFrame;
		aWalData[1] = pWal->hdr.aFrameCksum[0];
		aWalData[2] = pWal->hdr.aFrameCksum[1];
		aWalData[3] = pWal->nCkpt;
	}

	int sqlite3WalSavepointUndo(Wal *pWal, u32 *aWalData){
		int rc = SQLITE_OK;

		assert( pWal->writeLock );
		assert( aWalData[3]!=pWal->nCkpt || aWalData[0]<=pWal->hdr.mxFrame );

		if( aWalData[3]!=pWal->nCkpt ){
			/* This savepoint was opened immediately after the write-transaction
			** was started. Right after that, the writer decided to wrap around
			** to the start of the log. Update the savepoint values to match.
			*/
			aWalData[0] = 0;
			aWalData[3] = pWal->nCkpt;
		}

		if( aWalData[0]<pWal->hdr.mxFrame ){
			pWal->hdr.mxFrame = aWalData[0];
			pWal->hdr.aFrameCksum[0] = aWalData[1];
			pWal->hdr.aFrameCksum[1] = aWalData[2];
			walCleanupHash(pWal);
		}

		return rc;
	}

	static int walRestartLog(Wal *pWal){
		int rc = SQLITE_OK;
		int cnt;

		if( pWal->readLock==0 ){
			volatile WalCkptInfo *pInfo = walCkptInfo(pWal);
			assert( pInfo->nBackfill==pWal->hdr.mxFrame );
			if( pInfo->nBackfill>0 ){
				u32 salt1;
				sqlite3_randomness(4, &salt1);
				rc = walLockExclusive(pWal, WAL_READ_LOCK(1), WAL_NREADER-1);
				if( rc==SQLITE_OK ){
					/* If all readers are using WAL_READ_LOCK(0) (in other words if no
					** readers are currently using the WAL), then the transactions
					** frames will overwrite the start of the existing log. Update the
					** wal-index header to reflect this.
					**
					** In theory it would be Ok to update the cache of the header only
					** at this point. But updating the actual wal-index header is also
					** safe and means there is no special case for sqlite3WalUndo()
					** to handle if this transaction is rolled back.
					*/
					int i;                    /* Loop counter */
					u32 *aSalt = pWal->hdr.aSalt;       /* Big-endian salt values */

					pWal->nCkpt++;
					pWal->hdr.mxFrame = 0;
					sqlite3Put4byte((u8*)&aSalt[0], 1 + sqlite3Get4byte((u8*)&aSalt[0]));
					aSalt[1] = salt1;
					walIndexWriteHdr(pWal);
					pInfo->nBackfill = 0;
					pInfo->aReadMark[1] = 0;
					for(i=2; i<WAL_NREADER; i++) pInfo->aReadMark[i] = READMARK_NOT_USED;
					assert( pInfo->aReadMark[0]==0 );
					walUnlockExclusive(pWal, WAL_READ_LOCK(1), WAL_NREADER-1);
				}else if( rc!=SQLITE_BUSY ){
					return rc;
				}
			}
			walUnlockShared(pWal, WAL_READ_LOCK(0));
			pWal->readLock = -1;
			cnt = 0;
			do{
				int notUsed;
				rc = walTryBeginRead(pWal, &notUsed, 1, ++cnt);
			}while( rc==WAL_RETRY );
			assert( (rc&0xff)!=SQLITE_BUSY ); /* BUSY not possible when useWal==1 */
			testcase( (rc&0xff)==SQLITE_IOERR );
			testcase( rc==SQLITE_PROTOCOL );
			testcase( rc==SQLITE_OK );
		}
		return rc;
	}

	typedef struct WalWriter {
		Wal *pWal;                   /* The complete WAL information */
		sqlite3_file *pFd;           /* The WAL file to which we write */
		sqlite3_int64 iSyncPoint;    /* Fsync at this offset */
		int syncFlags;               /* Flags for the fsync */
		int szPage;                  /* Size of one page */
	} WalWriter;

	static int walWriteToLog(
		WalWriter *p,              /* WAL to write to */
		void *pContent,            /* Content to be written */
		int iAmt,                  /* Number of bytes to write */
		sqlite3_int64 iOffset      /* Start writing at this offset */
		){
			int rc;
			if( iOffset<p->iSyncPoint && iOffset+iAmt>=p->iSyncPoint ){
				int iFirstAmt = (int)(p->iSyncPoint - iOffset);
				rc = sqlite3OsWrite(p->pFd, pContent, iFirstAmt, iOffset);
				if( rc ) return rc;
				iOffset += iFirstAmt;
				iAmt -= iFirstAmt;
				pContent = (void*)(iFirstAmt + (char*)pContent);
				assert( p->syncFlags & (SQLITE_SYNC_NORMAL|SQLITE_SYNC_FULL) );
				rc = sqlite3OsSync(p->pFd, p->syncFlags);
				if( iAmt==0 || rc ) return rc;
			}
			rc = sqlite3OsWrite(p->pFd, pContent, iAmt, iOffset);
			return rc;
	}

	static int walWriteOneFrame(
		WalWriter *p,               /* Where to write the frame */
		PgHdr *pPage,               /* The page of the frame to be written */
		int nTruncate,              /* The commit flag.  Usually 0.  >0 for commit */
		sqlite3_int64 iOffset       /* Byte offset at which to write */
		){
			int rc;                         /* Result code from subfunctions */
			void *pData;                    /* Data actually written */
			u8 aFrame[WAL_FRAME_HDRSIZE];   /* Buffer to assemble frame-header in */
#if defined(SQLITE_HAS_CODEC)
			if( (pData = sqlite3PagerCodec(pPage))==0 ) return SQLITE_NOMEM;
#else
			pData = pPage->pData;
#endif
			walEncodeFrame(p->pWal, pPage->pgno, nTruncate, pData, aFrame);
			rc = walWriteToLog(p, aFrame, sizeof(aFrame), iOffset);
			if( rc ) return rc;
			/* Write the page data */
			rc = walWriteToLog(p, pData, p->szPage, iOffset+sizeof(aFrame));
			return rc;
	}


	int sqlite3WalFrames(
		Wal *pWal,                      /* Wal handle to write to */
		int szPage,                     /* Database page-size in bytes */
		PgHdr *pList,                   /* List of dirty pages to write */
		Pgno nTruncate,                 /* Database size after this commit */
		int isCommit,                   /* True if this is a commit */
		int sync_flags                  /* Flags to pass to OsSync() (or 0) */
		){
			int rc;                         /* Used to catch return codes */
			u32 iFrame;                     /* Next frame address */
			PgHdr *p;                       /* Iterator to run through pList with. */
			PgHdr *pLast = 0;               /* Last frame in list */
			int nExtra = 0;                 /* Number of extra copies of last page */
			int szFrame;                    /* The size of a single frame */
			i64 iOffset;                    /* Next byte to write in WAL file */
			WalWriter w;                    /* The writer */

			assert( pList );
			assert( pWal->writeLock );

			/* If this frame set completes a transaction, then nTruncate>0.  If
			** nTruncate==0 then this frame set does not complete the transaction. */
			assert( (isCommit!=0)==(nTruncate!=0) );

#if defined(SQLITE_TEST) && defined(SQLITE_DEBUG)
			{ int cnt; for(cnt=0, p=pList; p; p=p->pDirty, cnt++){}
			WALTRACE(("WAL%p: frame write begin. %d frames. mxFrame=%d. %s\n",
				pWal, cnt, pWal->hdr.mxFrame, isCommit ? "Commit" : "Spill"));
			}
#endif

			/* See if it is possible to write these frames into the start of the
			** log file, instead of appending to it at pWal->hdr.mxFrame.
			*/
			if( SQLITE_OK!=(rc = walRestartLog(pWal)) ){
				return rc;
			}

			/* If this is the first frame written into the log, write the WAL
			** header to the start of the WAL file. See comments at the top of
			** this source file for a description of the WAL header format.
			*/
			iFrame = pWal->hdr.mxFrame;
			if( iFrame==0 ){
				u8 aWalHdr[WAL_HDRSIZE];      /* Buffer to assemble wal-header in */
				u32 aCksum[2];                /* Checksum for wal-header */

				sqlite3Put4byte(&aWalHdr[0], (WAL_MAGIC | SQLITE_BIGENDIAN));
				sqlite3Put4byte(&aWalHdr[4], WAL_MAX_VERSION);
				sqlite3Put4byte(&aWalHdr[8], szPage);
				sqlite3Put4byte(&aWalHdr[12], pWal->nCkpt);
				if( pWal->nCkpt==0 ) sqlite3_randomness(8, pWal->hdr.aSalt);
				memcpy(&aWalHdr[16], pWal->hdr.aSalt, 8);
				walChecksumBytes(1, aWalHdr, WAL_HDRSIZE-2*4, 0, aCksum);
				sqlite3Put4byte(&aWalHdr[24], aCksum[0]);
				sqlite3Put4byte(&aWalHdr[28], aCksum[1]);

				pWal->szPage = szPage;
				pWal->hdr.bigEndCksum = SQLITE_BIGENDIAN;
				pWal->hdr.aFrameCksum[0] = aCksum[0];
				pWal->hdr.aFrameCksum[1] = aCksum[1];
				pWal->truncateOnCommit = 1;

				rc = sqlite3OsWrite(pWal->pWalFd, aWalHdr, sizeof(aWalHdr), 0);
				WALTRACE(("WAL%p: wal-header write %s\n", pWal, rc ? "failed" : "ok"));
				if( rc!=SQLITE_OK ){
					return rc;
				}

				/* Sync the header (unless SQLITE_IOCAP_SEQUENTIAL is true or unless
				** all syncing is turned off by PRAGMA synchronous=OFF).  Otherwise
				** an out-of-order write following a WAL restart could result in
				** database corruption.  See the ticket:
				**
				**     http://localhost:591/sqlite/info/ff5be73dee
				*/
				if( pWal->syncHeader && sync_flags ){
					rc = sqlite3OsSync(pWal->pWalFd, sync_flags & SQLITE_SYNC_MASK);
					if( rc ) return rc;
				}
			}
			assert( (int)pWal->szPage==szPage );

			/* Setup information needed to write frames into the WAL */
			w.pWal = pWal;
			w.pFd = pWal->pWalFd;
			w.iSyncPoint = 0;
			w.syncFlags = sync_flags;
			w.szPage = szPage;
			iOffset = walFrameOffset(iFrame+1, szPage);
			szFrame = szPage + WAL_FRAME_HDRSIZE;

			/* Write all frames into the log file exactly once */
			for(p=pList; p; p=p->pDirty){
				int nDbSize;   /* 0 normally.  Positive == commit flag */
				iFrame++;
				assert( iOffset==walFrameOffset(iFrame, szPage) );
				nDbSize = (isCommit && p->pDirty==0) ? nTruncate : 0;
				rc = walWriteOneFrame(&w, p, nDbSize, iOffset);
				if( rc ) return rc;
				pLast = p;
				iOffset += szFrame;
			}

			/* If this is the end of a transaction, then we might need to pad
			** the transaction and/or sync the WAL file.
			**
			** Padding and syncing only occur if this set of frames complete a
			** transaction and if PRAGMA synchronous=FULL.  If synchronous==NORMAL
			** or synchonous==OFF, then no padding or syncing are needed.
			**
			** If SQLITE_IOCAP_POWERSAFE_OVERWRITE is defined, then padding is not
			** needed and only the sync is done.  If padding is needed, then the
			** final frame is repeated (with its commit mark) until the next sector
			** boundary is crossed.  Only the part of the WAL prior to the last
			** sector boundary is synced; the part of the last frame that extends
			** past the sector boundary is written after the sync.
			*/
			if( isCommit && (sync_flags & WAL_SYNC_TRANSACTIONS)!=0 ){
				if( pWal->padToSectorBoundary ){
					int sectorSize = sqlite3SectorSize(pWal->pWalFd);
					w.iSyncPoint = ((iOffset+sectorSize-1)/sectorSize)*sectorSize;
					while( iOffset<w.iSyncPoint ){
						rc = walWriteOneFrame(&w, pLast, nTruncate, iOffset);
						if( rc ) return rc;
						iOffset += szFrame;
						nExtra++;
					}
				}else{
					rc = sqlite3OsSync(w.pFd, sync_flags & SQLITE_SYNC_MASK);
				}
			}

			/* If this frame set completes the first transaction in the WAL and
			** if PRAGMA journal_size_limit is set, then truncate the WAL to the
			** journal size limit, if possible.
			*/
			if( isCommit && pWal->truncateOnCommit && pWal->mxWalSize>=0 ){
				i64 sz = pWal->mxWalSize;
				if( walFrameOffset(iFrame+nExtra+1, szPage)>pWal->mxWalSize ){
					sz = walFrameOffset(iFrame+nExtra+1, szPage);
				}
				walLimitSize(pWal, sz);
				pWal->truncateOnCommit = 0;
			}

			/* Append data to the wal-index. It is not necessary to lock the 
			** wal-index to do this as the SQLITE_SHM_WRITE lock held on the wal-index
			** guarantees that there are no other writers, and no data that may
			** be in use by existing readers is being overwritten.
			*/
			iFrame = pWal->hdr.mxFrame;
			for(p=pList; p && rc==SQLITE_OK; p=p->pDirty){
				iFrame++;
				rc = walIndexAppend(pWal, iFrame, p->pgno);
			}
			while( rc==SQLITE_OK && nExtra>0 ){
				iFrame++;
				nExtra--;
				rc = walIndexAppend(pWal, iFrame, pLast->pgno);
			}

			if( rc==SQLITE_OK ){
				/* Update the private copy of the header. */
				pWal->hdr.szPage = (u16)((szPage&0xff00) | (szPage>>16));
				testcase( szPage<=32768 );
				testcase( szPage>=65536 );
				pWal->hdr.mxFrame = iFrame;
				if( isCommit ){
					pWal->hdr.iChange++;
					pWal->hdr.nPage = nTruncate;
				}
				/* If this is a commit, update the wal-index header too. */
				if( isCommit ){
					walIndexWriteHdr(pWal);
					pWal->iCallback = iFrame;
				}
			}

			WALTRACE(("WAL%p: frame write %s\n", pWal, rc ? "failed" : "ok"));
			return rc;
	}

	int sqlite3WalCheckpoint(Wal *wal, int mode, int (*busy)(void*), void *busyArg, int sync_flags, int bufferLength, u8 *bufuffer, int *logs, int *checkpoints)
	{
		int rc;                         /* Return code */
		int isChanged = 0;              /* True if a new wal-index header is loaded */
		int eMode2 = eMode;             /* Mode to pass to walCheckpoint() */

		_assert(wal->CheckpointLock == 0);
		_assert(wal->WriteLock == 0);

		if (wal->ReadOnly) return RC::READONLY;
		WALTRACE("WAL%p: checkpoint begins\n", wal);
		rc = walLockExclusive(wal, WAL_CKPT_LOCK, 1);
		if (rc) // Usually this is SQLITE_BUSY meaning that another thread or process is already running a checkpoint, or maybe a recovery.  But it might also be SQLITE_IOERR.
			return rc;
		wal->CheckpointLock = 1;

		// If this is a blocking-checkpoint, then obtain the write-lock as well to prevent any writers from running while the checkpoint is underway.
		// This has to be done before the call to walIndexReadHdr() below.
		//
		// If the writer lock cannot be obtained, then a passive checkpoint is run instead. Since the checkpointer is not holding the writer lock,
		// there is no point in blocking waiting for any readers. Assuming no other error occurs, this function will return SQLITE_BUSY to the caller.
		if (mode !=SQLITE_CHECKPOINT_PASSIVE)
		{
			rc = walBusyLock(wal, busy, busyArg, WAL_WRITE_LOCK, 1);
			if (rc == RC::OK)
				wal->WriteLock = 1;
			else if (rc == RC::BUSY)
			{
				mode2 = SQLITE_CHECKPOINT_PASSIVE;
				rc = RC::OK;
			}
		}

		// Read the wal-index header.
		if (rc == RC::OK)
			rc = walIndexReadHdr(wal, &isChanged);

		// Copy data from the log to the database file.
		if (rc == RC::OK)
		{
			if (wal->hdr.mxFrame && walPagesize(wal) != nBuf)
				rc = SQLITE_CORRUPT_BKPT;
			else
				rc = walCheckpoint(wal, mode2, busy, busyArg, sync_flags, bufffer);

			// If no error occurred, set the output variables.
			if (rc == RC::OK || rc == RC::BUSY)
			{
				if (logs) *logs = (int)wal->hdr.mxFrame;
				if (checkpoints) *checkpoints = (int)(walCkptInfo(wal)->nBackfill);
			}
		}

		if (isChanged)
		{
			// If a new wal-index header was loaded before the checkpoint was performed, then the pager-cache associated with pWal is now
			// out of date. So zero the cached wal-index header to ensure that next time the pager opens a snapshot on this database it knows that
			// the cache needs to be reset.
			memset(&wal->Hdr, 0, sizeof(WalIndexHdr));
		}

		// Release the locks.
		sqlite3WalEndWriteTransaction(wal);
		walUnlockExclusive(wal, WAL_CKPT_LOCK, 1);
		wal->CheckpointLock = 0;
		WALTRACE("WAL%p: checkpoint %s\n", wal, rc ? "failed" : "ok");
		return (rc == RC::OK && mode != mode2 ? RC::BUSY : rc);
	}

	int sqlite3WalCallback(Wal *wal)
	{
		uint32 ret = 0;
		if (wal)
		{
			ret = wal->Callback;
			wal->Callback = 0;
		}
		return (int)ret;
	}


	RC sqlite3WalExclusiveMode(Wal *wal, int op)
	{
		_assert(wal->WriteLock == 0);
		_assert(wal->ExclusiveMode != WAL_HEAPMEMORY::MODE || op == -1);

		// pWal->readLock is usually set, but might be -1 if there was a prior error while attempting to acquire are read-lock. This cannot 
		// happen if the connection is actually in exclusive mode (as no xShmLock locks are taken in this case). Nor should the pager attempt to
		// upgrade to exclusive-mode following such an error.
		_assert(wal->ReadLock >= 0 || wal->LockError);
		_assert(wal->ReadLock >= 0 || (op <= 0 && wal->ExclusiveMode == 0));

		RC rc;
		if (op == 0)
		{
			if (wal->ExclusiveMode)
			{
				wal->exclusiveMode = 0;
				if (walLockShared(wal, WAL_READ_LOCK(wal->ReadLock)) != RC::OK)
					wal->ExclusiveMode = 1;
				rc = wal->ExclusiveMode == 0;
			}
			else // Already in locking_mode=NORMAL
				rc = RC::OK;
		}
		else if (op > 0)
		{
			_assert(wal->ExclusiveMode == 0);
			_assert(wal->ReadLock >= 0);
			walUnlockShared(wal, WAL_READ_LOCK(wal->ReadLock));
			wal->ExclusiveMode = 1;
			rc = 1;
		}
		else
			rc = wal->ExclusiveMode == RC::OK;
		return rc;
	}

	int sqlite3WalHeapMemory(Wal *wal)
	{
		return (wal && wal->ExclusiveMode == WAL_HEAPMEMORY::MODE);
	}

#ifdef ENABLE_ZIPVFS
	int Wal::Framesize(Wal *wal)
	{
		_assert(wal == nullptr || wal->ReadLock >= 0);
		return (wal ? wal->SizePage : 0);
	}
#endif
