// pager.c
#include "Core+Pager.cu.h"
using namespace Core;

namespace Core
{
	static const unsigned char _journalMagic[] = { 0xd9, 0xd5, 0x05, 0xf9, 0x20, 0xa1, 0x63, 0xd7 };

	// sqliteLimit.h
#define MAX_PAGE_SIZE 65536
#define DEFAULT_PAGE_SIZE 1024
#define MAX_DEFAULT_PAGE_SIZE 8192
#define MAX_PAGE_COUNT 1073741823

	class Pager
	{
	public:
		enum PAGER : char
		{
			OPEN = 0,
			READER = 1,
			WRITER_LOCKED = 2,
			WRITER_CACHEMOD = 3,
			WRITER_DBMOD = 4,
			WRITER_FINISHED = 5,
			ERROR = 6,
		};

#define MAX_SECTOR_SIZE 0x10000

		struct PagerSavepoint
		{
			int64 Offset;             // Starting offset in main journal
			int64 HdrOffset;          // See above
			Bitvec *InSavepoint;      // Set of pages in this savepoint
			Pid Orig;                 // Original number of pages in file
			Pid SubRec;               // Index of first record in sub-journal
#ifndef OMIT_WAL
			uint32 WalData[WAL_SAVEPOINT_NDATA];  // WAL savepoint context
#endif
		};

		VFileSystem Vfs;			// OS functions to use for IO
		bool ExclusiveMode;			// Boolean. True if locking_mode==EXCLUSIVE
		uint8 JournalMode;			// One of the PAGER_JOURNALMODE_* values
		byte UseJournal;			// Use a rollback journal on this file
		byte NoSync;				// Do not sync the journal if true
		bool FullSync;				// Do extra syncs of the journal for robustness
		VFile::SYNC CheckpointSyncFlags;	// SYNC_NORMAL or SYNC_FULL for checkpoint
		VFile::SYNC WalSyncFlags;	// SYNC_NORMAL or SYNC_FULL for wal writes
		VFile::SYNC SyncFlags;		// SYNC_NORMAL or SYNC_FULL otherwise
		bool TempFile;				// zFilename is a temporary file
		bool ReadOnly;				// True for a read-only database
		bool MemoryDB;				// True to inhibit all file I/O
		// The following block contains those class members that change during routine opertion.  Class members not in this block are either fixed
		// when the pager is first created or else only change when there is a significant mode change (such as changing the page_size, locking_mode,
		// or the journal_mode).  From another view, these class members describe the "state" of the pager, while other class members describe the "configuration" of the pager.
		PAGER State;                // Pager state (OPEN, READER, WRITER_LOCKED..)
		VFile::LOCK Lock;           // Current lock held on database file
		bool ChangeCountDone;       // Set after incrementing the change-counter
		bool SetMaster;             // True if a m-j name has been written to jrnl
		byte DoNotSpill;            // Do not spill the cache when non-zero
		byte DoNotSyncSpill;        // Do not do a spill that requires jrnl sync
		byte SubjInMemory;          // True to use in-memory sub-journals
		Pid DBSize;					// Number of pages in the database
		Pid DBOrigSize;				// dbSize before the current transaction
		Pid DBFileSize;				// Number of pages in the database file
		Pid DBHintSize;				// Value passed to FCNTL_SIZE_HINT call
		RC ErrorCode;               // One of several kinds of errors
		int _nRec;                  // Pages journalled since last j-header written
		uint32 ChecksumInit;        // Quasi-random value added to every checksum
		uint32 SubRecords;          // Number of records written to sub-journal
		Bitvec *InJournal;			// One bit for each page in the database file
		VFile *File;				// File descriptor for database
		VFile *JournalFile;			// File descriptor for main journal
		VFile *SubJournalFile;		// File descriptor for sub-journal
		int64 JournalOffset;        // Current write offset in the journal file
		int64 JournalHeader;        // Byte offset to previous journal header
		IBackup *Backup;			// Pointer to list of ongoing backup processes
		//int _0;					// Number of elements in Savepoint[]
		PagerSavepoint *Savepoints;	// Array of active savepoints
		char DBFileVersions[16];    // Changes whenever database file changes
		// End of the routinely-changing class members
		uint16 ExtraBytes;          // Add this many bytes to each in-memory page
		int16 ReserveBytes;         // Number of unused bytes at end of each page
		uint32 VfsFlags;            // Flags for sqlite3_vfs.xOpen()
		uint32 SectorSize;          // Assumed sector size during rollback
		int PageSize;               // Number of bytes in a page
		Pid MaxPid;					// Maximum allowed size of the database
		int64 JournalSizeLimit;     // Size limit for persistent journal files
		char *Filename;            // Name of the database file
		char *Journal;             // Name of the journal file
		int (*BusyHandler)(void*);	// Function to call when busy
		void *BusyHandlerArg;		// Context argument for xBusyHandler
		int Stats[3];               // Total cache hits, misses and writes
#ifdef TEST
		int Reads;                  // Database pages read
#endif
		void (*Reiniter)(IPage *);	// Call this routine when reloading pages
#ifdef HAS_CODEC
		void *(*Codec)(void *,void *, Pid, int);	// Routine for en/decoding data
		void (*CodecSizeChange)(void *, int, int);	// Notify of page size changes
		void (*CodecFree)(void *);					// Destructor for the codec
		void *CodecArg;								// First argument to xCodec... methods
#endif
		unsigned char *TmpSpace;	// Pager.pageSize bytes of space for tmp use
		PCache *PCache;				// Pointer to page cache object
#ifndef OMIT_WAL
		Wal *Wal;					// Write-ahead log used by "journal_mode=wal"
		char *WalName;              // File name for write-ahead log
#endif

		enum STAT : char
		{
			HIT = 0,
			MISS = 1,
			WRITE = 2,
		};

#define JOURNAL_PG_SZ(pager) ((pager->PageSize) + 8)
#define JOURNAL_HDR_SZ(pager) (pager->SectorSize)

#define PAGER_MAX_PID 2147483647

#ifndef OMIT_WAL
		static int UseWal(Pager *pager) { return (pager->Wal != nullptr); }
#else
#define UseWal(x) 0
#define RollbackWal(x) 0
#define WalFrames(v,w,x,y) 0
#define OpenWalIfPresent(z) SQLITE_OK
#define BeginReadTransaction(z) SQLITE_OK
#endif

#pragma region Debug
#if _DEBUG 

		static int assert_pager_state(Pager *p)
		{
			// State must be valid.
			_assert(p->State == PAGER::OPEN ||
				p->State == PAGER::READER ||
				p->State == PAGER::WRITER_LOCKED ||
				p->State == PAGER::WRITER_CACHEMOD ||
				p->State == PAGER::WRITER_DBMOD ||
				p->State == PAGER::WRITER_FINISHED ||
				p->State == PAGER::ERROR);

			// Regardless of the current state, a temp-file connection always behaves as if it has an exclusive lock on the database file. It never updates
			// the change-counter field, so the changeCountDone flag is always set.
			_assert(p->TempFile == 0 || p->Lock == VFile::LOCK::EXCLUSIVE);
			_assert(p->TempFile == 0 || p->ChangeCountDone);

			// If the useJournal flag is clear, the journal-mode must be "OFF". And if the journal-mode is "OFF", the journal file must not be open.
			_assert(p->JournalMode == IPager::JOURNALMODE::OFF || p->UseJournal);
			_assert(p->JournalMode != IPager::JOURNALMODE::OFF || !p->JournalFile->Opened);

			// Check that MEMDB implies noSync. And an in-memory journal. Since this means an in-memory pager performs no IO at all, it cannot encounter 
			// either SQLITE_IOERR or SQLITE_FULL during rollback or while finalizing a journal file. (although the in-memory journal implementation may 
			// return SQLITE_IOERR_NOMEM while the journal file is being written). It is therefore not possible for an in-memory pager to enter the ERROR state.
			if (p->MemoryDB)
			{
				_assert(p->NoSync);
				_assert(p->JournalMode == IPager::JOURNALMODE::OFF || p->JournalMode == IPager::JOURNALMODE::JMEMORY);
				_assert(p->State != PAGER::ERROR && p->State != PAGER::OPEN);
				_assert(!UseWal(p));
			}

			// If changeCountDone is set, a RESERVED lock or greater must be held on the file.
			_assert(p->ChangeCountDone == 0 || p->Lock >= VFile::LOCK::RESERVED);
			_assert(p->Lock != VFile::LOCK::PENDING);

			switch (p->State)
			{
			case PAGER::OPEN:
				_assert(!p->MemoryDB);
				_assert(p->ErrorCode == RC::OK);
				_assert(PCache_RefCount(p->PCache) == 0 || p->TempFile);
				break;

			case PAGER::READER:
				_assert(p->ErrorCode == RC::OK);
				_assert(p->Lock != VFile::LOCK::UNKNOWN);
				_assert(p->Lock >= VFile::LOCK::SHARED);
				break;

			case PAGER::WRITER_LOCKED:
				_assert(p->Lock != VFile::LOCK::UNKNOWN);
				_assert(p->ErrorCode == RC::OK);
				if (!UseWal(p))
					_assert(p->Lock >= VFile::LOCK::RESERVED);
				_assert(p->DBSize == p->DBOrigSize);
				_assert(p->DBOrigSize == p->DBFileSize);
				_assert(p->DBOrigSize == p->DBHintSize);
				_assert(!p->SetMaster);
				break;

			case PAGER::WRITER_CACHEMOD:
				_assert(p->Lock != VFile::LOCK::UNKNOWN);
				_assert(p->ErrorCode == RC::OK);
				if (!UseWal(p))
				{
					// It is possible that if journal_mode=wal here that neither the journal file nor the WAL file are open. This happens during
					// a rollback transaction that switches from journal_mode=off to journal_mode=wal.
					_assert(p->Lock >= VFile::LOCK::RESERVED);
					_assert(p->JournalFile->Opened || p->JournalMode == IPager::JOURNALMODE::OFF || p->JournalMode == IPager::JOURNALMODE::WAL);
				}
				_assert(p->DBOrigSize == p->DBFileSize);
				_assert(p->DBOrigSize == p->DBHintSize);
				break;

			case PAGER::WRITER_DBMOD:
				_assert(p->Lock == VFile::LOCK::EXCLUSIVE);
				_assert(p->ErrorCode == RC::OK);
				_assert(!UseWal(p));
				_assert(p->Lock >= VFile::LOCK::EXCLUSIVE);
				_assert(p->JournalFile->Opened || p->JournalMode == IPager::JOURNALMODE::OFF || p->JournalMode == IPager::JOURNALMODE::WAL);
				_assert(p->DBOrigSize <= p->DBHintSize);
				break;

			case PAGER::WRITER_FINISHED:
				_assert(p->Lock == VFile::LOCK::EXCLUSIVE);
				_assert(p->ErrorCode == RC::OK);
				_assert(!UseWal(p));
				_assert(p->JournalFile->Opened || p->JournalMode == IPager::JOURNALMODE::OFF || p->JournalMode == IPager::JOURNALMODE::WAL);
				break;

			case PAGER::ERROR:
				// There must be at least one outstanding reference to the pager if in ERROR state. Otherwise the pager should have already dropped back to OPEN state.
				_assert(p->ErrorCode != RC::OK);
				_assert(PCache_RefCount(p->PCache) > 0);
				break;
			}

			return true;
		}

		static char *print_pager_state(Pager *p)
		{
			static char r[1024];
			_snprintf(r, 1024,
				"Filename:      %s\n"
				"State:         %s errCode=%d\n"
				"Lock:          %s\n"
				"Locking mode:  locking_mode=%s\n"
				"Journal mode:  journal_mode=%s\n"
				"Backing store: tempFile=%d memDb=%d useJournal=%d\n"
				"Journal:       journalOff=%lld journalHdr=%lld\n"
				"Size:          dbsize=%d dbOrigSize=%d dbFileSize=%d\n"
				, p->Filename
				, p->State == PAGER::OPEN ? "OPEN" :
				p->State == PAGER::READER ? "READER" :
				p->State == PAGER::WRITER_LOCKED ? "WRITER_LOCKED" :
				p->State == PAGER::WRITER_CACHEMOD ? "WRITER_CACHEMOD" :
				p->State == PAGER::WRITER_DBMOD ? "WRITER_DBMOD" :
				p->State == PAGER::WRITER_FINISHED ? "WRITER_FINISHED" :
				p->State == PAGER::ERROR ? "ERROR" : "?error?"
				, (int)p->ErrorCode
				, p->Lock == VFile::LOCK::NO ? "NO_LOCK" :
				p->Lock == VFile::LOCK::RESERVED ? "RESERVED" :
				p->Lock == VFile::LOCK::EXCLUSIVE ? "EXCLUSIVE" :
				p->Lock == VFile::LOCK::SHARED ? "SHARED" :
				p->Lock == VFile::LOCK::UNKNOWN ? "UNKNOWN" : "?error?"
				, p->ExclusiveMode ? "exclusive" : "normal"
				, p->JournalMode == IPager::JOURNALMODE::JMEMORY ? "memory" :
				p->JournalMode == IPager::JOURNALMODE::OFF ? "off" :
				p->JournalMode == IPager::JOURNALMODE::DELETE ? "delete" :
				p->JournalMode == IPager::JOURNALMODE::PERSIST ? "persist" :
				p->JournalMode == IPager::JOURNALMODE::TRUNCATE ? "truncate" :
				p->JournalMode == IPager::JOURNALMODE::WAL ? "wal" : "?error?"
				, (int)p->TempFile, (int)p->MemoryDB, (int)p->UseJournal
				, p->JournalOffset, p->JournalHeader
				, (int)p->DBSize, (int)p->DBOrigSize, (int)p->DBFileSize);
			return r;
		}

#endif
#pragma endregion

#pragma region Name1

		static bool subjRequiresPage(PgHdr *pg)
		{
			Pid id = pg->ID;
			Pager *pager = pg->Pager;
			for (int i = 0; i < __arrayLength(pager->Savepoints); i++)
			{
				PagerSavepoint *p = &pager->Savepoints[i];
				if (p->Orig >= id && !p->InSavepoint->Get(id))
					return true;
			}
			return false;
		}

		static bool pageInJournal(PgHdr *pg)
		{
			return pg->Pager->InJournal->Get(pg->ID);
		}

		static int pagerUnlockDb(Pager *pager, VFile::LOCK lock)
		{
			_assert(!pager->ExclusiveMode || pager->Lock == lock);
			_assert(lock == VFile::LOCK::NO || lock == VFile::LOCK::SHARED);
			_assert(lock != VFile::LOCK::NO || !UseWal(pager));
			int rc = RC::OK;
			if (pager->File->Opened)
			{
				_assert(pager->Lock >= lock);
				rc = pager->File->Unlock(lock);
				if (pager->Lock != VFile::LOCK::UNKNOWN)
					pager->Lock = lock;
				SysEx_IOTRACE("UNLOCK %p %d\n", pager, lock);
			}
			return rc;
		}

		static int pagerLockDb(Pager *pager, VFile::LOCK lock)
		{
			_assert(lock == VFile::LOCK::SHARED || lock == VFile::LOCK::RESERVED || lock == VFile::LOCK::EXCLUSIVE);
			int rc = RC::OK;
			if (pager->Lock < lock || pager->Lock == VFile::LOCK::UNKNOWN)
			{
				rc = pager->File->Lock(lock);
				if (rc == RC::OK && (pager->Lock != VFile::LOCK::UNKNOWN || lock == VFile::LOCK::EXCLUSIVE))
				{
					pager->Lock = lock;
					SysEx_IOTRACE("LOCK %p %d\n", pager, lock);
				}
			}
			return rc;
		}

#ifdef ENABLE_ATOMIC_WRITE
		static int jrnlBufferSize(Pager *pager)
		{
			_assert(!pager->MemoryDB);
			if (!pager->TempFile)
			{
				_assert(pager->File->Opened);
				int dc = pager->File->get_DeviceCharacteristics();
				int sectorSize = pager->SectorSize;
				int sizePage = pager->PageSize;
				_assert(IOCAP_ATOMIC512 == (512 >> 8));
				_assert(IOCAP_ATOMIC64K == (65536 >> 8));
				if ((dc & (IOCAP_ATOMIC | (sizePage >> 8)) || sectorSize > sizePage) == 0)
					return 0;
			}
			return JOURNAL_HDR_SZ(pager) + JOURNAL_PG_SZ(pager);
		}
#endif

#ifdef CHECK_PAGES
		static uint32 pager_datahash(int bytes, unsigned char *data)
		{
			uint32 hash = 0;
			for (int i = 0; i < bytes; i++)
				hash = (hash * 1039) + data[i];
			return hash;
		}
		static uint32 pager_pagehash(PgHdr *page) { return pager_datahash(page->Pager->PageSize, (unsigned char *)page->Data); }
		static void pager_set_pagehash(PgHdr *page) { page->PageHash = pager_pagehash(page); }
		//#define CHECK_PAGE(x) checkPage(x)
		static void checkPage(PgHdr *page)
		{
			Pager *pager = page->Pager;
			_assert(pager->State != PAGER::ERROR);
			_assert((page->Flags & PgHdr::PGHDR::DIRTY) || page->PageHash == pager_pagehash(page));
		}
#else
#define pager_datahash(X, Y) 0
#define pager_pagehash(X) 0
#define pager_set_pagehash(X)
#define CHECK_PAGE(x)
#endif

#pragma endregion

#pragma region Journal1

		static RC readMasterJournal(VFile *journalFile, char *master, uint32 masterLength)
		{
			uint32 nameLength;		// Length in bytes of master journal name
			int64 fileSize;			// Total size in bytes of journal file pJrnl
			uint32 checksum;		// MJ checksum value read from journal
			unsigned char magic[8]; // A buffer to hold the magic header
			master[0] = '\0';
			RC rc;
			if ((rc = journalFile->get_FileSize(fileSize)) != RC::OK ||
				fileSize < 16 ||
				(rc = journalFile->Read4(fileSize - 16, &nameLength)) != RC::OK ||
				nameLength >= masterLength ||
				(rc = journalFile->Read4(fileSize - 12, &checksum)) != RC::OK ||
				(rc = journalFile->Read(magic, 8, fileSize - 8)) != RC::OK ||
				_memcmp(magic, _journalMagic, 8) ||
				(rc = journalFile->Read(master, nameLength, fileSize - 16 - nameLength)) != RC::OK)
				return rc;
			// See if the checksum matches the master journal name
			for (uint32 u = 0; u < nameLength; u++)
				checksum -= master[u];
			if (checksum)
			{
				// If the checksum doesn't add up, then one or more of the disk sectors containing the master journal filename is corrupted. This means
				// definitely roll back, so just return SQLITE_OK and report a (nul) master-journal filename.
				nameLength = 0;
			}
			master[nameLength] = '\0';
			return RC::OK;
		}

		static int64 journalHdrOffset(Pager *pager)
		{
			int64 offset = 0;
			int64 c = pager->JournalOffset;
			if (c)
				offset = ((c-1) / JOURNAL_HDR_SZ(pager) + 1) * JOURNAL_HDR_SZ(pager);
			_assert(offset % JOURNAL_HDR_SZ(pager) == 0);
			_assert(offset >= c);
			_assert((offset - c) < JOURNAL_HDR_SZ(pager));
			return offset;
		}

		static RC zeroJournalHdr(Pager *pager, bool doTruncate)
		{
			_assert(pager->JournalFile->Opened);
			RC rc = RC::OK;
			if (pager->JournalOffset)
			{
				static const char zeroHeader[28] = { 0 };
				const int64 limit = pager->JournalSizeLimit; // Local cache of jsl
				SysEx_IOTRACE("JZEROHDR %p\n", pager);
				if (doTruncate || limit == 0)
					rc = pager->JournalFile->Truncate(0);
				else
					rc = pager->JournalFile->Write(zeroHeader, sizeof(zeroHeader), 0);
				if (rc == RC::OK && !pager->NoSync)
					rc = pager->JournalFile->Sync(VFile::SYNC::DATAONLY | pager->SyncFlags);
				// At this point the transaction is committed but the write lock is still held on the file. If there is a size limit configured for 
				// the persistent journal and the journal file currently consumes more space than that limit allows for, truncate it now. There is no need
				// to sync the file following this operation.
				if (rc == RC::OK && limit > 0)
				{
					int64 fileSize;
					rc = pager->JournalFile->get_FileSize(fileSize);
					if (rc == RC::OK && fileSize > limit)
						rc = pager->JournalFile->Truncate(limit);
				}
			}
			return rc;
		}

		static RC writeJournalHdr(Pager *pager)
		{
			_assert(pager->JournalFile->Opened); 
			unsigned char *header = pager->TmpSpace;		// Temporary space used to build header
			uint32 headerSize = (uint32)pager->PageSize;	// Size of buffer pointed to by zHeader
			if (headerSize > JOURNAL_HDR_SZ(pager))
				headerSize = JOURNAL_HDR_SZ(pager);

			// If there are active savepoints and any of them were created since the most recent journal header was written, update the PagerSavepoint.iHdrOffset fields now.
			for (int ii = 0; ii < __arrayLength(pager->Savepoints); ii++)
				if (pager->Savepoints[ii].HdrOffset == 0)
					pager->Savepoints[ii].HdrOffset = pager->JournalOffset;
			pager->JournalHeader = pager->JournalOffset = journalHdrOffset(pager);

			// Write the nRec Field - the number of page records that follow this journal header. Normally, zero is written to this value at this time.
			// After the records are added to the journal (and the journal synced, if in full-sync mode), the zero is overwritten with the true number
			// of records (see syncJournal()).
			//
			// A faster alternative is to write 0xFFFFFFFF to the nRec field. When reading the journal this value tells SQLite to assume that the
			// rest of the journal file contains valid page records. This assumption is dangerous, as if a failure occurred whilst writing to the journal
			// file it may contain some garbage data. There are two scenarios where this risk can be ignored:
			//   * When the pager is in no-sync mode. Corruption can follow a power failure in this case anyway.
			//   * When the SQLITE_IOCAP_SAFE_APPEND flag is set. This guarantees that garbage data is never appended to the journal file.
			_assert(pager->File->Opened || pager->NoSync);
			if (pager->NoSync || (pager->JournalMode == IPager::JOURNALMODE::JMEMORY) || (pager->File->get_DeviceCharacteristics() & VFile::IOCAP::SAFE_APPEND) != 0)
			{
				_memcpy(header, _journalMagic, sizeof(_journalMagic));
				ConvertEx::Put4(&header[sizeof(header)], 0xffffffff);
			}
			else
				_memset(header, 0, sizeof(_journalMagic) + 4);
			SysEx_MakeRandomness(sizeof(pager->ChecksumInit), &pager->ChecksumInit);
			ConvertEx::Put4(&header[sizeof(_journalMagic) + 4], pager->ChecksumInit);	// The random check-hash initializer
			ConvertEx::Put4(&header[sizeof(_journalMagic) + 8], pager->DBOrigSize);		// The initial database size
			ConvertEx::Put4(&header[sizeof(_journalMagic) + 12], pager->SectorSize);	// The assumed sector size for this process
			ConvertEx::Put4(&header[sizeof(_journalMagic) + 16], pager->PageSize);		// The page size
			// Initializing the tail of the buffer is not necessary.  Everything works find if the following memset() is omitted.  But initializing
			// the memory prevents valgrind from complaining, so we are willing to take the performance hit.
			_memset(&header[sizeof(_journalMagic) + 20], 0, headerSize - (sizeof(_journalMagic) + 20));

			// In theory, it is only necessary to write the 28 bytes that the journal header consumes to the journal file here. Then increment the 
			// Pager.journalOff variable by JOURNAL_HDR_SZ so that the next record is written to the following sector (leaving a gap in the file
			// that will be implicitly filled in by the OS).
			//
			// However it has been discovered that on some systems this pattern can be significantly slower than contiguously writing data to the file,
			// even if that means explicitly writing data to the block of (JOURNAL_HDR_SZ - 28) bytes that will not be used. So that is what is done. 
			//
			// The loop is required here in case the sector-size is larger than the database page size. Since the zHeader buffer is only Pager.pageSize
			// bytes in size, more than one call to sqlite3OsWrite() may be required to populate the entire journal header sector. 
			RC rc = RC::OK;
			for (uint32 headerWritten = 0; rc == RC::OK && headerWritten < JOURNAL_HDR_SZ(pager); headerWritten += headerSize)
			{
				SysEx_IOTRACE("JHDR %p %lld %d\n", pager, pager->JournalHeader, headerSize);
				rc = pager->JournalFile->Write(header, headerSize, pager->JournalOffset);
				_assert(pager->JournalHeader <= pager->JournalOffset);
				pager->JournalOffset += headerSize;
			}
			return rc;
		}

		static RC readJournalHdr(Pager *pager, int isHot, int64 journalSize, uint32 *recordsOut, uint32 *dbSizeOut)
		{
			_assert(pager->JournalFile->Opened);

			// Advance Pager.journalOff to the start of the next sector. If the journal file is too small for there to be a header stored at this
			// point, return SQLITE_DONE.
			pager->JournalOffset = journalHdrOffset(pager);
			if (pager->JournalOffset + JOURNAL_HDR_SZ(pager) > journalSize)
				return RC::DONE;
			int64 headerOffset = pager->JournalOffset;

			// Read in the first 8 bytes of the journal header. If they do not match the  magic string found at the start of each journal header, return
			// SQLITE_DONE. If an IO error occurs, return an error code. Otherwise, proceed.
			RC rc;
			unsigned char magic[8];
			if (isHot || headerOffset != pager->JournalHeader)
			{
				rc = pager->JournalFile->Read(magic, sizeof(magic), headerOffset);
				if (rc)
					return rc;
				if (_memcmp(magic, _journalMagic, sizeof(magic)) != 0)
					return RC::DONE;
			}

			// Read the first three 32-bit fields of the journal header: The nRec field, the checksum-initializer and the database size at the start
			// of the transaction. Return an error code if anything goes wrong.
			if ((rc = pager->JournalFile->Read4(headerOffset + 8, recordsOut)) != RC::OK ||
				(rc = pager->JournalFile->Read4(headerOffset + 12, &pager->ChecksumInit)) != RC::OK ||
				(rc = pager->JournalFile->Read4(headerOffset + 16, dbSizeOut)) != RC::OK)
				return rc;

			if (pager->JournalOffset == 0)
			{
				uint32 pageSize;	// Page-size field of journal header
				uint32 sectorSize;	// Sector-size field of journal header
				// Read the page-size and sector-size journal header fields.
				if ((rc = pager->JournalFile->Read4(headerOffset + 20, &sectorSize)) != RC::OK ||
					(rc = pager->JournalFile->Read4(headerOffset + 24, &pageSize)) != RC::OK)
					return rc;

				// Versions of SQLite prior to 3.5.8 set the page-size field of the journal header to zero. In this case, assume that the Pager.pageSize
				// variable is already set to the correct page size.
				if (pageSize == 0)
					pageSize = pager->PageSize;

				// Check that the values read from the page-size and sector-size fields are within range. To be 'in range', both values need to be a power
				// of two greater than or equal to 512 or 32, and not greater than their respective compile time maximum limits.
				if (pageSize < 512 || sectorSize < 32 ||
					pageSize > MAX_PAGE_SIZE || sectorSize > MAX_SECTOR_SIZE ||
					((pageSize - 1) & pageSize) != 0 || ((sectorSize - 1) & sectorSize) != 0)
					// If the either the page-size or sector-size in the journal-header is invalid, then the process that wrote the journal-header must have 
					// crashed before the header was synced. In this case stop reading the journal file here.
					return RC::DONE;

				// Update the page-size to match the value read from the journal. Use a testcase() macro to make sure that malloc failure within PagerSetPagesize() is tested.
				rc = SetPagesize(pager, &pageSize, -1);
				ASSERTCOVERAGE(rc != RC::OK);

				// Update the assumed sector-size to match the value used by the process that created this journal. If this journal was
				// created by a process other than this one, then this routine is being called from within pager_playback(). The local value
				// of Pager.sectorSize is restored at the end of that routine.
				pager->SectorSize = sectorSize;
			}

			pager->JournalOffset += JOURNAL_HDR_SZ(pager);
			return rc;
		}

		static int writeMasterJournal(Pager *pager, const char *master)
		{
			_assert(!pager->SetMaster);
			_assert(!UseWal(pager));

			if (!master ||
				pager->JournalMode == IPager::JOURNALMODE::JMEMORY ||
				pager->JournalMode == IPager::JOURNALMODE::OFF)
				return RC::OK;
			pager->SetMaster = true;
			_assert(pager->JournalFile->Opened);
			_assert(pager->JournalHeader <= pager->JournalOffset);

			// Calculate the length in bytes and the checksum of zMaster
			uint32 checksum = 0;	// Checksum of string zMaster
			int masterLength;		// Length of string zMaster
			for (masterLength = 0; master[masterLength]; masterLength++)
				checksum += master[masterLength];

			// If in full-sync mode, advance to the next disk sector before writing the master journal name. This is in case the previous page written to
			// the journal has already been synced.
			if (pager->FullSync)
				pager->JournalOffset = journalHdrOffset(pager);
			int64 headerOffset = pager->JournalOffset; // Offset of header in journal file

			// Write the master journal data to the end of the journal file. If an error occurs, return the error code to the caller.
			RC rc;
			if ((rc = pager->JournalFile->Write4(headerOffset, PAGER_MJ_PID(pager))) != RC::OK ||
				(rc = pager->JournalFile->Write(master, masterLength, headerOffset+4)) != RC::OK ||
				(rc = pager->JournalFile->Write4(headerOffset + 4 + masterLength, masterLength)) != RC::OK ||
				(rc = pager->JournalFile->Write4(headerOffset + 4 + masterLength + 4, checksum)) != RC::OK ||
				(rc = pager->JournalFile->Write(_journalMagic, 8, headerOffset + 4 + masterLength + 8)) != RC::OK)
				return rc;
			pager->JournalOffset += (masterLength + 20);

			// If the pager is in peristent-journal mode, then the physical journal-file may extend past the end of the master-journal name
			// and 8 bytes of magic data just written to the file. This is dangerous because the code to rollback a hot-journal file
			// will not be able to find the master-journal name to determine whether or not the journal is hot. 
			//
			// Easiest thing to do in this scenario is to truncate the journal file to the required size. 
			int64 journalSize;	// Size of journal file on disk
			if ((rc = pager->JournalFile->get_FileSize(journalSize)) == RC::OK && journalSize > pager->JournalOffset)
				rc = pager->JournalFile->Truncate(pager->JournalOffset);
			return rc;
		}

#pragma endregion

#pragma region Name2

		static PgHdr *pager_lookup(Pager *pager, Pid id)
		{
			PgHdr *p;
			// It is not possible for a call to PcacheFetch() with createFlag==0 to fail, since no attempt to allocate dynamic memory will be made.
			Pcache_Fetch(pager->PCache, id, 0, &p);
			return p;
		}

		// Discard the entire contents of the in-memory page-cache.
		static void pager_reset(Pager *pager)
		{
			pager->Backup->Restart();
			Pcache_Clear(pager->PCache);
		}

		// Free all structures in the Pager.aSavepoint[] array and set both Pager.aSavepoint and Pager.nSavepoint to zero. Close the sub-journal
		// if it is open and the pager is not in exclusive mode.
		static void releaseAllSavepoints(Pager *pager)
		{
			for (int ii = 0; ii < __arrayLength(pager->Savepoints); ii++)
				Bitvec::Destroy(pager->Savepoints[ii].InSavepoint);
			if (!pager->ExclusiveMode || pager->SubJournalFile->IsMemJournal)
				pager->SubJournalFile->Close();
			SysEx::Free(pager->Savepoints);
			pager->Savepoints = nullptr;
			pager->SubRecords = 0;
		}

		static RC addToSavepointBitvecs(Pager *pager, Pid id)
		{
			int rc = RC::OK;
			for (int ii = 0; ii < __arrayLength(pager->Savepoint); ii++)
			{
				PagerSavepoint *p = &pager->Savepoints[ii];
				if (id <= p->Orig)
				{
					rc |= p->InSavepoint->Set(id);
					ASSERTCOVERAGE(rc == RC::NOMEM);
					_assert(rc == RC::OK || rc == RC::NOMEM);
				}
			}
			return (RC)rc;
		}

		static void pager_unlock(Pager *pager)
		{
			_assert(pager->State==PAGER::READER ||
				pager->State==PAGER::OPEN ||
				pager->State==PAGER::ERROR);

			Bitvec::Destroy(pager->InJournal);
			pager->InJournal = nullptr;
			releaseAllSavepoints(pager);

			if (UseWal(pager))
			{
				_assert(!pager->JournalFile->Opened);
				WalEndReadTransaction(pager->Wal);
				pager->State = PAGER::OPEN;
			}
			else if (!pager->ExclusiveMode)
			{
				// If the operating system support deletion of open files, then close the journal file when dropping the database lock.  Otherwise
				// another connection with journal_mode=delete might delete the file out from under us.
				_assert((IPager::JOURNALMODE::JMEMORY & 5) != 1);
				_assert((IPager::JOURNALMODE::OFF & 5) != 1);
				_assert((IPager::JOURNALMODE::WAL & 5) != 1);
				_assert((IPager::JOURNALMODE::DELETE & 5) != 1);
				_assert((IPager::JOURNALMODE::TRUNCATE & 5) == 1);
				_assert((IPager::JOURNALMODE::PERSIST & 5) == 1);
				int dc = (pager->File->Opened ? pager->File->get_DeviceCharacteristics() : 0);
				if ((dc & IOCAP_UNDELETABLE_WHEN_OPEN) == 0 || (pager->JournalMode & 5) != 1)
					pager->JournalFile->Close();

				// If the pager is in the ERROR state and the call to unlock the database file fails, set the current lock to UNKNOWN_LOCK. See the comment
				// above the #define for UNKNOWN_LOCK for an explanation of why this is necessary.
				RC rc = pagerUnlockDb(pager, VFile::LOCK::NO);
				if (rc != RC::OK && pager->State == PAGER::ERROR)
					pager->Lock = VFile::LOCK::UNKNOWN;

				// The pager state may be changed from PAGER_ERROR to PAGER_OPEN here without clearing the error code. This is intentional - the error
				// code is cleared and the cache reset in the block below.
				_assert(pager->ErrorCode || pager->State != PAGER::ERROR);
				pager->ChangeCountDone = 0;
				pager->State = PAGER::OPEN;
			}

			// If Pager.errCode is set, the contents of the pager cache cannot be trusted. Now that there are no outstanding references to the pager,
			// it can safely move back to PAGER_OPEN state. This happens in both normal and exclusive-locking mode.
			if (pager->ErrorCode)
			{
				_assert(!pager->MemoryDB);
				pager_reset(pager);
				pager->ChangeCountDone = pager->TempFile;
				pager->State = PAGER::OPEN;
				pager->ErrorCode = RC::OK;
			}

			pager->JournalOffset = 0;
			pager->JournalHeader = 0;
			pager->SetMaster = false;
		}

		static RC pager_error(Pager *pager, RC rc)
		{
			RC rc2 = (rc & 0xff);
			_assert(rc == RC::OK || !pager->MemoryDB);
			_assert(pager->ErrorCode == RC::FULL ||
				pager->ErrorCode == RC::OK ||
				(pager->ErrorCode & 0xff) == RC::IOERR);
			if (rc2 == RC::FULL || rc2 == RC::IOERR)
			{
				pager->ErrorCode = rc;
				pager->State = PAGER::ERROR;
			}
			return rc;
		}

#pragma endregion

#pragma region Transaction1

		static int pager_truncate(Pager *pager, Pid pages);

		static int pager_end_transaction(Pager *pager, int hasMaster, bool commit)
		{
			// Do nothing if the pager does not have an open write transaction or at least a RESERVED lock. This function may be called when there
			// is no write-transaction active but a RESERVED or greater lock is held under two circumstances:
			//
			//   1. After a successful hot-journal rollback, it is called with eState==PAGER_NONE and eLock==EXCLUSIVE_LOCK.
			//
			//   2. If a connection with locking_mode=exclusive holding an EXCLUSIVE lock switches back to locking_mode=normal and then executes a
			//      read-transaction, this function is called with eState==PAGER_READER and eLock==EXCLUSIVE_LOCK when the read-transaction is closed.
			_assert(assert_pager_state(pager));
			_assert(pager->State != PAGER::ERROR);
			if (pager->State < PAGER::WRITER_LOCKED && pager->Lock < VFile::LOCK::RESERVED)
				return RC::OK;

			releaseAllSavepoints(pager);
			_assert(pager->JournalFile->Opened || pager->InJournal == nullptr);
			RC rc = RC::OK;
			if (pager->JournalFile->Opened)
			{
				_assert(!UseWal(pager));

				// Finalize the journal file.
				if (pager->JournalFile->IsMemJournal)
				{
					_assert(pager->JournalMode == IPager::JOURNALMODE::JMEMORY);
					pager->JournalFile->Close();
				}
				else if (pager->JournalMode == IPager::JOURNALMODE::TRUNCATE)
				{
					rc = (pager->JournalOffset == 0 ? RC::OK : pager->JournalFile->Truncate(0));
					pager->JournalOffset = 0;
				}
				else if (pager->JournalMode == IPager::JOURNALMODE::PERSIST || (pager->ExclusiveMode && pager->JournalMode != IPager::JOURNALMODE::WAL))
				{
					rc = zeroJournalHdr(pager, hasMaster);
					pager->JournalOffset = 0;
				}
				else
				{
					// This branch may be executed with Pager.journalMode==MEMORY if a hot-journal was just rolled back. In this case the journal
					// file should be closed and deleted. If this connection writes to the database file, it will do so using an in-memory journal. 
					bool delete_ = (!pager->TempFile && pager->JournalFile->JournalExists());
					_assert(pager->JournalMode == IPager::JOURNALMODE::DELETE ||
						pager->JournalMode == IPager::JOURNALMODE::JMEMORY ||
						pager->JournalMode == IPager::JOURNALMODE::WAL);
					pager->JournalFile->Close();
					if (delete_)
						pager->Vfs->Delete(pager->Journal, 0);
				}
			}

#ifdef CHECK_PAGES
			Pcache_IterateDirty(pager->PCache, pager_set_pagehash);
			if (pager->DBSize == 0 && Pcache_RefCount(pager->PCache) > 0)
			{
				PgHdr *p = pager_lookup(pager, 1);
				if (p)
				{
					p->PageHash = 0;
					Pager_Unref(p);
				}
			}
#endif

			Bitvec::Destroy(pager->InJournal); pager->InJournal = nullptr;
			pager->Records = 0;
			PCache_CleanAll(pager->PCache);
			PCache_Truncate(pager->PCache, pager->DBSize);

			RC rc2 = RC::OK;
			if (UseWal(pager))
			{
				// Drop the WAL write-lock, if any. Also, if the connection was in locking_mode=exclusive mode but is no longer, drop the EXCLUSIVE 
				// lock held on the database file.
				rc2 = WalEndWriteTransaction(pager->Wal);
				_assert(rc2 == RC::OK);
			}
			else if (rc == RC::OK && commit && pager->DBFileSize > pager->DBSize)
			{
				// This branch is taken when committing a transaction in rollback-journal mode if the database file on disk is larger than the database image.
				// At this point the journal has been finalized and the transaction successfully committed, but the EXCLUSIVE lock is still held on the
				// file. So it is safe to truncate the database file to its minimum required size.
				_assert(pager->Lock == VFile::LOCK::EXCLUSIVE);
				rc = pager_truncate(pager, pager->DBSize);
			}

			if (!pager->ExclusiveMode && (!UseWal(pager) || WalExclusiveMode(pager->Wal, 0)))
			{
				rc2 = pagerUnlockDb(pager, VFile::LOCK::SHARED);
				pager->ChangeCountDone = 0;
			}
			pager->State = PAGER::READER;
			pager->SetMaster = false;

			return (rc == RC::OK ? rc2 : rc);
		}

		static void pagerUnlockAndRollback(Pager *pager)
		{
			if (pager->State != PAGER::ERROR && pager->State != PAGER::OPEN)
			{
				_assert(assert_pager_state(pager));
				if (pager->State >= PAGER::WRITER_LOCKED)
				{
					SysEx::BeginBenignMalloc();
					sqlite3PagerRollback(pager);
					SysEx::EndBenignMalloc();
				}
				else if (!pager->ExclusiveMode)
				{
					_assert(pager->State == PAGER::READER);
					pager_end_transaction(pager, 0, 0);
				}
			}
			pager_unlock(pager);
		}

		static uint32 pager_cksum(Pager *pager, const uint8 *data)
		{
			uint32 checksum = pager->ChecksumInit;
			int i = pager->PageSize - 200;
			while (i > 0)
			{
				checksum += data[i];
				i -= 200;
			}
			return checksum;
		}

#ifdef HAS_CODEC
		static void pagerReportSize(Pager *pager)
		{
			if (pager->CodecSizeChange)
				pager->CodecSizeChange(pager->Codec, pager->PageSize, (int)pager->Reserve);
		}
#else
#define pagerReportSize(X)
#endif

		static int pager_playback_one_page(Pager *pager, int64 *offset, Bitvec *done, int isMainJournal, int isSavepoint)
		{
			_assert((isMainJournal & ~1) == 0);    // isMainJrnl is 0 or 1
			_assert((isSavepoint & ~1) == 0);     // isSavepnt is 0 or 1
			_assert(isMainJrnl || done);		// pDone always used on sub-journals
			_assert(isSavepnt || done == 0);	// pDone never used on non-savepoint

			char *data = pager->TmpSpace; // Temporary storage for the page
			_assert(data); // Temp storage must have already been allocated
			_assert(!UseWal(pager) || (!isMainJournal && isSavepoint));

			// Either the state is greater than PAGER_WRITER_CACHEMOD (a transaction or savepoint rollback done at the request of the caller) or this is
			// a hot-journal rollback. If it is a hot-journal rollback, the pager is in state OPEN and holds an EXCLUSIVE lock. Hot-journal rollback
			// only reads from the main journal, not the sub-journal.
			_assert(pager->State >= PAGER::WRITER_CACHEMOD || (pager->State == PAGER::OPEN && pager->Lock == VFile::LOCK::EXCLUSIVE));
			_assert(pager->State >= PAGER::WRITER_CACHEMOD || isMainJournal);

			// Read the page number and page data from the journal or sub-journal file. Return an error code to the caller if an IO error occurs.
			VFile *journalFile = (isMainJournal ? pager->JournalFile : pager->SubJournalFile); // The file descriptor for the journal file
			Pid id; // The page number of a page in journal
			RC rc = journalFile->Read4(*offset, &id);
			if (rc != RC::OK) return rc;
			rc = journalFile->Read((uint8 *)data, pager->PageSize, (*offset) + 4);
			if (rc != RC::OK) return rc;
			*offset += pager->PageSize + 4 + isMainJournal * 4;

			// Sanity checking on the page.  This is more important that I originally thought.  If a power failure occurs while the journal is being written,
			// it could cause invalid data to be written into the journal.  We need to detect this invalid data (with high probability) and ignore it.
			if (id == 0 || id == PAGER_MJ_PGNO(pager))
			{
				_assert(!isSavepoint);
				return RC::DONE;
			}
			if (id > (Pid)pager->DBSize || done->Get(id))
				return RC::OK;
			if (isMainJournal)
			{
				uint32 checksum; // Checksum used for sanity checking
				rc = journalFile->Read4((*offset) - 4, &checksum);
				if (rc) return rc;
				if (!isSavepnt && pager_cksum(pager, (uint8*)data) != checksum)
					return RC::DONE;
			}

			// If this page has already been played by before during the current rollback, then don't bother to play it back again.
			if (done && (rc = done->Set(id)) != RC::OK)
				return rc;

			// When playing back page 1, restore the nReserve setting
			if (id == 1 && pager->Reserves != ((uint8 *)data)[20])
			{
				pager->Reserves = ((uint8 *)data)[20];
				pagerReportSize(pager);
			}

			// If the pager is in CACHEMOD state, then there must be a copy of this page in the pager cache. In this case just update the pager cache,
			// not the database file. The page is left marked dirty in this case.
			//
			// An exception to the above rule: If the database is in no-sync mode and a page is moved during an incremental vacuum then the page may
			// not be in the pager cache. Later: if a malloc() or IO error occurs during a Movepage() call, then the page may not be in the cache
			// either. So the condition described in the above paragraph is not assert()able.
			//
			// If in WRITER_DBMOD, WRITER_FINISHED or OPEN state, then we update the pager cache if it exists and the main file. The page is then marked 
			// not dirty. Since this code is only executed in PAGER_OPEN state for a hot-journal rollback, it is guaranteed that the page-cache is empty
			// if the pager is in OPEN state.
			//
			// Ticket #1171:  The statement journal might contain page content that is different from the page content at the start of the transaction.
			// This occurs when a page is changed prior to the start of a statement then changed again within the statement.  When rolling back such a
			// statement we must not write to the original database unless we know for certain that original page contents are synced into the main rollback
			// journal.  Otherwise, a power loss might leave modified data in the database file without an entry in the rollback journal that can
			// restore the database to its original form.  Two conditions must be met before writing to the database files. (1) the database must be
			// locked.  (2) we know that the original page content is fully synced in the main journal either because the page is not in cache or else
			// the page is marked as needSync==0.
			//
			// 2008-04-14:  When attempting to vacuum a corrupt database file, it is possible to fail a statement on a database that does not yet exist.
			// Do not attempt to write if database file has never been opened.
			PgHdr *pg = (UseWal(pager) ? nullptr : pager_lookup(pager, id)); // An existing page in the cache
			_assert(pg || !pager->MemoryDB);
			_assert(pager->State != PAGER::OPEN || pg == 0);
			PAGERTRACE("PLAYBACK %d page %d hash(%08x) %s\n", PAGERID(pager), id, pager_datahash(pager->PageSize, (uint8 *)data), (isMainJournal ? "main-journal" : "sub-journal"));
			bool isSynced; // True if journal page is synced
			if (isMainJournal)
				isSynced = pager->NoSync || (*offset <= pager->JournalHeader);
			else
				isSynced = (pg == nullptr || (pg->Flags & PgHdr::PGHDR::NEED_SYNC) == 0);
			if (pager->File->Opened && (pager->State >= PAGER::WRITER_DBMOD || pager->State == PAGER::OPEN) && isSynced)
			{
				int64 offset = (id - 1) * (int64)pager->PageSize;
				ASSERTCOVERAGE(!isSavepoint && pg != nullptr && (pg->Flags & PgHdr::PGHDR::NEED_SYNC) != 0);
				_assert(!UseWal(pager));
				rc = pager->File->Write((uint8 *)data, pager->PageSize, offset);
				if (id > pager->DBFileSize)
					pager->DBFileSize = id;
				if (pager->Backup)
				{
					CODEC1(pager, data, id, 3, rc = RC::NOMEM);
					pager->Backup->Update(id, (uint8 *)data);
					CODEC2(pager, data, id, 7, rc = RC::NOMEM, data);
				}
			}
			else if (!isMainJournal && pg == nullptr)
			{
				// If this is a rollback of a savepoint and data was not written to the database and the page is not in-memory, there is a potential
				// problem. When the page is next fetched by the b-tree layer, it will be read from the database file, which may or may not be 
				// current. 
				//
				// There are a couple of different ways this can happen. All are quite obscure. When running in synchronous mode, this can only happen 
				// if the page is on the free-list at the start of the transaction, then populated, then moved using sqlite3PagerMovepage().
				//
				// The solution is to add an in-memory page to the cache containing the data just read from the sub-journal. Mark the page as dirty 
				// and if the pager requires a journal-sync, then mark the page as requiring a journal-sync before it is written.
				_assert(isSavepoint);
				_assert(pager->DoNotSpill == 0);
				pager->DoNotSpill++;
				rc = sqlite3PagerAcquire(pager, id, &pg, 1);
				_assert(pager->DoNotSpill == 1);
				pager->DoNotSpill--;
				if (rc != RC::OK) return rc;
				pg->Flags &= ~PgHdr::PGHDR::NEED_READ;
				Pcache_MakeDirty(pg);
			}
			if (pg)
			{
				// No page should ever be explicitly rolled back that is in use, except for page 1 which is held in use in order to keep the lock on the
				// database active. However such a page may be rolled back as a result of an internal error resulting in an automatic call to
				// sqlite3PagerRollback().
				void *pageData = pg->Data;
				_memcpy(pageData, (uint8 *)data, pager->PageSize);
				pager->Reiniter(pg);
				if (isMainJournal && (!isSavepoint || *offset <= pager->JournalHeader))
				{
					// If the contents of this page were just restored from the main journal file, then its content must be as they were when the 
					// transaction was first opened. In this case we can mark the page as clean, since there will be no need to write it out to the
					// database.
					//
					// There is one exception to this rule. If the page is being rolled back as part of a savepoint (or statement) rollback from an 
					// unsynced portion of the main journal file, then it is not safe to mark the page as clean. This is because marking the page as
					// clean will clear the PGHDR_NEED_SYNC flag. Since the page is already in the journal file (recorded in Pager.pInJournal) and
					// the PGHDR_NEED_SYNC flag is cleared, if the page is written to again within this transaction, it will be marked as dirty but
					// the PGHDR_NEED_SYNC flag will not be set. It could then potentially be written out into the database file before its journal file
					// segment is synced. If a crash occurs during or following this, database corruption may ensue.
					_assert(!UseWal(pager));
					Pcache_MakeClean(pg);
				}
				pager_set_pagehash(pg);

				// If this was page 1, then restore the value of Pager.dbFileVers. Do this before any decoding.
				if (id == 1)
					_memcpy(&pager->DBFileVers, &((uint8 *)pageData)[24], sizeof(pager->DBFileVers));

				// Decode the page just read from disk
				CODEC1(pager, pageData, pg->ID, 3, rc = RC::NOMEM);
				Pcache_Release(pg);
			}
			return rc;
		}

		static int pager_delmaster(Pager *pager, const char *master)
		{
			int rc;
			VFileSystem *vfs = pager->Vfs;

			// Allocate space for both the pJournal and pMaster file descriptors. If successful, open the master journal file for reading.         
			VFile *masterFile = (VFile *)SysEx::Alloc(vfs->SizeOsFile * 2, true); // Malloc'd master-journal file descriptor
			VFile *journalFile = (VFile *)(((uint8 *)masterFile) + vfs->SizeOsFile); // Malloc'd child-journal file descriptor
			if (!masterFile)
				rc = RC::NOMEM;
			else
			{
				const int flags = (SQLITE_OPEN_READONLY | SQLITE_OPEN_MASTER_JOURNAL);
				rc = vfs->Open(master, masterFile, flags, 0);
			}
			if (rc != RC::OK) goto delmaster_out;

			// Load the entire master journal file into space obtained from sqlite3_malloc() and pointed to by zMasterJournal.   Also obtain
			// sufficient space (in zMasterPtr) to hold the names of master journal files extracted from regular rollback-journals.
			int64 masterJournalSize; // Size of master journal file
			rc = masterFile->get_FileSize(masterJournalSize);
			if (rc != RC::OK) goto delmaster_out;
			int masterPtrSize = vfs->MaxPathName + 1; // Amount of space allocated to zMasterPtr[]
			char *masterJournal = (char *)SysEx::Alloc((int)masterJournalSize + masterPtrSize + 1); // Contents of master journal file
			if (!masterJournal)
			{
				rc = RC::NOMEM;
				goto delmaster_out;
			}
			char *masterPtr = &masterJournal[masterJournalSize + 1]; // Space to hold MJ filename from a journal file
			rc = masterFile->Read(masterJournal, (int)masterJournalSize, 0);
			if (rc != RC::OK) goto delmaster_out;
			masterJournal[masterJournalSize] = 0;

			char *journal = masterJournal; // Pointer to one journal within MJ file
			while ((journal - masterJournal) < masterJournalSize)
			{
				int exists;
				rc = vfs->Access(journal, SQLITE_ACCESS_EXISTS, &exists);
				if (rc != RC::OK)
					goto delmaster_out;
				if (exists)
				{
					// One of the journals pointed to by the master journal exists. Open it and check if it points at the master journal. If so, return without deleting the master journal file.
					int flags = (SQLITE_OPEN_READONLY | SQLITE_OPEN_MAIN_JOURNAL);
					rc = vfs->Open(journal, journalFile, flags, 0);
					if (rc != RC::OK)
						goto delmaster_out;

					rc = readMasterJournal(journalFile, masterPtr, masterPtrSize);
					journalFile->Close();
					if (rc != RC::OK)
						goto delmaster_out;

					int c = masterPtr[0] != 0 && _strcmp(masterPtr, master) == 0;
					if (c)
					{
						// We have a match. Do not delete the master journal file.
						goto delmaster_out;
					}
				}
				journal += (sqlite3Strlen30(journal) + 1);
			}

			masterFile->Close();
			rc = vfs->Delete(master, 0);

delmaster_out:
			SysEx::Free(masterJournal);
			if (masterFile != nullptr)
			{
				masterFile->Close();
				_assert(!journalFile->Opened);
				SysEx::Free(journalFile);
			}
			return rc;
		}


		static int pager_truncate(Pager *pPager, Pid pages)
		{
			RC rc = RC::OK;
			_assert(pager->State != PAGER::ERROR);
			_assert(pager->State != PAGER::READER);

			if (pager->File->Opened && pager->State >= PAGER::WRITER_DBMOD || pager->State == PAGER::OPEN)
			{
				newSize;
				int sizePage = pager->PageSize;
				_assert(pager->Lock == VFile::LOCK::EXCLUSIVE);
				// TODO: Is it safe to use Pager.dbFileSize here?
				int64 currentSize;
				rc = pager->File->get_FileSize(currentSize);
				int64 newSize = sizePage * (int64)pages;
				if (rc == RC::OK && currentSize != newSize)
				{
					if (currentSize > newSize)
						rc = pager->File->Truncate(newSize);
					else if ((currentSize + sizePage) <= newSize)
					{
						char *tmp = pager->TmpSpace;
						memset(tmp, 0, sizePage);
						ASSERTCOVERAGE((newSize - sizePage) == currentSize);
						ASSERTCOVERAGE((newSize - sizePage) > currentSize);
						rc = pager->File->Write(tmp, sizePage, newSize - sizePage);
					}
					if (rc == RC::OK)
						pager->DBFileSize = pages;
				}
			}
			return rc;
		}

#pragma endregion

#pragma region Transaction2

		int sqlite3SectorSize(VFile *file)
		{
			int ret = file->SectorSize;
			if (ret < 32)
				ret = 512;
			else if (ret > MAX_SECTOR_SIZE)
			{
				_assert(MAX_SECTOR_SIZE >= 512);
				ret = MAX_SECTOR_SIZE;
			}
			return ret;
		}

		static void setSectorSize(Pager *pager)
		{
			_assert(pager->File->Opened || pager->TempFile);
			if (pager->TempFile || (pager->File->get_DeviceCharacteristics() & SQLITE_IOCAP_POWERSAFE_OVERWRITE) != 0)
				pager->SectorSize = 512; // Sector size doesn't matter for temporary files. Also, the file may not have been opened yet, in which case the OsSectorSize() call will segfault.
			else
				pager->SectorSize = pager->File->SectorSize;
		}

		static int pager_playback(Pager *pPager, int isHot){
			sqlite3_vfs *pVfs = pPager->pVfs;
			i64 szJ;                 /* Size of the journal file in bytes */
			u32 nRec;                /* Number of Records in the journal */
			u32 u;                   /* Unsigned loop counter */
			Pgno mxPg = 0;           /* Size of the original file in pages */
			int rc;                  /* Result code of a subroutine */
			int res = 1;             /* Value returned by sqlite3OsAccess() */
			char *zMaster = 0;       /* Name of master journal file if any */
			int needPagerReset;      /* True to reset page prior to first page rollback */

			/* Figure out how many records are in the journal.  Abort early if
			** the journal is empty.
			*/
			assert( isOpen(pPager->jfd) );
			rc = sqlite3OsFileSize(pPager->jfd, &szJ);
			if( rc!=SQLITE_OK ){
				goto end_playback;
			}

			/* Read the master journal name from the journal, if it is present.
			** If a master journal file name is specified, but the file is not
			** present on disk, then the journal is not hot and does not need to be
			** played back.
			**
			** TODO: Technically the following is an error because it assumes that
			** buffer Pager.pTmpSpace is (mxPathname+1) bytes or larger. i.e. that
			** (pPager->pageSize >= pPager->pVfs->mxPathname+1). Using os_unix.c,
			**  mxPathname is 512, which is the same as the minimum allowable value
			** for pageSize.
			*/
			zMaster = pPager->pTmpSpace;
			rc = readMasterJournal(pPager->jfd, zMaster, pPager->pVfs->mxPathname+1);
			if( rc==SQLITE_OK && zMaster[0] ){
				rc = sqlite3OsAccess(pVfs, zMaster, SQLITE_ACCESS_EXISTS, &res);
			}
			zMaster = 0;
			if( rc!=SQLITE_OK || !res ){
				goto end_playback;
			}
			pPager->journalOff = 0;
			needPagerReset = isHot;

			/* This loop terminates either when a readJournalHdr() or 
			** pager_playback_one_page() call returns SQLITE_DONE or an IO error 
			** occurs. 
			*/
			while( 1 ){
				/* Read the next journal header from the journal file.  If there are
				** not enough bytes left in the journal file for a complete header, or
				** it is corrupted, then a process must have failed while writing it.
				** This indicates nothing more needs to be rolled back.
				*/
				rc = readJournalHdr(pPager, isHot, szJ, &nRec, &mxPg);
				if( rc!=SQLITE_OK ){ 
					if( rc==SQLITE_DONE ){
						rc = SQLITE_OK;
					}
					goto end_playback;
				}

				/* If nRec is 0xffffffff, then this journal was created by a process
				** working in no-sync mode. This means that the rest of the journal
				** file consists of pages, there are no more journal headers. Compute
				** the value of nRec based on this assumption.
				*/
				if( nRec==0xffffffff ){
					assert( pPager->journalOff==JOURNAL_HDR_SZ(pPager) );
					nRec = (int)((szJ - JOURNAL_HDR_SZ(pPager))/JOURNAL_PG_SZ(pPager));
				}

				/* If nRec is 0 and this rollback is of a transaction created by this
				** process and if this is the final header in the journal, then it means
				** that this part of the journal was being filled but has not yet been
				** synced to disk.  Compute the number of pages based on the remaining
				** size of the file.
				**
				** The third term of the test was added to fix ticket #2565.
				** When rolling back a hot journal, nRec==0 always means that the next
				** chunk of the journal contains zero pages to be rolled back.  But
				** when doing a ROLLBACK and the nRec==0 chunk is the last chunk in
				** the journal, it means that the journal might contain additional
				** pages that need to be rolled back and that the number of pages 
				** should be computed based on the journal file size.
				*/
				if( nRec==0 && !isHot &&
					pPager->journalHdr+JOURNAL_HDR_SZ(pPager)==pPager->journalOff ){
						nRec = (int)((szJ - pPager->journalOff) / JOURNAL_PG_SZ(pPager));
				}

				/* If this is the first header read from the journal, truncate the
				** database file back to its original size.
				*/
				if( pPager->journalOff==JOURNAL_HDR_SZ(pPager) ){
					rc = pager_truncate(pPager, mxPg);
					if( rc!=SQLITE_OK ){
						goto end_playback;
					}
					pPager->dbSize = mxPg;
				}

				/* Copy original pages out of the journal and back into the 
				** database file and/or page cache.
				*/
				for(u=0; u<nRec; u++){
					if( needPagerReset ){
						pager_reset(pPager);
						needPagerReset = 0;
					}
					rc = pager_playback_one_page(pPager,&pPager->journalOff,0,1,0);
					if( rc!=SQLITE_OK ){
						if( rc==SQLITE_DONE ){
							pPager->journalOff = szJ;
							break;
						}else if( rc==SQLITE_IOERR_SHORT_READ ){
							/* If the journal has been truncated, simply stop reading and
							** processing the journal. This might happen if the journal was
							** not completely written and synced prior to a crash.  In that
							** case, the database should have never been written in the
							** first place so it is OK to simply abandon the rollback. */
							rc = SQLITE_OK;
							goto end_playback;
						}else{
							/* If we are unable to rollback, quit and return the error
							** code.  This will cause the pager to enter the error state
							** so that no further harm will be done.  Perhaps the next
							** process to come along will be able to rollback the database.
							*/
							goto end_playback;
						}
					}
				}
			}
			/*NOTREACHED*/
			assert( 0 );

end_playback:
			/* Following a rollback, the database file should be back in its original
			** state prior to the start of the transaction, so invoke the
			** SQLITE_FCNTL_DB_UNCHANGED file-control method to disable the
			** assertion that the transaction counter was modified.
			*/
#ifdef SQLITE_DEBUG
			if( pPager->fd->pMethods ){
				sqlite3OsFileControlHint(pPager->fd,SQLITE_FCNTL_DB_UNCHANGED,0);
			}
#endif

			/* If this playback is happening automatically as a result of an IO or 
			** malloc error that occurred after the change-counter was updated but 
			** before the transaction was committed, then the change-counter 
			** modification may just have been reverted. If this happens in exclusive 
			** mode, then subsequent transactions performed by the connection will not
			** update the change-counter at all. This may lead to cache inconsistency
			** problems for other processes at some point in the future. So, just
			** in case this has happened, clear the changeCountDone flag now.
			*/
			pPager->changeCountDone = pPager->tempFile;

			if( rc==SQLITE_OK ){
				zMaster = pPager->pTmpSpace;
				rc = readMasterJournal(pPager->jfd, zMaster, pPager->pVfs->mxPathname+1);
				testcase( rc!=SQLITE_OK );
			}
			if( rc==SQLITE_OK
				&& (pPager->eState>=PAGER_WRITER_DBMOD || pPager->eState==PAGER_OPEN)
				){
					rc = sqlite3PagerSync(pPager);
			}
			if( rc==SQLITE_OK ){
				rc = pager_end_transaction(pPager, zMaster[0]!='\0', 0);
				testcase( rc!=SQLITE_OK );
			}
			if( rc==SQLITE_OK && zMaster[0] && res ){
				/* If there was a master journal and this routine will return success,
				** see if it is possible to delete the master journal.
				*/
				rc = pager_delmaster(pPager, zMaster);
				testcase( rc!=SQLITE_OK );
			}

			/* The Pager.sectorSize variable may have been updated while rolling
			** back a journal created by a process with a different sector size
			** value. Reset it to the correct value for this process.
			*/
			setSectorSize(pPager);
			return rc;
		}

		static int readDbPage(PgHdr *pPg){
			Pager *pPager = pPg->pPager; /* Pager object associated with page pPg */
			Pgno pgno = pPg->pgno;       /* Page number to read */
			int rc = SQLITE_OK;          /* Return code */
			int isInWal = 0;             /* True if page is in log file */
			int pgsz = pPager->pageSize; /* Number of bytes to read */

			assert( pPager->eState>=PAGER_READER && !MEMDB );
			assert( isOpen(pPager->fd) );

			if( NEVER(!isOpen(pPager->fd)) ){
				assert( pPager->tempFile );
				memset(pPg->pData, 0, pPager->pageSize);
				return SQLITE_OK;
			}

			if( pagerUseWal(pPager) ){
				/* Try to pull the page from the write-ahead log. */
				rc = sqlite3WalRead(pPager->pWal, pgno, &isInWal, pgsz, pPg->pData);
			}
			if( rc==SQLITE_OK && !isInWal ){
				i64 iOffset = (pgno-1)*(i64)pPager->pageSize;
				rc = sqlite3OsRead(pPager->fd, pPg->pData, pgsz, iOffset);
				if( rc==SQLITE_IOERR_SHORT_READ ){
					rc = SQLITE_OK;
				}
			}

			if( pgno==1 ){
				if( rc ){
					/* If the read is unsuccessful, set the dbFileVers[] to something
					** that will never be a valid file version.  dbFileVers[] is a copy
					** of bytes 24..39 of the database.  Bytes 28..31 should always be
					** zero or the size of the database in page. Bytes 32..35 and 35..39
					** should be page numbers which are never 0xffffffff.  So filling
					** pPager->dbFileVers[] with all 0xff bytes should suffice.
					**
					** For an encrypted database, the situation is more complex:  bytes
					** 24..39 of the database are white noise.  But the probability of
					** white noising equaling 16 bytes of 0xff is vanishingly small so
					** we should still be ok.
					*/
					memset(pPager->dbFileVers, 0xff, sizeof(pPager->dbFileVers));
				}else{
					u8 *dbFileVers = &((u8*)pPg->pData)[24];
					memcpy(&pPager->dbFileVers, dbFileVers, sizeof(pPager->dbFileVers));
				}
			}
			CODEC1(pPager, pPg->pData, pgno, 3, rc = SQLITE_NOMEM);

			PAGER_INCR(sqlite3_pager_readdb_count);
			PAGER_INCR(pPager->nRead);
			IOTRACE(("PGIN %p %d\n", pPager, pgno));
			PAGERTRACE(("FETCH %d page %d hash(%08x)\n",
				PAGERID(pPager), pgno, pager_pagehash(pPg)));

			return rc;
		}

		static void pager_write_changecounter(PgHdr *pPg){
			u32 change_counter;

			/* Increment the value just read and write it back to byte 24. */
			change_counter = sqlite3Get4byte((u8*)pPg->pPager->dbFileVers)+1;
			put32bits(((char*)pPg->pData)+24, change_counter);

			/* Also store the SQLite version number in bytes 96..99 and in
			** bytes 92..95 store the change counter for which the version number
			** is valid. */
			put32bits(((char*)pPg->pData)+92, change_counter);
			put32bits(((char*)pPg->pData)+96, SQLITE_VERSION_NUMBER);
		}

#ifndef OMIT_WAL
		static int pagerUndoCallback(void *pCtx, Pgno iPg){
			int rc = SQLITE_OK;
			Pager *pPager = (Pager *)pCtx;
			PgHdr *pPg;

			pPg = sqlite3PagerLookup(pPager, iPg);
			if( pPg ){
				if( sqlite3PcachePageRefcount(pPg)==1 ){
					sqlite3PcacheDrop(pPg);
				}else{
					rc = readDbPage(pPg);
					if( rc==SQLITE_OK ){
						pPager->xReiniter(pPg);
					}
					sqlite3PagerUnref(pPg);
				}
			}

			/* Normally, if a transaction is rolled back, any backup processes are
			** updated as data is copied out of the rollback journal and into the
			** database. This is not generally possible with a WAL database, as
			** rollback involves simply truncating the log file. Therefore, if one
			** or more frames have already been written to the log (and therefore 
			** also copied into the backup databases) as part of this transaction,
			** the backups must be restarted.
			*/
			sqlite3BackupRestart(pPager->pBackup);

			return rc;
		}

		static int pagerRollbackWal(Pager *pPager){
			int rc;                         /* Return Code */
			PgHdr *pList;                   /* List of dirty pages to revert */

			/* For all pages in the cache that are currently dirty or have already
			** been written (but not committed) to the log file, do one of the 
			** following:
			**
			**   + Discard the cached page (if refcount==0), or
			**   + Reload page content from the database (if refcount>0).
			*/
			pPager->dbSize = pPager->dbOrigSize;
			rc = sqlite3WalUndo(pPager->pWal, pagerUndoCallback, (void *)pPager);
			pList = sqlite3PcacheDirtyList(pPager->pPCache);
			while( pList && rc==SQLITE_OK ){
				PgHdr *pNext = pList->pDirty;
				rc = pagerUndoCallback((void *)pPager, pList->pgno);
				pList = pNext;
			}

			return rc;
		}

		static int pagerWalFrames(
			Pager *pPager,                  /* Pager object */
			PgHdr *pList,                   /* List of frames to log */
			Pgno nTruncate,                 /* Database size after this commit */
			int isCommit                    /* True if this is a commit */
			){
				int rc;                         /* Return code */
				int nList;                      /* Number of pages in pList */
#if defined(SQLITE_DEBUG) || defined(SQLITE_CHECK_PAGES)
				PgHdr *p;                       /* For looping over pages */
#endif

				assert( pPager->pWal );
				assert( pList );
#ifdef SQLITE_DEBUG
				/* Verify that the page list is in accending order */
				for(p=pList; p && p->pDirty; p=p->pDirty){
					assert( p->pgno < p->pDirty->pgno );
				}
#endif

				assert( pList->pDirty==0 || isCommit );
				if( isCommit ){
					/* If a WAL transaction is being committed, there is no point in writing
					** any pages with page numbers greater than nTruncate into the WAL file.
					** They will never be read by any client. So remove them from the pDirty
					** list here. */
					PgHdr *p;
					PgHdr **ppNext = &pList;
					nList = 0;
					for(p=pList; (*ppNext = p)!=0; p=p->pDirty){
						if( p->pgno<=nTruncate ){
							ppNext = &p->pDirty;
							nList++;
						}
					}
					assert( pList );
				}else{
					nList = 1;
				}
				pPager->aStat[PAGER_STAT_WRITE] += nList;

				if( pList->pgno==1 ) pager_write_changecounter(pList);
				rc = sqlite3WalFrames(pPager->pWal, 
					pPager->pageSize, pList, nTruncate, isCommit, pPager->walSyncFlags
					);
				if( rc==SQLITE_OK && pPager->pBackup ){
					PgHdr *p;
					for(p=pList; p; p=p->pDirty){
						sqlite3BackupUpdate(pPager->pBackup, p->pgno, (u8 *)p->pData);
					}
				}

#ifdef SQLITE_CHECK_PAGES
				pList = sqlite3PcacheDirtyList(pPager->pPCache);
				for(p=pList; p; p=p->pDirty){
					pager_set_pagehash(p);
				}
#endif

				return rc;
		}

		static int pagerBeginReadTransaction(Pager *pPager){
			int rc;                         /* Return code */
			int changed = 0;                /* True if cache must be reset */

			assert( pagerUseWal(pPager) );
			assert( pPager->eState==PAGER_OPEN || pPager->eState==PAGER_READER );

			/* sqlite3WalEndReadTransaction() was not called for the previous
			** transaction in locking_mode=EXCLUSIVE.  So call it now.  If we
			** are in locking_mode=NORMAL and EndRead() was previously called,
			** the duplicate call is harmless.
			*/
			sqlite3WalEndReadTransaction(pPager->pWal);

			rc = sqlite3WalBeginReadTransaction(pPager->pWal, &changed);
			if( rc!=SQLITE_OK || changed ){
				pager_reset(pPager);
			}

			return rc;
		}
#endif

		static int pagerPagecount(Pager *pPager, Pgno *pnPage){
			Pgno nPage;                     /* Value to return via *pnPage */

			/* Query the WAL sub-system for the database size. The WalDbsize()
			** function returns zero if the WAL is not open (i.e. Pager.pWal==0), or
			** if the database size is not available. The database size is not
			** available from the WAL sub-system if the log file is empty or
			** contains no valid committed transactions.
			*/
			assert( pPager->eState==PAGER_OPEN );
			assert( pPager->eLock>=SHARED_LOCK );
			nPage = sqlite3WalDbsize(pPager->pWal);

			/* If the database size was not available from the WAL sub-system,
			** determine it based on the size of the database file. If the size
			** of the database file is not an integer multiple of the page-size,
			** round down to the nearest page. Except, any file larger than 0
			** bytes in size is considered to contain at least one page.
			*/
			if( nPage==0 ){
				i64 n = 0;                    /* Size of db file in bytes */
				assert( isOpen(pPager->fd) || pPager->tempFile );
				if( isOpen(pPager->fd) ){
					int rc = sqlite3OsFileSize(pPager->fd, &n);
					if( rc!=SQLITE_OK ){
						return rc;
					}
				}
				nPage = (Pgno)((n+pPager->pageSize-1) / pPager->pageSize);
			}

			/* If the current number of pages in the file is greater than the
			** configured maximum pager number, increase the allowed limit so
			** that the file can be read.
			*/
			if( nPage>pPager->mxPgno ){
				pPager->mxPgno = (Pgno)nPage;
			}

			*pnPage = nPage;
			return SQLITE_OK;
		}

#ifndef OMIT_WAL
		static int pagerOpenWalIfPresent(Pager *pPager){
			int rc = SQLITE_OK;
			assert( pPager->eState==PAGER_OPEN );
			assert( pPager->eLock>=SHARED_LOCK );

			if( !pPager->tempFile ){
				int isWal;                    /* True if WAL file exists */
				Pgno nPage;                   /* Size of the database file */

				rc = pagerPagecount(pPager, &nPage);
				if( rc ) return rc;
				if( nPage==0 ){
					rc = sqlite3OsDelete(pPager->pVfs, pPager->zWal, 0);
					if( rc==SQLITE_IOERR_DELETE_NOENT ) rc = SQLITE_OK;
					isWal = 0;
				}else{
					rc = sqlite3OsAccess(
						pPager->pVfs, pPager->zWal, SQLITE_ACCESS_EXISTS, &isWal
						);
				}
				if( rc==SQLITE_OK ){
					if( isWal ){
						testcase( sqlite3PcachePagecount(pPager->pPCache)==0 );
						rc = sqlite3PagerOpenWal(pPager, 0);
					}else if( pPager->journalMode==PAGER_JOURNALMODE_WAL ){
						pPager->journalMode = PAGER_JOURNALMODE_DELETE;
					}
				}
			}
			return rc;
		}
#endif

		static int pagerPlaybackSavepoint(Pager *pPager, PagerSavepoint *pSavepoint){
			i64 szJ;                 /* Effective size of the main journal */
			i64 iHdrOff;             /* End of first segment of main-journal records */
			int rc = SQLITE_OK;      /* Return code */
			Bitvec *pDone = 0;       /* Bitvec to ensure pages played back only once */

			assert( pPager->eState!=PAGER_ERROR );
			assert( pPager->eState>=PAGER_WRITER_LOCKED );

			/* Allocate a bitvec to use to store the set of pages rolled back */
			if( pSavepoint ){
				pDone = sqlite3BitvecCreate(pSavepoint->nOrig);
				if( !pDone ){
					return SQLITE_NOMEM;
				}
			}

			/* Set the database size back to the value it was before the savepoint 
			** being reverted was opened.
			*/
			pPager->dbSize = pSavepoint ? pSavepoint->nOrig : pPager->dbOrigSize;
			pPager->changeCountDone = pPager->tempFile;

			if( !pSavepoint && pagerUseWal(pPager) ){
				return pagerRollbackWal(pPager);
			}

			/* Use pPager->journalOff as the effective size of the main rollback
			** journal.  The actual file might be larger than this in
			** PAGER_JOURNALMODE_TRUNCATE or PAGER_JOURNALMODE_PERSIST.  But anything
			** past pPager->journalOff is off-limits to us.
			*/
			szJ = pPager->journalOff;
			assert( pagerUseWal(pPager)==0 || szJ==0 );

			/* Begin by rolling back records from the main journal starting at
			** PagerSavepoint.iOffset and continuing to the next journal header.
			** There might be records in the main journal that have a page number
			** greater than the current database size (pPager->dbSize) but those
			** will be skipped automatically.  Pages are added to pDone as they
			** are played back.
			*/
			if( pSavepoint && !pagerUseWal(pPager) ){
				iHdrOff = pSavepoint->iHdrOffset ? pSavepoint->iHdrOffset : szJ;
				pPager->journalOff = pSavepoint->iOffset;
				while( rc==SQLITE_OK && pPager->journalOff<iHdrOff ){
					rc = pager_playback_one_page(pPager, &pPager->journalOff, pDone, 1, 1);
				}
				assert( rc!=SQLITE_DONE );
			}else{
				pPager->journalOff = 0;
			}

			/* Continue rolling back records out of the main journal starting at
			** the first journal header seen and continuing until the effective end
			** of the main journal file.  Continue to skip out-of-range pages and
			** continue adding pages rolled back to pDone.
			*/
			while( rc==SQLITE_OK && pPager->journalOff<szJ ){
				u32 ii;            /* Loop counter */
				u32 nJRec = 0;     /* Number of Journal Records */
				u32 dummy;
				rc = readJournalHdr(pPager, 0, szJ, &nJRec, &dummy);
				assert( rc!=SQLITE_DONE );

				/*
				** The "pPager->journalHdr+JOURNAL_HDR_SZ(pPager)==pPager->journalOff"
				** test is related to ticket #2565.  See the discussion in the
				** pager_playback() function for additional information.
				*/
				if( nJRec==0 
					&& pPager->journalHdr+JOURNAL_HDR_SZ(pPager)==pPager->journalOff
					){
						nJRec = (u32)((szJ - pPager->journalOff)/JOURNAL_PG_SZ(pPager));
				}
				for(ii=0; rc==SQLITE_OK && ii<nJRec && pPager->journalOff<szJ; ii++){
					rc = pager_playback_one_page(pPager, &pPager->journalOff, pDone, 1, 1);
				}
				assert( rc!=SQLITE_DONE );
			}
			assert( rc!=SQLITE_OK || pPager->journalOff>=szJ );

			/* Finally,  rollback pages from the sub-journal.  Page that were
			** previously rolled back out of the main journal (and are hence in pDone)
			** will be skipped.  Out-of-range pages are also skipped.
			*/
			if( pSavepoint ){
				u32 ii;            /* Loop counter */
				i64 offset = (i64)pSavepoint->iSubRec*(4+pPager->pageSize);

				if( pagerUseWal(pPager) ){
					rc = sqlite3WalSavepointUndo(pPager->pWal, pSavepoint->aWalData);
				}
				for(ii=pSavepoint->iSubRec; rc==SQLITE_OK && ii<pPager->nSubRec; ii++){
					assert( offset==(i64)ii*(4+pPager->pageSize) );
					rc = pager_playback_one_page(pPager, &offset, pDone, 0, 1);
				}
				assert( rc!=SQLITE_DONE );
			}

			sqlite3BitvecDestroy(pDone);
			if( rc==SQLITE_OK ){
				pPager->journalOff = szJ;
			}

			return rc;
		}

#pragma endregion

#if 0
#pragma region Name3

		void sqlite3PagerSetCachesize(Pager *pPager, int mxPage)
		{
			sqlite3PcacheSetCachesize(pPager->pPCache, mxPage);
		}

		void sqlite3PagerShrink(Pager *pPager)
		{
			sqlite3PcacheShrink(pPager->pPCache);
		}

#ifndef OMIT_PAGER_PRAGMAS
		void sqlite3PagerSetSafetyLevel(
			Pager *pPager,        /* The pager to set safety level for */
			int level,            /* PRAGMA synchronous.  1=OFF, 2=NORMAL, 3=FULL */  
			int bFullFsync,       /* PRAGMA fullfsync */
			int bCkptFullFsync    /* PRAGMA checkpoint_fullfsync */
			){
				assert( level>=1 && level<=3 );
				pPager->noSync =  (level==1 || pPager->tempFile) ?1:0;
				pPager->fullSync = (level==3 && !pPager->tempFile) ?1:0;
				if( pPager->noSync ){
					pPager->syncFlags = 0;
					pPager->ckptSyncFlags = 0;
				}else if( bFullFsync ){
					pPager->syncFlags = SQLITE_SYNC_FULL;
					pPager->ckptSyncFlags = SQLITE_SYNC_FULL;
				}else if( bCkptFullFsync ){
					pPager->syncFlags = SQLITE_SYNC_NORMAL;
					pPager->ckptSyncFlags = SQLITE_SYNC_FULL;
				}else{
					pPager->syncFlags = SQLITE_SYNC_NORMAL;
					pPager->ckptSyncFlags = SQLITE_SYNC_NORMAL;
				}
				pPager->walSyncFlags = pPager->syncFlags;
				if( pPager->fullSync ){
					pPager->walSyncFlags |= WAL_SYNC_TRANSACTIONS;
				}
		}
#endif

		// The following global variable is incremented whenever the library attempts to open a temporary file.  This information is used for testing and analysis only.  
#ifdef TEST
		int sqlite3_opentemp_count = 0;
#endif

		static int pagerOpentemp(
			Pager *pPager,        /* The pager object */
			sqlite3_file *pFile,  /* Write the file descriptor here */
			int vfsFlags          /* Flags passed through to the VFS */
			){
				int rc;               /* Return code */

#ifdef TEST
				sqlite3_opentemp_count++;  // Used for testing and analysis only
#endif
				vfsFlags |=  SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE |
					SQLITE_OPEN_EXCLUSIVE | SQLITE_OPEN_DELETEONCLOSE;
				rc = sqlite3OsOpen(pPager->pVfs, 0, pFile, vfsFlags, 0);
				assert( rc!=SQLITE_OK || isOpen(pFile) );
				return rc;
		}

		void sqlite3PagerSetBusyhandler(
			Pager *pPager,                       /* Pager object */
			int (*xBusyHandler)(void *),         /* Pointer to busy-handler function */
			void *pBusyHandlerArg                /* Argument to pass to xBusyHandler */
			){
				pPager->xBusyHandler = xBusyHandler;
				pPager->pBusyHandlerArg = pBusyHandlerArg;

				if( isOpen(pPager->fd) ){
					void **ap = (void **)&pPager->xBusyHandler;
					assert( ((int(*)(void *))(ap[0]))==xBusyHandler );
					assert( ap[1]==pBusyHandlerArg );
					sqlite3OsFileControlHint(pPager->fd, SQLITE_FCNTL_BUSYHANDLER, (void *)ap);
				}
		}

		int sqlite3PagerSetPagesize(Pager *pager, uint32 *pageSize, int reserves)
		{
			// It is not possible to do a full assert_pager_state() here, as this function may be called from within PagerOpen(), before the state
			// of the Pager object is internally consistent.
			//
			// At one point this function returned an error if the pager was in PAGER_ERROR state. But since PAGER_ERROR state guarantees that
			// there is at least one outstanding page reference, this function is a no-op for that case anyhow.
			uint32 pageSize_ = *pageSize;
			_assert(pageSize_ == 0 || (pageSize_ >= 512 && pageSize_ <= MAX_PAGE_SIZE));
			int rc = RC::OK;
			if ((!pager->MemoryDB || pager->DBSize == 0) && PCache_RefCount(pager->PCache) == 0 && pageSize_ && pageSize_ != (uint32)pager->PageSize)
			{
				char *tempSpace = nullptr; // New temp space
				i64 bytes = 0;
				if (pager->State > PAGER::OPEN && pager->File->Opened)
					rc = pager->File->get_FileSize(&bytes);
				if (rc == RC::OK)
				{
					tempSpace = (char *)sqlite3PageMalloc(pageSize);
					if (!tempSpace) rc = RC::NOMEM;
				}
				if (rc == RC::OK)
				{
					pager_reset(pager);
					pager->DBSize = (Pid)((bytes + pageSize_ - 1) / pageSize_);
					pager->PageSize = pageSize_;
					sqlite3PageFree(pager->TmpSpace);
					pager->TmpSpace = tempSpace;
					PCache_SetPageSize(pager->PCache, pageSize_);
				}
			}
			*pageSize = pager->PageSize;
			if (rc == RC::OK)
			{
				if (reserves < 0) reserves = pager->Reserves;
				_assert(reserves >= 0 && reserves < 1000);
				pager->Reserves = (int16)reserves;
				pagerReportSize(pager);
			}
			return rc;
		}

		void *sqlite3PagerTempSpace(Pager *pager)
		{
			return pager->TmpSpace;
		}

		int sqlite3PagerMaxPageCount(Pager *pager, int maxPage)
		{
			if (maxPage > 0)
				pager->MaxPid = maxPage;
			_assert(pager->State != PAGER::OPEN);		// Called only by OP_MaxPgcnt
			_assert(pager->MaxPid >= pager->DBSize);	// OP_MaxPgcnt enforces this
			return pager->MaxPid;
		}


#ifdef TEST
		extern int sqlite3_io_error_pending;
		extern int sqlite3_io_error_hit;
		static int saved_cnt;
		void disable_simulated_io_errors(void){
			saved_cnt = sqlite3_io_error_pending;
			sqlite3_io_error_pending = -1;
		}
		void enable_simulated_io_errors(void){
			sqlite3_io_error_pending = saved_cnt;
		}
#else
# define disable_simulated_io_errors()
# define enable_simulated_io_errors()
#endif

		int sqlite3PagerReadFileheader(Pager *pPager, int N, unsigned char *pDest)
		{
			int rc = SQLITE_OK;
			memset(pDest, 0, N);
			assert( isOpen(pPager->fd) || pPager->tempFile );

			// This routine is only called by btree immediately after creating the Pager object.  There has not been an opportunity to transition to WAL mode yet.
			assert( !pagerUseWal(pPager) );

			if( isOpen(pPager->fd) ){
				IOTRACE(("DBHDR %p 0 %d\n", pPager, N))
					rc = sqlite3OsRead(pPager->fd, pDest, N, 0);
				if( rc==SQLITE_IOERR_SHORT_READ ){
					rc = SQLITE_OK;
				}
			}
			return rc;
		}

		void sqlite3PagerPagecount(Pager *pPager, int *pnPage)
		{
			assert( pPager->eState>=PAGER_READER );
			assert( pPager->eState!=PAGER_WRITER_FINISHED );
			*pnPage = (int)pPager->dbSize;
		}


		static int pager_wait_on_lock(Pager *pPager, int locktype)
		{
			int rc;

			/* Check that this is either a no-op (because the requested lock is already held, or one of the transistions that the busy-handler
			** may be invoked during, according to the comment above sqlite3PagerSetBusyhandler().
			*/
			assert( (pPager->eLock>=locktype)
				|| (pPager->eLock==NO_LOCK && locktype==SHARED_LOCK)
				|| (pPager->eLock==RESERVED_LOCK && locktype==EXCLUSIVE_LOCK)
				);

			do {
				rc = pagerLockDb(pPager, locktype);
			}while( rc==SQLITE_BUSY && pPager->xBusyHandler(pPager->pBusyHandlerArg) );
			return rc;
		}
#if defined(_DEBUG)
		static void assertTruncateConstraintCb(PgHdr *pPg){
			assert( pPg->flags&PGHDR_DIRTY );
			assert( !subjRequiresPage(pPg) || pPg->pgno<=pPg->pPager->dbSize );
		}
		static void assertTruncateConstraint(Pager *pPager){
			sqlite3PcacheIterateDirty(pPager->pPCache, assertTruncateConstraintCb);
		}
#else
# define assertTruncateConstraint(pPager)
#endif

		void sqlite3PagerTruncateImage(Pager *pPager, Pgno nPage){
			assert( pPager->dbSize>=nPage );
			assert( pPager->eState>=PAGER_WRITER_CACHEMOD );
			pPager->dbSize = nPage;

			/* At one point the code here called assertTruncateConstraint() to
			** ensure that all pages being truncated away by this operation are,
			** if one or more savepoints are open, present in the savepoint 
			** journal so that they can be restored if the savepoint is rolled
			** back. This is no longer necessary as this function is now only
			** called right before committing a transaction. So although the 
			** Pager object may still have open savepoints (Pager.nSavepoint!=0), 
			** they cannot be rolled back. So the assertTruncateConstraint() call
			** is no longer correct. */
		}

		static int pagerSyncHotJournal(Pager *pPager){
			int rc = SQLITE_OK;
			if( !pPager->noSync ){
				rc = sqlite3OsSync(pPager->jfd, SQLITE_SYNC_NORMAL);
			}
			if( rc==SQLITE_OK ){
				rc = sqlite3OsFileSize(pPager->jfd, &pPager->journalHdr);
			}
			return rc;
		}

		int sqlite3PagerClose(Pager *pPager){
			u8 *pTmp = (u8 *)pPager->pTmpSpace;

			assert( assert_pager_state(pPager) );
			disable_simulated_io_errors();
			sqlite3BeginBenignMalloc();
			/* pPager->errCode = 0; */
			pPager->exclusiveMode = 0;
#ifndef SQLITE_OMIT_WAL
			sqlite3WalClose(pPager->pWal, pPager->ckptSyncFlags, pPager->pageSize, pTmp);
			pPager->pWal = 0;
#endif
			pager_reset(pPager);
			if( MEMDB ){
				pager_unlock(pPager);
			}else{
				/* If it is open, sync the journal file before calling UnlockAndRollback.
				** If this is not done, then an unsynced portion of the open journal 
				** file may be played back into the database. If a power failure occurs 
				** while this is happening, the database could become corrupt.
				**
				** If an error occurs while trying to sync the journal, shift the pager
				** into the ERROR state. This causes UnlockAndRollback to unlock the
				** database and close the journal file without attempting to roll it
				** back or finalize it. The next database user will have to do hot-journal
				** rollback before accessing the database file.
				*/
				if( isOpen(pPager->jfd) ){
					pager_error(pPager, pagerSyncHotJournal(pPager));
				}
				pagerUnlockAndRollback(pPager);
			}
			sqlite3EndBenignMalloc();
			enable_simulated_io_errors();
			PAGERTRACE(("CLOSE %d\n", PAGERID(pPager)));
			IOTRACE(("CLOSE %p\n", pPager))
				sqlite3OsClose(pPager->jfd);
			sqlite3OsClose(pPager->fd);
			sqlite3PageFree(pTmp);
			sqlite3PcacheClose(pPager->pPCache);

#ifdef SQLITE_HAS_CODEC
			if( pPager->xCodecFree ) pPager->xCodecFree(pPager->pCodec);
#endif

			assert( !pPager->aSavepoint && !pPager->pInJournal );
			assert( !isOpen(pPager->jfd) && !isOpen(pPager->sjfd) );

			sqlite3_free(pPager);
			return SQLITE_OK;
		}

#if !defined(_DEBUG) || defined(TEST)
		Pid sqlite3PagerPagenumber(DbPage *pg)
		{
			return pg->ID;
		}
#endif

		void sqlite3PagerRef(DbPage *pg)
		{
			sqlite3PcacheRef(pg);
		}

		static int syncJournal(Pager *pPager, int newHdr){
			int rc;                         /* Return code */

			assert( pPager->eState==PAGER_WRITER_CACHEMOD
				|| pPager->eState==PAGER_WRITER_DBMOD
				);
			assert( assert_pager_state(pPager) );
			assert( !pagerUseWal(pPager) );

			rc = sqlite3PagerExclusiveLock(pPager);
			if( rc!=SQLITE_OK ) return rc;

			if( !pPager->noSync ){
				assert( !pPager->tempFile );
				if( isOpen(pPager->jfd) && pPager->journalMode!=PAGER_JOURNALMODE_MEMORY ){
					const int iDc = sqlite3OsDeviceCharacteristics(pPager->fd);
					assert( isOpen(pPager->jfd) );

					if( 0==(iDc&SQLITE_IOCAP_SAFE_APPEND) ){
						/* This block deals with an obscure problem. If the last connection
						** that wrote to this database was operating in persistent-journal
						** mode, then the journal file may at this point actually be larger
						** than Pager.journalOff bytes. If the next thing in the journal
						** file happens to be a journal-header (written as part of the
						** previous connection's transaction), and a crash or power-failure 
						** occurs after nRec is updated but before this connection writes 
						** anything else to the journal file (or commits/rolls back its 
						** transaction), then SQLite may become confused when doing the 
						** hot-journal rollback following recovery. It may roll back all
						** of this connections data, then proceed to rolling back the old,
						** out-of-date data that follows it. Database corruption.
						**
						** To work around this, if the journal file does appear to contain
						** a valid header following Pager.journalOff, then write a 0x00
						** byte to the start of it to prevent it from being recognized.
						**
						** Variable iNextHdrOffset is set to the offset at which this
						** problematic header will occur, if it exists. aMagic is used 
						** as a temporary buffer to inspect the first couple of bytes of
						** the potential journal header.
						*/
						i64 iNextHdrOffset;
						u8 aMagic[8];
						u8 zHeader[sizeof(aJournalMagic)+4];

						memcpy(zHeader, aJournalMagic, sizeof(aJournalMagic));
						put32bits(&zHeader[sizeof(aJournalMagic)], pPager->nRec);

						iNextHdrOffset = journalHdrOffset(pPager);
						rc = sqlite3OsRead(pPager->jfd, aMagic, 8, iNextHdrOffset);
						if( rc==SQLITE_OK && 0==memcmp(aMagic, aJournalMagic, 8) ){
							static const u8 zerobyte = 0;
							rc = sqlite3OsWrite(pPager->jfd, &zerobyte, 1, iNextHdrOffset);
						}
						if( rc!=SQLITE_OK && rc!=SQLITE_IOERR_SHORT_READ ){
							return rc;
						}

						/* Write the nRec value into the journal file header. If in
						** full-synchronous mode, sync the journal first. This ensures that
						** all data has really hit the disk before nRec is updated to mark
						** it as a candidate for rollback.
						**
						** This is not required if the persistent media supports the
						** SAFE_APPEND property. Because in this case it is not possible 
						** for garbage data to be appended to the file, the nRec field
						** is populated with 0xFFFFFFFF when the journal header is written
						** and never needs to be updated.
						*/
						if( pPager->fullSync && 0==(iDc&SQLITE_IOCAP_SEQUENTIAL) ){
							PAGERTRACE(("SYNC journal of %d\n", PAGERID(pPager)));
							IOTRACE(("JSYNC %p\n", pPager))
								rc = sqlite3OsSync(pPager->jfd, pPager->syncFlags);
							if( rc!=SQLITE_OK ) return rc;
						}
						IOTRACE(("JHDR %p %lld\n", pPager, pPager->journalHdr));
						rc = sqlite3OsWrite(
							pPager->jfd, zHeader, sizeof(zHeader), pPager->journalHdr
							);
						if( rc!=SQLITE_OK ) return rc;
					}
					if( 0==(iDc&SQLITE_IOCAP_SEQUENTIAL) ){
						PAGERTRACE(("SYNC journal of %d\n", PAGERID(pPager)));
						IOTRACE(("JSYNC %p\n", pPager))
							rc = sqlite3OsSync(pPager->jfd, pPager->syncFlags| 
							(pPager->syncFlags==SQLITE_SYNC_FULL?SQLITE_SYNC_DATAONLY:0)
							);
						if( rc!=SQLITE_OK ) return rc;
					}

					pPager->journalHdr = pPager->journalOff;
					if( newHdr && 0==(iDc&SQLITE_IOCAP_SAFE_APPEND) ){
						pPager->nRec = 0;
						rc = writeJournalHdr(pPager);
						if( rc!=SQLITE_OK ) return rc;
					}
				}else{
					pPager->journalHdr = pPager->journalOff;
				}
			}

			/* Unless the pager is in noSync mode, the journal file was just 
			** successfully synced. Either way, clear the PGHDR_NEED_SYNC flag on 
			** all pages.
			*/
			sqlite3PcacheClearSyncFlags(pPager->pPCache);
			pPager->eState = PAGER_WRITER_DBMOD;
			assert( assert_pager_state(pPager) );
			return SQLITE_OK;
		}

		static int pager_write_pagelist(Pager *pPager, PgHdr *pList){
			int rc = SQLITE_OK;                  /* Return code */

			/* This function is only called for rollback pagers in WRITER_DBMOD state. */
			assert( !pagerUseWal(pPager) );
			assert( pPager->eState==PAGER_WRITER_DBMOD );
			assert( pPager->eLock==EXCLUSIVE_LOCK );

			/* If the file is a temp-file has not yet been opened, open it now. It
			** is not possible for rc to be other than SQLITE_OK if this branch
			** is taken, as pager_wait_on_lock() is a no-op for temp-files.
			*/
			if( !isOpen(pPager->fd) ){
				assert( pPager->tempFile && rc==SQLITE_OK );
				rc = pagerOpentemp(pPager, pPager->fd, pPager->vfsFlags);
			}

			/* Before the first write, give the VFS a hint of what the final
			** file size will be.
			*/
			assert( rc!=SQLITE_OK || isOpen(pPager->fd) );
			if( rc==SQLITE_OK && pPager->dbSize>pPager->dbHintSize ){
				sqlite3_int64 szFile = pPager->pageSize * (sqlite3_int64)pPager->dbSize;
				sqlite3OsFileControlHint(pPager->fd, SQLITE_FCNTL_SIZE_HINT, &szFile);
				pPager->dbHintSize = pPager->dbSize;
			}

			while( rc==SQLITE_OK && pList ){
				Pgno pgno = pList->pgno;

				/* If there are dirty pages in the page cache with page numbers greater
				** than Pager.dbSize, this means sqlite3PagerTruncateImage() was called to
				** make the file smaller (presumably by auto-vacuum code). Do not write
				** any such pages to the file.
				**
				** Also, do not write out any page that has the PGHDR_DONT_WRITE flag
				** set (set by sqlite3PagerDontWrite()).
				*/
				if( pgno<=pPager->dbSize && 0==(pList->flags&PGHDR_DONT_WRITE) ){
					i64 offset = (pgno-1)*(i64)pPager->pageSize;   /* Offset to write */
					char *pData;                                   /* Data to write */    

					assert( (pList->flags&PGHDR_NEED_SYNC)==0 );
					if( pList->pgno==1 ) pager_write_changecounter(pList);

					/* Encode the database */
					CODEC2(pPager, pList->pData, pgno, 6, return SQLITE_NOMEM, pData);

					/* Write out the page data. */
					rc = sqlite3OsWrite(pPager->fd, pData, pPager->pageSize, offset);

					/* If page 1 was just written, update Pager.dbFileVers to match
					** the value now stored in the database file. If writing this 
					** page caused the database file to grow, update dbFileSize. 
					*/
					if( pgno==1 ){
						memcpy(&pPager->dbFileVers, &pData[24], sizeof(pPager->dbFileVers));
					}
					if( pgno>pPager->dbFileSize ){
						pPager->dbFileSize = pgno;
					}
					pPager->aStat[PAGER_STAT_WRITE]++;

					/* Update any backup objects copying the contents of this pager. */
					sqlite3BackupUpdate(pPager->pBackup, pgno, (u8*)pList->pData);

					PAGERTRACE(("STORE %d page %d hash(%08x)\n",
						PAGERID(pPager), pgno, pager_pagehash(pList)));
					IOTRACE(("PGOUT %p %d\n", pPager, pgno));
					PAGER_INCR(sqlite3_pager_writedb_count);
				}else{
					PAGERTRACE(("NOSTORE %d page %d\n", PAGERID(pPager), pgno));
				}
				pager_set_pagehash(pList);
				pList = pList->pDirty;
			}

			return rc;
		}

		static int openSubJournal(Pager *pPager){
			int rc = SQLITE_OK;
			if( !isOpen(pPager->sjfd) ){
				if( pPager->journalMode==PAGER_JOURNALMODE_MEMORY || pPager->subjInMemory ){
					sqlite3MemJournalOpen(pPager->sjfd);
				}else{
					rc = pagerOpentemp(pPager, pPager->sjfd, SQLITE_OPEN_SUBJOURNAL);
				}
			}
			return rc;
		}

		static int subjournalPage(PgHdr *pPg){
			int rc = SQLITE_OK;
			Pager *pPager = pPg->pPager;
			if( pPager->journalMode!=PAGER_JOURNALMODE_OFF ){

				/* Open the sub-journal, if it has not already been opened */
				assert( pPager->useJournal );
				assert( isOpen(pPager->jfd) || pagerUseWal(pPager) );
				assert( isOpen(pPager->sjfd) || pPager->nSubRec==0 );
				assert( pagerUseWal(pPager) 
					|| pageInJournal(pPg) 
					|| pPg->pgno>pPager->dbOrigSize 
					);
				rc = openSubJournal(pPager);

				/* If the sub-journal was opened successfully (or was already open),
				** write the journal record into the file.  */
				if( rc==SQLITE_OK ){
					void *pData = pPg->pData;
					i64 offset = (i64)pPager->nSubRec*(4+pPager->pageSize);
					char *pData2;

					CODEC2(pPager, pData, pPg->pgno, 7, return SQLITE_NOMEM, pData2);
					PAGERTRACE(("STMT-JOURNAL %d page %d\n", PAGERID(pPager), pPg->pgno));
					rc = write32bits(pPager->sjfd, offset, pPg->pgno);
					if( rc==SQLITE_OK ){
						rc = sqlite3OsWrite(pPager->sjfd, pData2, pPager->pageSize, offset+4);
					}
				}
			}
			if( rc==SQLITE_OK ){
				pPager->nSubRec++;
				assert( pPager->nSavepoint>0 );
				rc = addToSavepointBitvecs(pPager, pPg->pgno);
			}
			return rc;
		}

		static int pagerStress(void *p, PgHdr *pPg){
			Pager *pPager = (Pager *)p;
			int rc = SQLITE_OK;

			assert( pPg->pPager==pPager );
			assert( pPg->flags&PGHDR_DIRTY );

			/* The doNotSyncSpill flag is set during times when doing a sync of
			** journal (and adding a new header) is not allowed.  This occurs
			** during calls to sqlite3PagerWrite() while trying to journal multiple
			** pages belonging to the same sector.
			**
			** The doNotSpill flag inhibits all cache spilling regardless of whether
			** or not a sync is required.  This is set during a rollback.
			**
			** Spilling is also prohibited when in an error state since that could
			** lead to database corruption.   In the current implementaton it 
			** is impossible for sqlite3PcacheFetch() to be called with createFlag==1
			** while in the error state, hence it is impossible for this routine to
			** be called in the error state.  Nevertheless, we include a NEVER()
			** test for the error state as a safeguard against future changes.
			*/
			if( NEVER(pPager->errCode) ) return SQLITE_OK;
			if( pPager->doNotSpill ) return SQLITE_OK;
			if( pPager->doNotSyncSpill && (pPg->flags & PGHDR_NEED_SYNC)!=0 ){
				return SQLITE_OK;
			}

			pPg->pDirty = 0;
			if( pagerUseWal(pPager) ){
				/* Write a single frame for this page to the log. */
				if( subjRequiresPage(pPg) ){ 
					rc = subjournalPage(pPg); 
				}
				if( rc==SQLITE_OK ){
					rc = pagerWalFrames(pPager, pPg, 0, 0);
				}
			}else{

				/* Sync the journal file if required. */
				if( pPg->flags&PGHDR_NEED_SYNC 
					|| pPager->eState==PAGER_WRITER_CACHEMOD
					){
						rc = syncJournal(pPager, 1);
				}

				/* If the page number of this page is larger than the current size of
				** the database image, it may need to be written to the sub-journal.
				** This is because the call to pager_write_pagelist() below will not
				** actually write data to the file in this case.
				**
				** Consider the following sequence of events:
				**
				**   BEGIN;
				**     <journal page X>
				**     <modify page X>
				**     SAVEPOINT sp;
				**       <shrink database file to Y pages>
				**       pagerStress(page X)
				**     ROLLBACK TO sp;
				**
				** If (X>Y), then when pagerStress is called page X will not be written
				** out to the database file, but will be dropped from the cache. Then,
				** following the "ROLLBACK TO sp" statement, reading page X will read
				** data from the database file. This will be the copy of page X as it
				** was when the transaction started, not as it was when "SAVEPOINT sp"
				** was executed.
				**
				** The solution is to write the current data for page X into the 
				** sub-journal file now (if it is not already there), so that it will
				** be restored to its current value when the "ROLLBACK TO sp" is 
				** executed.
				*/
				if( NEVER(
					rc==SQLITE_OK && pPg->pgno>pPager->dbSize && subjRequiresPage(pPg)
					) ){
						rc = subjournalPage(pPg);
				}

				/* Write the contents of the page out to the database file. */
				if( rc==SQLITE_OK ){
					assert( (pPg->flags&PGHDR_NEED_SYNC)==0 );
					rc = pager_write_pagelist(pPager, pPg);
				}
			}

			/* Mark the page as clean. */
			if( rc==SQLITE_OK ){
				PAGERTRACE(("STRESS %d page %d\n", PAGERID(pPager), pPg->pgno));
				sqlite3PcacheMakeClean(pPg);
			}

			return pager_error(pPager, rc); 
		}

		int sqlite3PagerOpen(
			sqlite3_vfs *pVfs,       /* The virtual file system to use */
			Pager **ppPager,         /* OUT: Return the Pager structure here */
			const char *zFilename,   /* Name of the database file to open */
			int nExtra,              /* Extra bytes append to each in-memory page */
			int flags,               /* flags controlling this file */
			int vfsFlags,            /* flags passed through to sqlite3_vfs.xOpen() */
			void (*xReinit)(DbPage*) /* Function to reinitialize pages */
			){
				u8 *pPtr;
				Pager *pPager = 0;       /* Pager object to allocate and return */
				int rc = SQLITE_OK;      /* Return code */
				int tempFile = 0;        /* True for temp files (incl. in-memory files) */
				int memDb = 0;           /* True if this is an in-memory file */
				int readOnly = 0;        /* True if this is a read-only file */
				int journalFileSize;     /* Bytes to allocate for each journal fd */
				char *zPathname = 0;     /* Full path to database file */
				int nPathname = 0;       /* Number of bytes in zPathname */
				int useJournal = (flags & PAGER_OMIT_JOURNAL)==0; /* False to omit journal */
				int pcacheSize = sqlite3PcacheSize();       /* Bytes to allocate for PCache */
				u32 szPageDflt = SQLITE_DEFAULT_PAGE_SIZE;  /* Default page size */
				const char *zUri = 0;    /* URI args to copy */
				int nUri = 0;            /* Number of bytes of URI args at *zUri */

				/* Figure out how much space is required for each journal file-handle
				** (there are two of them, the main journal and the sub-journal). This
				** is the maximum space required for an in-memory journal file handle 
				** and a regular journal file-handle. Note that a "regular journal-handle"
				** may be a wrapper capable of caching the first portion of the journal
				** file in memory to implement the atomic-write optimization (see 
				** source file journal.c).
				*/
				if( sqlite3JournalSize(pVfs)>sqlite3MemJournalSize() ){
					journalFileSize = ROUND8(sqlite3JournalSize(pVfs));
				}else{
					journalFileSize = ROUND8(sqlite3MemJournalSize());
				}

				/* Set the output variable to NULL in case an error occurs. */
				*ppPager = 0;

#ifndef SQLITE_OMIT_MEMORYDB
				if( flags & PAGER_MEMORY ){
					memDb = 1;
					if( zFilename && zFilename[0] ){
						zPathname = sqlite3DbStrDup(0, zFilename);
						if( zPathname==0  ) return SQLITE_NOMEM;
						nPathname = sqlite3Strlen30(zPathname);
						zFilename = 0;
					}
				}
#endif

				/* Compute and store the full pathname in an allocated buffer pointed
				** to by zPathname, length nPathname. Or, if this is a temporary file,
				** leave both nPathname and zPathname set to 0.
				*/
				if( zFilename && zFilename[0] ){
					const char *z;
					nPathname = pVfs->mxPathname+1;
					zPathname = sqlite3DbMallocRaw(0, nPathname*2);
					if( zPathname==0 ){
						return SQLITE_NOMEM;
					}
					zPathname[0] = 0; /* Make sure initialized even if FullPathname() fails */
					rc = sqlite3OsFullPathname(pVfs, zFilename, nPathname, zPathname);
					nPathname = sqlite3Strlen30(zPathname);
					z = zUri = &zFilename[sqlite3Strlen30(zFilename)+1];
					while( *z ){
						z += sqlite3Strlen30(z)+1;
						z += sqlite3Strlen30(z)+1;
					}
					nUri = (int)(&z[1] - zUri);
					assert( nUri>=0 );
					if( rc==SQLITE_OK && nPathname+8>pVfs->mxPathname ){
						/* This branch is taken when the journal path required by
						** the database being opened will be more than pVfs->mxPathname
						** bytes in length. This means the database cannot be opened,
						** as it will not be possible to open the journal file or even
						** check for a hot-journal before reading.
						*/
						rc = SQLITE_CANTOPEN_BKPT;
					}
					if( rc!=SQLITE_OK ){
						sqlite3DbFree(0, zPathname);
						return rc;
					}
				}

				/* Allocate memory for the Pager structure, PCache object, the
				** three file descriptors, the database file name and the journal 
				** file name. The layout in memory is as follows:
				**
				**     Pager object                    (sizeof(Pager) bytes)
				**     PCache object                   (sqlite3PcacheSize() bytes)
				**     Database file handle            (pVfs->szOsFile bytes)
				**     Sub-journal file handle         (journalFileSize bytes)
				**     Main journal file handle        (journalFileSize bytes)
				**     Database file name              (nPathname+1 bytes)
				**     Journal file name               (nPathname+8+1 bytes)
				*/
				pPtr = (u8 *)sqlite3MallocZero(
					ROUND8(sizeof(*pPager)) +      /* Pager structure */
					ROUND8(pcacheSize) +           /* PCache object */
					ROUND8(pVfs->szOsFile) +       /* The main db file */
					journalFileSize * 2 +          /* The two journal files */ 
					nPathname + 1 + nUri +         /* zFilename */
					nPathname + 8 + 2              /* zJournal */
#ifndef SQLITE_OMIT_WAL
					+ nPathname + 4 + 2            /* zWal */
#endif
					);
				assert( EIGHT_BYTE_ALIGNMENT(SQLITE_INT_TO_PTR(journalFileSize)) );
				if( !pPtr ){
					sqlite3DbFree(0, zPathname);
					return SQLITE_NOMEM;
				}
				pPager =              (Pager*)(pPtr);
				pPager->pPCache =    (PCache*)(pPtr += ROUND8(sizeof(*pPager)));
				pPager->fd =   (sqlite3_file*)(pPtr += ROUND8(pcacheSize));
				pPager->sjfd = (sqlite3_file*)(pPtr += ROUND8(pVfs->szOsFile));
				pPager->jfd =  (sqlite3_file*)(pPtr += journalFileSize);
				pPager->zFilename =    (char*)(pPtr += journalFileSize);
				assert( EIGHT_BYTE_ALIGNMENT(pPager->jfd) );

				/* Fill in the Pager.zFilename and Pager.zJournal buffers, if required. */
				if( zPathname ){
					assert( nPathname>0 );
					pPager->zJournal =   (char*)(pPtr += nPathname + 1 + nUri);
					memcpy(pPager->zFilename, zPathname, nPathname);
					if( nUri ) memcpy(&pPager->zFilename[nPathname+1], zUri, nUri);
					memcpy(pPager->zJournal, zPathname, nPathname);
					memcpy(&pPager->zJournal[nPathname], "-journal\000", 8+2);
					sqlite3FileSuffix3(pPager->zFilename, pPager->zJournal);
#ifndef SQLITE_OMIT_WAL
					pPager->zWal = &pPager->zJournal[nPathname+8+1];
					memcpy(pPager->zWal, zPathname, nPathname);
					memcpy(&pPager->zWal[nPathname], "-wal\000", 4+1);
					sqlite3FileSuffix3(pPager->zFilename, pPager->zWal);
#endif
					sqlite3DbFree(0, zPathname);
				}
				pPager->pVfs = pVfs;
				pPager->vfsFlags = vfsFlags;

				/* Open the pager file.
				*/
				if( zFilename && zFilename[0] ){
					int fout = 0;                    /* VFS flags returned by xOpen() */
					rc = sqlite3OsOpen(pVfs, pPager->zFilename, pPager->fd, vfsFlags, &fout);
					assert( !memDb );
					readOnly = (fout&SQLITE_OPEN_READONLY);

					/* If the file was successfully opened for read/write access,
					** choose a default page size in case we have to create the
					** database file. The default page size is the maximum of:
					**
					**    + SQLITE_DEFAULT_PAGE_SIZE,
					**    + The value returned by sqlite3OsSectorSize()
					**    + The largest page size that can be written atomically.
					*/
					if( rc==SQLITE_OK && !readOnly ){
						setSectorSize(pPager);
						assert(SQLITE_DEFAULT_PAGE_SIZE<=SQLITE_MAX_DEFAULT_PAGE_SIZE);
						if( szPageDflt<pPager->sectorSize ){
							if( pPager->sectorSize>SQLITE_MAX_DEFAULT_PAGE_SIZE ){
								szPageDflt = SQLITE_MAX_DEFAULT_PAGE_SIZE;
							}else{
								szPageDflt = (u32)pPager->sectorSize;
							}
						}
#ifdef SQLITE_ENABLE_ATOMIC_WRITE
						{
							int iDc = sqlite3OsDeviceCharacteristics(pPager->fd);
							int ii;
							assert(SQLITE_IOCAP_ATOMIC512==(512>>8));
							assert(SQLITE_IOCAP_ATOMIC64K==(65536>>8));
							assert(SQLITE_MAX_DEFAULT_PAGE_SIZE<=65536);
							for(ii=szPageDflt; ii<=SQLITE_MAX_DEFAULT_PAGE_SIZE; ii=ii*2){
								if( iDc&(SQLITE_IOCAP_ATOMIC|(ii>>8)) ){
									szPageDflt = ii;
								}
							}
						}
#endif
					}
				}else{
					/* If a temporary file is requested, it is not opened immediately.
					** In this case we accept the default page size and delay actually
					** opening the file until the first call to OsWrite().
					**
					** This branch is also run for an in-memory database. An in-memory
					** database is the same as a temp-file that is never written out to
					** disk and uses an in-memory rollback journal.
					*/ 
					tempFile = 1;
					pPager->eState = PAGER_READER;
					pPager->eLock = EXCLUSIVE_LOCK;
					readOnly = (vfsFlags&SQLITE_OPEN_READONLY);
				}

				/* The following call to PagerSetPagesize() serves to set the value of 
				** Pager.pageSize and to allocate the Pager.pTmpSpace buffer.
				*/
				if( rc==SQLITE_OK ){
					assert( pPager->memDb==0 );
					rc = sqlite3PagerSetPagesize(pPager, &szPageDflt, -1);
					testcase( rc!=SQLITE_OK );
				}

				/* If an error occurred in either of the blocks above, free the 
				** Pager structure and close the file.
				*/
				if( rc!=SQLITE_OK ){
					assert( !pPager->pTmpSpace );
					sqlite3OsClose(pPager->fd);
					sqlite3_free(pPager);
					return rc;
				}

				/* Initialize the PCache object. */
				assert( nExtra<1000 );
				nExtra = ROUND8(nExtra);
				sqlite3PcacheOpen(szPageDflt, nExtra, !memDb,
					!memDb?pagerStress:0, (void *)pPager, pPager->pPCache);

				PAGERTRACE(("OPEN %d %s\n", FILEHANDLEID(pPager->fd), pPager->zFilename));
				IOTRACE(("OPEN %p %s\n", pPager, pPager->zFilename))

					pPager->useJournal = (u8)useJournal;
				/* pPager->stmtOpen = 0; */
				/* pPager->stmtInUse = 0; */
				/* pPager->nRef = 0; */
				/* pPager->stmtSize = 0; */
				/* pPager->stmtJSize = 0; */
				/* pPager->nPage = 0; */
				pPager->mxPgno = SQLITE_MAX_PAGE_COUNT;
				/* pPager->state = PAGER_UNLOCK; */
#if 0
				assert( pPager->state == (tempFile ? PAGER_EXCLUSIVE : PAGER_UNLOCK) );
#endif
				/* pPager->errMask = 0; */
				pPager->tempFile = (u8)tempFile;
				assert( tempFile==PAGER_LOCKINGMODE_NORMAL 
					|| tempFile==PAGER_LOCKINGMODE_EXCLUSIVE );
				assert( PAGER_LOCKINGMODE_EXCLUSIVE==1 );
				pPager->exclusiveMode = (u8)tempFile; 
				pPager->changeCountDone = pPager->tempFile;
				pPager->memDb = (u8)memDb;
				pPager->readOnly = (u8)readOnly;
				assert( useJournal || pPager->tempFile );
				pPager->noSync = pPager->tempFile;
				if( pPager->noSync ){
					assert( pPager->fullSync==0 );
					assert( pPager->syncFlags==0 );
					assert( pPager->walSyncFlags==0 );
					assert( pPager->ckptSyncFlags==0 );
				}else{
					pPager->fullSync = 1;
					pPager->syncFlags = SQLITE_SYNC_NORMAL;
					pPager->walSyncFlags = SQLITE_SYNC_NORMAL | WAL_SYNC_TRANSACTIONS;
					pPager->ckptSyncFlags = SQLITE_SYNC_NORMAL;
				}
				/* pPager->pFirst = 0; */
				/* pPager->pFirstSynced = 0; */
				/* pPager->pLast = 0; */
				pPager->nExtra = (u16)nExtra;
				pPager->journalSizeLimit = SQLITE_DEFAULT_JOURNAL_SIZE_LIMIT;
				assert( isOpen(pPager->fd) || tempFile );
				setSectorSize(pPager);
				if( !useJournal ){
					pPager->journalMode = PAGER_JOURNALMODE_OFF;
				}else if( memDb ){
					pPager->journalMode = PAGER_JOURNALMODE_MEMORY;
				}
				/* pPager->xBusyHandler = 0; */
				/* pPager->pBusyHandlerArg = 0; */
				pPager->xReiniter = xReinit;
				/* memset(pPager->aHash, 0, sizeof(pPager->aHash)); */

				*ppPager = pPager;
				return SQLITE_OK;
		}

		static int hasHotJournal(Pager *pPager, int *pExists){
			sqlite3_vfs * const pVfs = pPager->pVfs;
			int rc = SQLITE_OK;           /* Return code */
			int exists = 1;               /* True if a journal file is present */
			int jrnlOpen = !!isOpen(pPager->jfd);

			assert( pPager->useJournal );
			assert( isOpen(pPager->fd) );
			assert( pPager->eState==PAGER_OPEN );

			assert( jrnlOpen==0 || ( sqlite3OsDeviceCharacteristics(pPager->jfd) &
				SQLITE_IOCAP_UNDELETABLE_WHEN_OPEN
				));

			*pExists = 0;
			if( !jrnlOpen ){
				rc = sqlite3OsAccess(pVfs, pPager->zJournal, SQLITE_ACCESS_EXISTS, &exists);
			}
			if( rc==SQLITE_OK && exists ){
				int locked = 0;             /* True if some process holds a RESERVED lock */

				/* Race condition here:  Another process might have been holding the
				** the RESERVED lock and have a journal open at the sqlite3OsAccess() 
				** call above, but then delete the journal and drop the lock before
				** we get to the following sqlite3OsCheckReservedLock() call.  If that
				** is the case, this routine might think there is a hot journal when
				** in fact there is none.  This results in a false-positive which will
				** be dealt with by the playback routine.  Ticket #3883.
				*/
				rc = sqlite3OsCheckReservedLock(pPager->fd, &locked);
				if( rc==SQLITE_OK && !locked ){
					Pgno nPage;                 /* Number of pages in database file */

					/* Check the size of the database file. If it consists of 0 pages,
					** then delete the journal file. See the header comment above for 
					** the reasoning here.  Delete the obsolete journal file under
					** a RESERVED lock to avoid race conditions and to avoid violating
					** [H33020].
					*/
					rc = pagerPagecount(pPager, &nPage);
					if( rc==SQLITE_OK ){
						if( nPage==0 ){
							sqlite3BeginBenignMalloc();
							if( pagerLockDb(pPager, RESERVED_LOCK)==SQLITE_OK ){
								sqlite3OsDelete(pVfs, pPager->zJournal, 0);
								if( !pPager->exclusiveMode ) pagerUnlockDb(pPager, SHARED_LOCK);
							}
							sqlite3EndBenignMalloc();
						}else{
							/* The journal file exists and no other connection has a reserved
							** or greater lock on the database file. Now check that there is
							** at least one non-zero bytes at the start of the journal file.
							** If there is, then we consider this journal to be hot. If not, 
							** it can be ignored.
							*/
							if( !jrnlOpen ){
								int f = SQLITE_OPEN_READONLY|SQLITE_OPEN_MAIN_JOURNAL;
								rc = sqlite3OsOpen(pVfs, pPager->zJournal, pPager->jfd, f, &f);
							}
							if( rc==SQLITE_OK ){
								u8 first = 0;
								rc = sqlite3OsRead(pPager->jfd, (void *)&first, 1, 0);
								if( rc==SQLITE_IOERR_SHORT_READ ){
									rc = SQLITE_OK;
								}
								if( !jrnlOpen ){
									sqlite3OsClose(pPager->jfd);
								}
								*pExists = (first!=0);
							}else if( rc==SQLITE_CANTOPEN ){
								/* If we cannot open the rollback journal file in order to see if
								** its has a zero header, that might be due to an I/O error, or
								** it might be due to the race condition described above and in
								** ticket #3883.  Either way, assume that the journal is hot.
								** This might be a false positive.  But if it is, then the
								** automatic journal playback and recovery mechanism will deal
								** with it under an EXCLUSIVE lock where we do not need to
								** worry so much with race conditions.
								*/
								*pExists = 1;
								rc = SQLITE_OK;
							}
						}
					}
				}
			}

			return rc;
		}

		int sqlite3PagerSharedLock(Pager *pPager){
			int rc = SQLITE_OK;                /* Return code */

			/* This routine is only called from b-tree and only when there are no
			** outstanding pages. This implies that the pager state should either
			** be OPEN or READER. READER is only possible if the pager is or was in 
			** exclusive access mode.
			*/
			assert( sqlite3PcacheRefCount(pPager->pPCache)==0 );
			assert( assert_pager_state(pPager) );
			assert( pPager->eState==PAGER_OPEN || pPager->eState==PAGER_READER );
			if( NEVER(MEMDB && pPager->errCode) ){ return pPager->errCode; }

			if( !pagerUseWal(pPager) && pPager->eState==PAGER_OPEN ){
				int bHotJournal = 1;          /* True if there exists a hot journal-file */

				assert( !MEMDB );

				rc = pager_wait_on_lock(pPager, SHARED_LOCK);
				if( rc!=SQLITE_OK ){
					assert( pPager->eLock==NO_LOCK || pPager->eLock==UNKNOWN_LOCK );
					goto failed;
				}

				/* If a journal file exists, and there is no RESERVED lock on the
				** database file, then it either needs to be played back or deleted.
				*/
				if( pPager->eLock<=SHARED_LOCK ){
					rc = hasHotJournal(pPager, &bHotJournal);
				}
				if( rc!=SQLITE_OK ){
					goto failed;
				}
				if( bHotJournal ){
					if( pPager->readOnly ){
						rc = SQLITE_READONLY_ROLLBACK;
						goto failed;
					}

					/* Get an EXCLUSIVE lock on the database file. At this point it is
					** important that a RESERVED lock is not obtained on the way to the
					** EXCLUSIVE lock. If it were, another process might open the
					** database file, detect the RESERVED lock, and conclude that the
					** database is safe to read while this process is still rolling the 
					** hot-journal back.
					** 
					** Because the intermediate RESERVED lock is not requested, any
					** other process attempting to access the database file will get to 
					** this point in the code and fail to obtain its own EXCLUSIVE lock 
					** on the database file.
					**
					** Unless the pager is in locking_mode=exclusive mode, the lock is
					** downgraded to SHARED_LOCK before this function returns.
					*/
					rc = pagerLockDb(pPager, EXCLUSIVE_LOCK);
					if( rc!=SQLITE_OK ){
						goto failed;
					}

					/* If it is not already open and the file exists on disk, open the 
					** journal for read/write access. Write access is required because 
					** in exclusive-access mode the file descriptor will be kept open 
					** and possibly used for a transaction later on. Also, write-access 
					** is usually required to finalize the journal in journal_mode=persist 
					** mode (and also for journal_mode=truncate on some systems).
					**
					** If the journal does not exist, it usually means that some 
					** other connection managed to get in and roll it back before 
					** this connection obtained the exclusive lock above. Or, it 
					** may mean that the pager was in the error-state when this
					** function was called and the journal file does not exist.
					*/
					if( !isOpen(pPager->jfd) ){
						sqlite3_vfs * const pVfs = pPager->pVfs;
						int bExists;              /* True if journal file exists */
						rc = sqlite3OsAccess(
							pVfs, pPager->zJournal, SQLITE_ACCESS_EXISTS, &bExists);
						if( rc==SQLITE_OK && bExists ){
							int fout = 0;
							int f = SQLITE_OPEN_READWRITE|SQLITE_OPEN_MAIN_JOURNAL;
							assert( !pPager->tempFile );
							rc = sqlite3OsOpen(pVfs, pPager->zJournal, pPager->jfd, f, &fout);
							assert( rc!=SQLITE_OK || isOpen(pPager->jfd) );
							if( rc==SQLITE_OK && fout&SQLITE_OPEN_READONLY ){
								rc = SQLITE_CANTOPEN_BKPT;
								sqlite3OsClose(pPager->jfd);
							}
						}
					}

					/* Playback and delete the journal.  Drop the database write
					** lock and reacquire the read lock. Purge the cache before
					** playing back the hot-journal so that we don't end up with
					** an inconsistent cache.  Sync the hot journal before playing
					** it back since the process that crashed and left the hot journal
					** probably did not sync it and we are required to always sync
					** the journal before playing it back.
					*/
					if( isOpen(pPager->jfd) ){
						assert( rc==SQLITE_OK );
						rc = pagerSyncHotJournal(pPager);
						if( rc==SQLITE_OK ){
							rc = pager_playback(pPager, 1);
							pPager->eState = PAGER_OPEN;
						}
					}else if( !pPager->exclusiveMode ){
						pagerUnlockDb(pPager, SHARED_LOCK);
					}

					if( rc!=SQLITE_OK ){
						/* This branch is taken if an error occurs while trying to open
						** or roll back a hot-journal while holding an EXCLUSIVE lock. The
						** pager_unlock() routine will be called before returning to unlock
						** the file. If the unlock attempt fails, then Pager.eLock must be
						** set to UNKNOWN_LOCK (see the comment above the #define for 
						** UNKNOWN_LOCK above for an explanation). 
						**
						** In order to get pager_unlock() to do this, set Pager.eState to
						** PAGER_ERROR now. This is not actually counted as a transition
						** to ERROR state in the state diagram at the top of this file,
						** since we know that the same call to pager_unlock() will very
						** shortly transition the pager object to the OPEN state. Calling
						** assert_pager_state() would fail now, as it should not be possible
						** to be in ERROR state when there are zero outstanding page 
						** references.
						*/
						pager_error(pPager, rc);
						goto failed;
					}

					assert( pPager->eState==PAGER_OPEN );
					assert( (pPager->eLock==SHARED_LOCK)
						|| (pPager->exclusiveMode && pPager->eLock>SHARED_LOCK)
						);
				}

				if( !pPager->tempFile 
					&& (pPager->pBackup || sqlite3PcachePagecount(pPager->pPCache)>0) 
					){
						/* The shared-lock has just been acquired on the database file
						** and there are already pages in the cache (from a previous
						** read or write transaction).  Check to see if the database
						** has been modified.  If the database has changed, flush the
						** cache.
						**
						** Database changes is detected by looking at 15 bytes beginning
						** at offset 24 into the file.  The first 4 of these 16 bytes are
						** a 32-bit counter that is incremented with each change.  The
						** other bytes change randomly with each file change when
						** a codec is in use.
						** 
						** There is a vanishingly small chance that a change will not be 
						** detected.  The chance of an undetected change is so small that
						** it can be neglected.
						*/
						Pgno nPage = 0;
						char dbFileVers[sizeof(pPager->dbFileVers)];

						rc = pagerPagecount(pPager, &nPage);
						if( rc ) goto failed;

						if( nPage>0 ){
							IOTRACE(("CKVERS %p %d\n", pPager, sizeof(dbFileVers)));
							rc = sqlite3OsRead(pPager->fd, &dbFileVers, sizeof(dbFileVers), 24);
							if( rc!=SQLITE_OK ){
								goto failed;
							}
						}else{
							memset(dbFileVers, 0, sizeof(dbFileVers));
						}

						if( memcmp(pPager->dbFileVers, dbFileVers, sizeof(dbFileVers))!=0 ){
							pager_reset(pPager);
						}
				}

				/* If there is a WAL file in the file-system, open this database in WAL
				** mode. Otherwise, the following function call is a no-op.
				*/
				rc = pagerOpenWalIfPresent(pPager);
#ifndef SQLITE_OMIT_WAL
				assert( pPager->pWal==0 || rc==SQLITE_OK );
#endif
			}

			if( pagerUseWal(pPager) ){
				assert( rc==SQLITE_OK );
				rc = pagerBeginReadTransaction(pPager);
			}

			if( pPager->eState==PAGER_OPEN && rc==SQLITE_OK ){
				rc = pagerPagecount(pPager, &pPager->dbSize);
			}

failed:
			if( rc!=SQLITE_OK ){
				assert( !MEMDB );
				pager_unlock(pPager);
				assert( pPager->eState==PAGER_OPEN );
			}else{
				pPager->eState = PAGER_READER;
			}
			return rc;
		}

		static void pagerUnlockIfUnused(Pager *pPager){
			if( (sqlite3PcacheRefCount(pPager->pPCache)==0) ){
				pagerUnlockAndRollback(pPager);
			}
		}

		int sqlite3PagerAcquire(
			Pager *pPager,      /* The pager open on the database file */
			Pgno pgno,          /* Page number to fetch */
			DbPage **ppPage,    /* Write a pointer to the page here */
			int noContent       /* Do not bother reading content from disk if true */
			){
				int rc;
				PgHdr *pPg;

				assert( pPager->eState>=PAGER_READER );
				assert( assert_pager_state(pPager) );

				if( pgno==0 ){
					return SQLITE_CORRUPT_BKPT;
				}

				/* If the pager is in the error state, return an error immediately. 
				** Otherwise, request the page from the PCache layer. */
				if( pPager->errCode!=SQLITE_OK ){
					rc = pPager->errCode;
				}else{
					rc = sqlite3PcacheFetch(pPager->pPCache, pgno, 1, ppPage);
				}

				if( rc!=SQLITE_OK ){
					/* Either the call to sqlite3PcacheFetch() returned an error or the
					** pager was already in the error-state when this function was called.
					** Set pPg to 0 and jump to the exception handler.  */
					pPg = 0;
					goto pager_acquire_err;
				}
				assert( (*ppPage)->pgno==pgno );
				assert( (*ppPage)->pPager==pPager || (*ppPage)->pPager==0 );

				if( (*ppPage)->pPager && !noContent ){
					/* In this case the pcache already contains an initialized copy of
					** the page. Return without further ado.  */
					assert( pgno<=PAGER_MAX_PGNO && pgno!=PAGER_MJ_PGNO(pPager) );
					pPager->aStat[PAGER_STAT_HIT]++;
					return SQLITE_OK;

				}else{
					/* The pager cache has created a new page. Its content needs to 
					** be initialized.  */

					pPg = *ppPage;
					pPg->pPager = pPager;

					/* The maximum page number is 2^31. Return SQLITE_CORRUPT if a page
					** number greater than this, or the unused locking-page, is requested. */
					if( pgno>PAGER_MAX_PGNO || pgno==PAGER_MJ_PGNO(pPager) ){
						rc = SQLITE_CORRUPT_BKPT;
						goto pager_acquire_err;
					}

					if( MEMDB || pPager->dbSize<pgno || noContent || !isOpen(pPager->fd) ){
						if( pgno>pPager->mxPgno ){
							rc = SQLITE_FULL;
							goto pager_acquire_err;
						}
						if( noContent ){
							/* Failure to set the bits in the InJournal bit-vectors is benign.
							** It merely means that we might do some extra work to journal a 
							** page that does not need to be journaled.  Nevertheless, be sure 
							** to test the case where a malloc error occurs while trying to set 
							** a bit in a bit vector.
							*/
							sqlite3BeginBenignMalloc();
							if( pgno<=pPager->dbOrigSize ){
								TESTONLY( rc = ) sqlite3BitvecSet(pPager->pInJournal, pgno);
								testcase( rc==SQLITE_NOMEM );
							}
							TESTONLY( rc = ) addToSavepointBitvecs(pPager, pgno);
							testcase( rc==SQLITE_NOMEM );
							sqlite3EndBenignMalloc();
						}
						memset(pPg->pData, 0, pPager->pageSize);
						IOTRACE(("ZERO %p %d\n", pPager, pgno));
					}else{
						assert( pPg->pPager==pPager );
						pPager->aStat[PAGER_STAT_MISS]++;
						rc = readDbPage(pPg);
						if( rc!=SQLITE_OK ){
							goto pager_acquire_err;
						}
					}
					pager_set_pagehash(pPg);
				}

				return SQLITE_OK;

pager_acquire_err:
				assert( rc!=SQLITE_OK );
				if( pPg ){
					sqlite3PcacheDrop(pPg);
				}
				pagerUnlockIfUnused(pPager);

				*ppPage = 0;
				return rc;
		}

		DbPage *sqlite3PagerLookup(Pager *pPager, Pgno pgno){
			PgHdr *pPg = 0;
			assert( pPager!=0 );
			assert( pgno!=0 );
			assert( pPager->pPCache!=0 );
			assert( pPager->eState>=PAGER_READER && pPager->eState!=PAGER_ERROR );
			sqlite3PcacheFetch(pPager->pPCache, pgno, 0, &pPg);
			return pPg;
		}

		void sqlite3PagerUnref(DbPage *pPg){
			if( pPg ){
				Pager *pPager = pPg->pPager;
				sqlite3PcacheRelease(pPg);
				pagerUnlockIfUnused(pPager);
			}
		}

		static int pager_open_journal(Pager *pPager){
			int rc = SQLITE_OK;                        /* Return code */
			sqlite3_vfs * const pVfs = pPager->pVfs;   /* Local cache of vfs pointer */

			assert( pPager->eState==PAGER_WRITER_LOCKED );
			assert( assert_pager_state(pPager) );
			assert( pPager->pInJournal==0 );

			/* If already in the error state, this function is a no-op.  But on
			** the other hand, this routine is never called if we are already in
			** an error state. */
			if( NEVER(pPager->errCode) ) return pPager->errCode;

			if( !pagerUseWal(pPager) && pPager->journalMode!=PAGER_JOURNALMODE_OFF ){
				pPager->pInJournal = sqlite3BitvecCreate(pPager->dbSize);
				if( pPager->pInJournal==0 ){
					return SQLITE_NOMEM;
				}

				/* Open the journal file if it is not already open. */
				if( !isOpen(pPager->jfd) ){
					if( pPager->journalMode==PAGER_JOURNALMODE_MEMORY ){
						sqlite3MemJournalOpen(pPager->jfd);
					}else{
						const int flags =                   /* VFS flags to open journal file */
							SQLITE_OPEN_READWRITE|SQLITE_OPEN_CREATE|
							(pPager->tempFile ? 
							(SQLITE_OPEN_DELETEONCLOSE|SQLITE_OPEN_TEMP_JOURNAL):
						(SQLITE_OPEN_MAIN_JOURNAL)
							);
#ifdef SQLITE_ENABLE_ATOMIC_WRITE
						rc = sqlite3JournalOpen(
							pVfs, pPager->zJournal, pPager->jfd, flags, jrnlBufferSize(pPager)
							);
#else
						rc = sqlite3OsOpen(pVfs, pPager->zJournal, pPager->jfd, flags, 0);
#endif
					}
					assert( rc!=SQLITE_OK || isOpen(pPager->jfd) );
				}


				/* Write the first journal header to the journal file and open 
				** the sub-journal if necessary.
				*/
				if( rc==SQLITE_OK ){
					/* TODO: Check if all of these are really required. */
					pPager->nRec = 0;
					pPager->journalOff = 0;
					pPager->setMaster = 0;
					pPager->journalHdr = 0;
					rc = writeJournalHdr(pPager);
				}
			}

			if( rc!=SQLITE_OK ){
				sqlite3BitvecDestroy(pPager->pInJournal);
				pPager->pInJournal = 0;
			}else{
				assert( pPager->eState==PAGER_WRITER_LOCKED );
				pPager->eState = PAGER_WRITER_CACHEMOD;
			}

			return rc;
		}

		int sqlite3PagerBegin(Pager *pPager, int exFlag, int subjInMemory){
			int rc = SQLITE_OK;

			if( pPager->errCode ) return pPager->errCode;
			assert( pPager->eState>=PAGER_READER && pPager->eState<PAGER_ERROR );
			pPager->subjInMemory = (u8)subjInMemory;

			if( ALWAYS(pPager->eState==PAGER_READER) ){
				assert( pPager->pInJournal==0 );

				if( pagerUseWal(pPager) ){
					/* If the pager is configured to use locking_mode=exclusive, and an
					** exclusive lock on the database is not already held, obtain it now.
					*/
					if( pPager->exclusiveMode && sqlite3WalExclusiveMode(pPager->pWal, -1) ){
						rc = pagerLockDb(pPager, EXCLUSIVE_LOCK);
						if( rc!=SQLITE_OK ){
							return rc;
						}
						sqlite3WalExclusiveMode(pPager->pWal, 1);
					}

					/* Grab the write lock on the log file. If successful, upgrade to
					** PAGER_RESERVED state. Otherwise, return an error code to the caller.
					** The busy-handler is not invoked if another connection already
					** holds the write-lock. If possible, the upper layer will call it.
					*/
					rc = sqlite3WalBeginWriteTransaction(pPager->pWal);
				}else{
					/* Obtain a RESERVED lock on the database file. If the exFlag parameter
					** is true, then immediately upgrade this to an EXCLUSIVE lock. The
					** busy-handler callback can be used when upgrading to the EXCLUSIVE
					** lock, but not when obtaining the RESERVED lock.
					*/
					rc = pagerLockDb(pPager, RESERVED_LOCK);
					if( rc==SQLITE_OK && exFlag ){
						rc = pager_wait_on_lock(pPager, EXCLUSIVE_LOCK);
					}
				}

				if( rc==SQLITE_OK ){
					/* Change to WRITER_LOCKED state.
					**
					** WAL mode sets Pager.eState to PAGER_WRITER_LOCKED or CACHEMOD
					** when it has an open transaction, but never to DBMOD or FINISHED.
					** This is because in those states the code to roll back savepoint 
					** transactions may copy data from the sub-journal into the database 
					** file as well as into the page cache. Which would be incorrect in 
					** WAL mode.
					*/
					pPager->eState = PAGER_WRITER_LOCKED;
					pPager->dbHintSize = pPager->dbSize;
					pPager->dbFileSize = pPager->dbSize;
					pPager->dbOrigSize = pPager->dbSize;
					pPager->journalOff = 0;
				}

				assert( rc==SQLITE_OK || pPager->eState==PAGER_READER );
				assert( rc!=SQLITE_OK || pPager->eState==PAGER_WRITER_LOCKED );
				assert( assert_pager_state(pPager) );
			}

			PAGERTRACE(("TRANSACTION %d\n", PAGERID(pPager)));
			return rc;
		}

		static int pager_write(PgHdr *pg)
		{
			void *data = pg->Data;
			Pager *pager = pg->Pager;
			RC rc = RC::OK;

			// This routine is not called unless a write-transaction has already been started. The journal file may or may not be open at this point. It is never called in the ERROR state.
			_assert(pager->State == PAGER::WRITER_LOCKED
				|| pager->State == PAGER::WRITER_CACHEMOD
				|| pager->State == PAGER::WRITER_DBMOD);
			_assert(assert_pager_state(pager));

			// If an error has been previously detected, report the same error again. This should not happen, but the check provides robustness.
			if (SysEx_NEVER(pager->ErrorCode)) return pager->ErrorCode;

			// Higher-level routines never call this function if database is not writable.  But check anyway, just for robustness.
			if (SysEx_NEVER(pager->ReadOnly)) return RC::PERM;

			CHECK_PAGE(pg);

			// The journal file needs to be opened. Higher level routines have already obtained the necessary locks to begin the write-transaction, but the
			// rollback journal might not yet be open. Open it now if this is the case.
			//
			// This is done before calling sqlite3PcacheMakeDirty() on the page. Otherwise, if it were done after calling sqlite3PcacheMakeDirty(), then
			// an error might occur and the pager would end up in WRITER_LOCKED state with pages marked as dirty in the cache.
			if (pager->State == PAGER::WRITER_LOCKED)
			{
				rc = pager_open_journal(pager);
				if (rc != RC::OK) return rc;
			}
			_assert(pager->State >= PAGER::WRITER_CACHEMOD);
			_assert(assert_pager_state(pager));

			// Mark the page as dirty.  If the page has already been written to the journal then we can return right away.
			sqlite3PcacheMakeDirty(pg);
			if (pageInJournal(pg) && !subjRequiresPage(pg))
				_assert(!UseWal(pager));
			else
			{
				// The transaction journal now exists and we have a RESERVED or an EXCLUSIVE lock on the main database file.  Write the current page to the transaction journal if it is not there already.
				if (!pageInJournal(pg) && !UseWal(pager))
				{
					_assert(UseWal(pager) == 0);
					if (pg->ID <= pager->DBOrigSize && pager->JournalFile->Opened)
					{
						// We should never write to the journal file the page that contains the database locks.  The following assert verifies that we do not.
						_assert(pg->ID != PAGER_MJ_PGNO(pager));

						_assert(pager->JournalHeader <= pager->JournalOffset);
						CODEC2(pager, data, pg->ID, 7, return RC::NOMEM, data2);
						char *data2;
						uint32 cksum = pager_cksum(pager, (uint8 *)data2);

						// Even if an IO or diskfull error occurs while journalling the page in the block above, set the need-sync flag for the page.
						// Otherwise, when the transaction is rolled back, the logic in playback_one_page() will think that the page needs to be restored
						// in the database file. And if an IO error occurs while doing so, then corruption may follow.
						pg->Flags |= PgHdr::PGHDR::NEED_SYNC;

						int64 offset = pager->JournalOffset;
						rc = pPager->JournalFile->Write4(offset, pg->ID);
						if (rc != RC::OK) return rc;
						rc = pager->JournalFile->Write(data2, pager->PageSize, offset + 4);
						if (rc != RC::OK) return rc;
						rc = pager->JournalFile->Write4(offset + pager->PageSize + 4, checksum);
						if (rc != RC::OK) return rc;

						IOTRACE("JOUT %p %d %lld %d\n", pager, pg->ID, pager->JournalOffset, pager->PageSize);
						PAGER_INCR(sqlite3_pager_writej_count);
						PAGERTRACE("JOURNAL %d page %d needSync=%d hash(%08x)\n", PAGERID(pager), pg->ID, ((pg->Flags & PgHdr::PGHDR::NEED_SYNC) ? 1 : 0), pager_pagehash(pg)));

						pager->JournalOff += 8 + pager->PageSize;
						pager->Records++;
						_assert(pager->InJournal != 0);
						rc = pager->InJournal->Set(pg->ID);
						ASSERTCOVERAGE(rc == RC::SQLITE);
						_assert(rc == RC::OK || rc == RC::NOMEM);
						rc |= addToSavepointBitvecs(pager, pg->ID);
						if (rc != RC::OK)
						{
							_assert(rc == RC::NOMEM);
							return rc;
						}
					}
					else
					{
						if (pager->State != PAGER::WRITER_DBMOD)
							pg->Flags |= PgHdr::PGHDR::NEED_SYNC;
						PAGERTRACE("APPEND %d page %d needSync=%d\n", PAGERID(pager), pg->ID, ((pg->Flags & PgHdr::PGHDR_NEED_SYNC) ? 1 : 0)));
					}
				}

				// If the statement journal is open and the page is not in it, then write the current page to the statement journal.  Note that
				// the statement journal format differs from the standard journal format in that it omits the checksums and the header.
				if (subjRequiresPage(pg))
					rc = subjournalPage(pg);
			}

			// Update the database size and return.
			if (pager->DBSize < pg->ID)
				pager->DBSize = pg->ID;
			return rc;
		}

		int sqlite3PagerWrite(DbPage *dbPage)
		{
			RC rc = RC::OK;

			PgHdr *pg = dbPage;
			Pager *pager = pg->Pager;
			Pid pagePerSector = (pager->SectorSize / pager->PageSize);

			_assert(pager->State >= PAGER::WRITER_LOCKED);
			_assert(pager->State != PAGER::ERROR);
			_assert(assert_pager_state(pager));

			if (pagePerSector > 1)
			{
				// Set the doNotSyncSpill flag to 1. This is because we cannot allow a journal header to be written between the pages journaled by this function.
				_assert(!pager->MemoryDB);
				_assert(pager->DoNotSyncSpill == 0);
				pager->DoNotSyncSpill++;

				// This trick assumes that both the page-size and sector-size are an integer power of 2. It sets variable pg1 to the identifier of the first page of the sector pPg is located on.
				Pid pg1 = ((pg->ID - 1) & ~(nagePerSector - 1)) + 1; // First page of the sector pPg is located on.

				int pages = 0; // Number of pages starting at pg1 to journal
				Pid pageCount = pager->DBSize; // Total number of pages in database file
				if (pg->ID > pageCount)
					pages = (pg->ID - pg1) + 1;
				else if ((pg1 + pagePerSector - 1) > nageCount)
					pages = pageCount + 1 - pg1;
				else
					pages = pagePerSector;
				_assert(pages > 0);
				_assert(pg1 <= pg->ID);
				_assert((pg1 + pages) > pg->ID);

				bool needSync = false; // True if any page has PGHDR_NEED_SYNC
				for (int ii = 0; ii < pages && rc == RC::OK; ii++)
				{
					PID id = pg1 + ii;
					PgHdr *page;
					if (id == pg->ID || !sqlite3BitvecTest(pager->InJournal->Get(id))
					{
						if (id != PAGER_MJ_PGNO(pager))
						{
							rc = sqlite3PagerGet(pager, id, &page);
							if (rc == RC::OK)
							{
								rc = pager_write(page);
								if ( age->Flags & PgHdr::PGHDR::NEED_SYNC)
									needSync = true;
								sqlite3PagerUnref(page);
							}
						}
					}
					else if ((page = pager_lookup(pager, id)) != 0)
					{
						if (page->Flags & PgHdr::PGHDR::NEED_SYNC)
							needSync = true;
						sqlite3PagerUnref(page);
					}
				}

				// If the PGHDR_NEED_SYNC flag is set for any of the nPage pages starting at pg1, then it needs to be set for all of them. Because
				// writing to any of these nPage pages may damage the others, the journal file must contain sync()ed copies of all of them
				// before any of them can be written out to the database file.
				if (rc == RC::OK && needSync)
				{
					_assert(!pager->MemoryDB);
					for (int ii = 0; ii < pages; ii++)
					{
						PgHdr *page = pager_lookup(pager, pg1 + ii);
						if (page)
						{
							page->Flags |= PgHdr::PGHDR::NEED_SYNC;
							sqlite3PagerUnref(page);
						}
					}
				}

				_assert(pager->DoNotSyncSpill == 1);
				pager->DoNotSyncSpill--;
			}
			else
				rc = pager_write(dbPage);
			return rc;
		}

#ifndef DEBUG
		int sqlite3PagerIswriteable(DbPage *pg)
		{
			return pg->Flags & PGHDR_DIRTY;
		}
#endif

		void sqlite3PagerDontWrite(PgHdr *pg)
		{
			Pager *pager = pg->Pager;
			if ((pg->Flags & PgHdr::PGHDR::DIRTY) && __arrayLength(pager->Savepoints) == 0)
			{
				PAGERTRACE("DONT_WRITE page %d of %d\n", pg->ID, PAGERID(pager));
				IOTRACE("CLEAN %p %d\n", pager, pg->ID);
				pg->Flags |= PgHdr::PGHDR::DONT_WRITE;
				pager_set_pagehash(pg);
			}
		}

		static RC pager_incr_changecounter(Pager *pager, int isDirectMode)
		{
			_assert(pager->State == PAGER::WRITER_CACHEMOD
				|| pager->State == PAGER::WRITER_DBMOD);
			_assert(assert_pager_state(pager));

			// Declare and initialize constant integer 'isDirect'. If the atomic-write optimization is enabled in this build, then isDirect
			// is initialized to the value passed as the isDirectMode parameter to this function. Otherwise, it is always set to zero.
			//
			// The idea is that if the atomic-write optimization is not enabled at compile time, the compiler can omit the tests of
			// 'isDirect' below, as well as the block enclosed in the "if( isDirect )" condition.
#ifndef ENABLE_ATOMIC_WRITE
#define DIRECT_MODE 0
			assert(isDirectMode==0);
			UNUSED_PARAMETER(isDirectMode);
#else
#define DIRECT_MODE isDirectMode
#endif
			RC rc = RC::OK;
			if (!pager->ChangeCountDone && SysEx::ALWAYS(pager->DBSize > 0))
			{
				_assert(!pager->TempFile && pager->File->Opened);

				// Open page 1 of the file for writing.
				PgHdr *pgHdr; // Reference to page 1
				rc = sqlite3PagerGet(pager, 1, &pgHdr);
				_assert(pgHdr == nullptr || rc == RC::OK);

				// If page one was fetched successfully, and this function is not operating in direct-mode, make page 1 writable.  When not in 
				// direct mode, page 1 is always held in cache and hence the PagerGet() above is always successful - hence the ALWAYS on rc==SQLITE_OK.
				if (!DIRECT_MODE && SysEx_ALWAYS(rc == RC::OK))
					rc = sqlite3PagerWrite(pgHdr);

				if (rc == RC::OK)
				{
					// Actually do the update of the change counter
					pager_write_changecounter(pgHdr);

					// If running in direct mode, write the contents of page 1 to the file.
					if (DIRECT_MODE)
					{
						const void *buf;
						assert(pager->DBFileSize > 0);
						CODEC2(pager, pgHdr->Data, 1, 6, rc = RC::NOMEM, buf);
						if (rc == RC::OK)
						{
							rc = pager->File->Write(buf, pager->PageSize, 0);
							pager->Stats[IPager::STAT::WRITE]++;
						}
						if (rc == RC::OK)
							pager->ChangeCountDone = 1;
					}
					else
					{
						pager->ChangeCountDone = 1;
					}
				}

				// Release the page reference.
				sqlite3PagerUnref(pgHdr);
			}
			return rc;
		}

		RC sqlite3PagerSync(Pager *pager)
		{
			RC rc = RC::OK;
			if (!pager->NSync)
			{
				_assert(!pager->MemoryDB);
				rc = pager->File->Sync(pager->SyncFlags);
			}
			else if (pager->File->Opened)
			{
				_assert(!pager->MemoryDB);
				rc = pager->File->FileControl(FCNT::SYNC_OMITTED, 0);
				if (rc == RC::NOTFOUND)
					rc = RC::OK;
			}
			return rc;
		}

		RC sqlite3PagerExclusiveLock(Pager *pager)
		{
			_assert(pager->State == PAGER::WRITER_CACHEMOD || 
				pager->State == PAGER::WRITER_DBMOD ||
				pager->State == PAGER::WRITER_LOCKED);
			_assert(assert_pager_state(pager));
			RC rc = RC::OK;
			if (!UseWal(pager))
				rc = pager_wait_on_lock(pager, EXCLUSIVE_LOCK);
			return rc;
		}

		RC sqlite3PagerCommitPhaseOne(Pager *pager, const char *master, int noSync)
		{
			_assert(pager->State == PAGER::WRITER_LOCKED ||
				pager->State == PAGER::WRITER_CACHEMOD ||
				pager->State == PAGER::WRITER_DBMOD ||
				pager->State == PAGER::ERROR);
			_assert(assert_pager_state(pager));

			// If a prior error occurred, report that error again.
			if (SysEx_NEVER(pager->ErrorCode)) return pager->ErrorCode;

			PAGERTRACE("DATABASE SYNC: File=%s zMaster=%s nSize=%d\n", pager->Filename, zaster, pager->DBSize);

			// If no database changes have been made, return early.
			if (pager->State < PAGER::WRITER_CACHEMOD) return RC::OK;

			RC rc = RC::OK;
			if (pager->MemoryDB)
			{
				// If this is an in-memory db, or no pages have been written to, or this function has already been called, it is mostly a no-op.  However, any
				// backup in progress needs to be restarted.
				sqlite3BackupRestart(pager->Backup);
			}
			else
			{
				if (UseWal(pager))
				{
					PgHdr *list = PCache_DirtyList(pager->PCache);
					PgHdr *pageOne = nullptr;
					if (list == 0)
					{
						// Must have at least one page for the WAL commit flag. Ticket [2d1a5c67dfc2363e44f29d9bbd57f] 2011-05-18
						rc = sqlite3PagerGet(pager, 1, &pageOne);
						pist = pageOne;
						pist->Dirty = nullptr;
					}
					_assert(rc == RC::OK);
					if (SysEx_ALWAYS(list))
						rc = pagerWalFrames(pager, list, pager->DBSize, 1);
					sqlite3PagerUnref(pageOne);
					if (rc == RC::OK)
					{
						PCache_CleanAll(pager->PCache);
					}
				}
				else
				{
					// The following block updates the change-counter. Exactly how it does this depends on whether or not the atomic-update optimization
					// was enabled at compile time, and if this transaction meets the runtime criteria to use the operation: 
					//
					//    * The file-system supports the atomic-write property for blocks of size page-size, and 
					//    * This commit is not part of a multi-file transaction, and
					//    * Exactly one page has been modified and store in the journal file.
					//
					// If the optimization was not enabled at compile time, then the pager_incr_changecounter() function is called to update the change
					// counter in 'indirect-mode'. If the optimization is compiled in but is not applicable to this transaction, call sqlite3JournalCreate()
					// to make sure the journal file has actually been created, then call pager_incr_changecounter() to update the change-counter in indirect mode. 
					//
					// Otherwise, if the optimization is both enabled and applicable, then call pager_incr_changecounter() to update the change-counter
					// in 'direct' mode. In this case the journal file will never be created for this transaction.
#ifdef ENABLE_ATOMIC_WRITE
					PgHdr *pg;
					_assert(pager->JournalFile->Opened ||
						pager->JournalMode == PAGER::JOURNALMODE_OFF ||
						pager->JournalMode == PAGER::JOURNALMODE_WAL);
					if (!master && pager->JournalFile->Opened &&
						pager->JournalOffset == jrnlBufferSize(pager) &&
						pager->DBSize >= pager->DBOrigSize &&
						((pg = Pcache_DirtyList(pager->PCache)) == nullptr || pg->Dirty == nullptr))
					{
						// Update the db file change counter via the direct-write method. The following call will modify the in-memory representation of page 1 
						// to include the updated change counter and then write page 1 directly to the database file. Because of the atomic-write 
						// property of the host file-system, this is safe.
						rc = pager_incr_changecounter(pager, 1);
					}
					else
					{
						rc = sqlite3JournalCreate(pPager->jfd);
						if (rc RC::OK)
							rc = pager_incr_changecounter(pager, 0);
					}
#else
					rc = pager_incr_changecounter(pager, 0);
#endif
					if (rc != RC::OK) goto commit_phase_one_exit;

					// Write the master journal name into the journal file. If a master journal file name has already been written to the journal file, 
					// or if zMaster is NULL (no master journal), then this call is a no-op.
					rc = writeMasterJournal(pager, master);
					if (rc != RC::OK) goto commit_phase_one_exit;

					// Sync the journal file and write all dirty pages to the database. If the atomic-update optimization is being used, this sync will not 
					// create the journal file or perform any real IO.
					//
					// Because the change-counter page was just modified, unless the atomic-update optimization is used it is almost certain that the
					// journal requires a sync here. However, in locking_mode=exclusive on a system under memory pressure it is just possible that this is 
					// not the case. In this case it is likely enough that the redundant xSync() call will be changed to a no-op by the OS anyhow. 
					rc = syncJournal(pager, 0);
					if (rc != RC::OK) goto commit_phase_one_exit;

					rc = pager_write_pagelist(pager, PCache_DirtyList(pager->PCache));
					if (rc != RC::OK)
					{
						_assert(rc != RC::IOERR_BLOCKED);
						goto commit_phase_one_exit;
					}
					sqlite3PcacheCleanAll(pager->PCache);

					// If the file on disk is smaller than the database image, use pager_truncate to grow the file here. This can happen if the database
					// image was extended as part of the current transaction and then the last page in the db image moved to the free-list. In this case the
					// last page is never written out to disk, leaving the database file undersized. Fix this now if it is the case.
					if (pager->DBSize > pager->DBFileSize)
					{
						Pid newID = pager->DBSize - (pager->DBSize == PAGER_MJ_PGNO(pager));
						_assert(pager->State == PAGER::WRITER_DBMOD);
						rc = pager_truncate(pager, newID);
						if (rc != RC::OK) goto commit_phase_one_exit;
					}

					// Finally, sync the database file.
					if (!noSync)
						rc = sqlite3PagerSync(pager);
					IOTRACE("DBSYNC %p\n", pager);
				}
			}

commit_phase_one_exit:
			if (rc == RC::OK && !UseWal(pager))
				pager->State = PAGER::WRITER_FINISHED;
			return rc;
		}

		RC sqlite3PagerCommitPhaseTwo(Pager *pager)
		{
			// This routine should not be called if a prior error has occurred. But if (due to a coding error elsewhere in the system) it does get
			// called, just return the same error code without doing anything.
			if (SysEx_NEVER(pager->ErrorCode)) return pager->ErrorCode;
			_assert(pager->State == PAGER::WRITER_LOCKED ||
				pager->State == PAGER::WRITER_FINISHED ||
				(UseWal(pager) && pager->State == PAGER::WRITER_CACHEMOD));
			_assert(assert_pager_state(pager));

			// An optimization. If the database was not actually modified during this transaction, the pager is running in exclusive-mode and is
			// using persistent journals, then this function is a no-op.
			//
			// The start of the journal file currently contains a single journal header with the nRec field set to 0. If such a journal is used as
			// a hot-journal during hot-journal rollback, 0 changes will be made to the database file. So there is no need to zero the journal 
			// header. Since the pager is in exclusive mode, there is no need to drop any locks either.
			if (pager->State == PAGER::WRITER_LOCKED &&
				pager->ExclusiveMode &&
				pager->JournalMode == PAGER::JOURNALMODE_PERSIST)
			{
				_assert(pager->JournalOffset == JOURNAL_HDR_SZ(pager) || !pager->JournalOffset);
				pager->State = PAGER::READER;
				return RC::OK;
			}

			PAGERTRACE(("COMMIT %d\n", PAGERID(pager)));
			RC rc = pager_end_transaction(pager, pager->SetMaster, 1);
			return pager_error(pager, rc);
		}

		RC sqlite3PagerRollback(Pager *pager)
		{
			PAGERTRACE("ROLLBACK %d\n", PAGERID(pager));
			// PagerRollback() is a no-op if called in READER or OPEN state. If the pager is already in the ERROR state, the rollback is not attempted here. Instead, the error code is returned to the caller.
			_assert(assert_pager_state(pager));
			if (pager->State == PAGER::ERROR) return pager->ErrorCode;
			if (pager->State <= PAGER::READER) return RC::OK;

			RC rc = RC::OK;
			if (UseWal(pager))
			{
				rc = sqlite3PagerSavepoint(pager, SAVEPOINT::ROLLBACK, -1);
				RC rc2 = pager_end_transaction(pager, pager->SetMaster, 0);
				if (rc == RC::OK) rc = rc2;
			}
			else if (!pager->JournalFile->Opened || pager->State == PAGER::WRITER_LOCKED)
			{
				PAGER state = pager->State;
				rc = pager_end_transaction(pager, 0, 0);
				if (!pager->MemoryDB && state > PAGER::WRITER_LOCKED)
				{
					// This can happen using journal_mode=off. Move the pager to the error state to indicate that the contents of the cache may not be trusted. Any active readers will get SQLITE_ABORT.
					pager->ErrorCode = RC::ABORT;
					pager->State = PAGER::ERROR;
					return rc;
				}
			}
			else
				rc = pager_playback(pPager, 0);

			_assert(pager->State == PAGER::READER || rc != RC::OK);
			_assert(rc == RC::OK || rc == RC::FULL || rc == RC::NOMEM || (rc & 0xFF) == RC::IOERR);

			// If an error occurs during a ROLLBACK, we can no longer trust the pager cache. So call pager_error() on the way out to make any error persistent.
			return pager_error(pPager, rc);
		}

		uint8 sqlite3PagerIsreadonly(Pager *pager)
		{
			return pager->ReadOnly;
		}

		int sqlite3PagerRefcount(Pager *pager)
		{
			return PCache_RefCount(pager->PCache);
		}

		int sqlite3PagerMemUsed(Pager *pager)
		{
			int perPageSize = pager->PageSize + pager->Extras + sizeof(PgHdr) + 5 * sizeof(void *);
			return perPageSize * PCache_Pagecount(pager->PCache) + SysEx::AllocSize(pager) + pager->PageSize;
		}

		int sqlite3PagerPageRefcount(DbPage *page)
		{
			return sqlite3PcachePageRefcount(page);
		}

#ifdef TEST
		int *sqlite3PagerStats(Pager *pager)
		{
			static int a[11];
			a[0] = PCache_RefCount(pager->PCache);
			a[1] = PCache_Pagecount(pager->PCache);
			a[2] = PCache_GetCachesize(pager->PCache);
			a[3] = pager->eState == (PAGER::OPEN ? -1 : (int)pager->DBSize);
			a[4] = pager->eState;
			a[5] = pager->errCode;
			a[6] = pager->aStat[PAGER_STAT_HIT];
			a[7] = pager->aStat[PAGER_STAT_MISS];
			a[8] = 0;  // Used to be pPager->nOvfl
			a[9] = pager->Reads;
			a[10] = pager->Stats[PAGER_STAT_WRITE];
			return a;
		}
#endif

		void sqlite3PagerCacheStat(Pager *pager, DBSTATUS dbStatus, bool reset, int *valueOut)
		{
			_assert(dbStatus == DBSTATUS::CACHE_HIT ||
				dbStatus == DBSTATUS::CACHE_MISS ||
				dbStatus == DBSTATUS::CACHE_WRITE);
			_assert(DBSTATUS::CACHE_HIT + 1 == DBSTATUS::CACHE_MISS);
			_assert(DBSTATUS::CACHE_HIT + 2 == DBSTATUS::CACHE_WRITE);
			_assert(PAGER_STAT_HIT == 0 &&
				PAGER_STAT_MISS == 1 &&
				PAGER_STAT_WRITE == 2);

			*valueOut += pager->Stats[dbStatus - DBSTATUS::CACHE_HIT];
			if (reset)
				pager->Stats[dbStatus - DBSTATUS::CACHE_HIT] = 0;
		}

		int sqlite3PagerIsMemdb(Pager *pager)
		{
			return pager->MemoryDB;
		}

		int sqlite3PagerOpenSavepoint(Pager *pager, int savepoints)
		{
			_assert(pager->State >= PAGER::WRITER_LOCKED);
			_assert(assert_pager_state(pager));

			RC rc = RC::OK;
			int currentSavepoints = __arrayLength(pager->Savepoints); // Current number of savepoints
			if (savepoints > currentSavepoints && pager->UseJournal)
			{
				// Grow the Pager.aSavepoint array using realloc(). Return SQLITE_NOMEM if the allocation fails. Otherwise, zero the new portion in case a 
				// malloc failure occurs while populating it in the for(...) loop below.
				PagerSavepoint *newSavepoints = (PagerSavepoint *)SysEx::Realloc(pager->Savepoints, sizeof(PagerSavepoint) * savepoints); // New Pager.Savepoints array
				if (!newSavepoints)
					return RC::NOMEM;
				_memset(&newSavepoints[currentSavepoints], 0, (savepoints - currentSavepoints) * sizeof(PagerSavepoint));
				pager->Savepoints = newSavepoints;

				// Populate the PagerSavepoint structures just allocated.
				for (int ii = currentSavepoints; ii < savepoints; ii++)
				{
					newSavepoints[ii].Orig = pager->DBSize;
					newSavepoints[ii].Offset = (pager->JournalFile->Opened && pager->JournalOffset > 0 ? pager->JournalOffset : JOURNAL_HDR_SZ(pager));
					newSavepoints[ii].SubRecords = pager->SubRecords;
					newSavepoints[ii].InSavepoint = Bitvec::Create(pager->DBSize);
					if (!newSavepoints[ii].InSavepoint)
						return RC::NOMEM;
					if (UseWal(pager))
						sqlite3WalSavepoint(pager->Wal, newSavepoints[ii].WalData);
					__arraySetLength(pager->Savepoints, ii + 1);
				}
				_assert(__arrayLength(pager->Savepoints) == savepoints);
				assertTruncateConstraint(pager);
			}
			return rc;
		}

		int sqlite3PagerSavepoint(Pager *pager, int op, int savepoints)
		{
			_assert(op == SAVEPOINT::RELEASE || op == SAVEPOINT::ROLLBACK);
			_assert(savepoints >= 0 || op == SAVEPOINT::ROLLBACK);
			RC rc = pager->ErrorCode;
			if (rc == RC::OK && savepoints < __arrayLength(pager->Savepoints))
			{
				// Figure out how many savepoints will still be active after this operation. Store this value in nNew. Then free resources associated 
				// with any savepoints that are destroyed by this operation.
				int newLength = savepoints + (op == SAVEPOINT::RELEASE ? 0 : 1); // Number of remaining savepoints after this op.
				for (int ii = newLength; ii < __arrayLength(pager->Savepoints); ii++)
					Bitvec::Destroy(pager->Savepoints[ii].InSavepoint);
				__arraySetLength(pager->Savepoints, newLength);

				// If this is a release of the outermost savepoint, truncate the sub-journal to zero bytes in size.
				if (op == SAVEPOINT::RELEASE)
				{
					if (newLength == 0 && pager->SubJournalFile->Opened)
					{
						// Only truncate if it is an in-memory sub-journal.
						if (pager->SubJournalFile->IsMemJournal)
						{
							rc = pager->SubJournalFile->Truncate(0);
							_assert(rc == RC::OK);
						}
						pager->SubRecords = 0;
					}
				}
				// Else this is a rollback operation, playback the specified savepoint. If this is a temp-file, it is possible that the journal file has
				// not yet been opened. In this case there have been no changes to the database file, so the playback operation can be skipped.
				else if (UseWal(pager) || pager->JournalFile->Opened)
				{
					PagerSavepoint *savepoint = (newLength == 0 ? nullptr : &pager->Savepoints[newLength - 1]);
					rc = pagerPlaybackSavepoint(pager, savepoint);
					_assert(rc != RC::DONE);
				}
			}
			return rc;
		}

		const char *sqlite3PagerFilename(Pager *pager, int nullIfMemDb)
		{
			return (nullIfMemDb && pager->MemoryDB ? "" : pager->Filename);
		}

		const VFileSystem *sqlite3PagerVfs(Pager *pager)
		{
			return pager->Vfs;
		}

		VFile *sqlite3PagerFile(Pager *pager)
		{
			return pager->File;
		}

		const char *sqlite3PagerJournalname(Pager *pager)
		{
			return pager->Journal;
		}

		int sqlite3PagerNosync(Pager *pager)
		{
			return pager->NoSync;
		}

#ifndef HAS_CODEC
		void sqlite3PagerSetCodec(Pager *pager, void *(*codec)(void *,void *, Pid, int), void (*codecSizeChange)(void *, int, int), void (*codecFree)(void *), void *codecArg)
		{
			if (pager->CodecFree) pager->CodecFree(pager->Codec);
			pager->Codec = (pager->MemoryDB ? nullptr : codec);
			pager->CodecSizeChng = codecSizeChange;
			pager->CodecFree = codecFree;
			pager->CodecArg = codecArg;
			pagerReportSize(pager);
		}
		void *sqlite3PagerGetCodec(Pager *pager)
		{
			return pager->Codec;
		}
#endif

#ifndef OMIT_AUTOVACUUM
		int sqlite3PagerMovepage(Pager *pager, DbPage *pg, Pid id, int isCommit)
		{
			_assert(pg->Refs > 0);
			_assert(pager->State == PAGER::WRITER_CACHEMOD ||
				pager->State==PAGER::WRITER_DBMOD);
			_assert(assert_pager_state(pager));

			// In order to be able to rollback, an in-memory database must journal the page we are moving from.
			RC rc;
			if (pager->MemoryDB)
			{
				rc = sqlite3PagerWrite(pPg);
				if (rc) return rc;
			}

			// If the page being moved is dirty and has not been saved by the latest savepoint, then save the current contents of the page into the 
			// sub-journal now. This is required to handle the following scenario:
			//
			//   BEGIN;
			//     <journal page X, then modify it in memory>
			//     SAVEPOINT one;
			//       <Move page X to location Y>
			//     ROLLBACK TO one;
			//
			// If page X were not written to the sub-journal here, it would not be possible to restore its contents when the "ROLLBACK TO one"
			// statement were is processed.
			//
			// subjournalPage() may need to allocate space to store pPg->pgno into one or more savepoint bitvecs. This is the reason this function
			// may return SQLITE_NOMEM.
			if (pg->Flags & PgHdr::PGHDR_DIRTY
				&& subjRequiresPage(pg)
				&& (rc = subjournalPage(pg) != RC::OK))
				return rc;

			SysEx_PAGERTRACE(("MOVE %d page %d (needSync=%d) moves to %d\n", PAGERID(pager), pg->ID, (pg->Flags & PgHdr::PGHDR::NEED_SYNC) ? 1 : 0, id));
			SysEx_IOTRACE(("MOVE %p %d %d\n", pager, pg->ID, id));

			// If the journal needs to be sync()ed before page pPg->pgno can be written to, store pPg->pgno in local variable needSyncPgno.
			//
			// If the isCommit flag is set, there is no need to remember that the journal needs to be sync()ed before database page pPg->pgno 
			// can be written to. The caller has already promised not to write to it.
			Pid needSyncID = 0; // Old value of pPg->pgno, if sync is required
			if ((pg->Flags & PgHdr::PGHDR::NEED_SYNC) && !isCommit)
			{
				needSyncID = pg->ID;
				_assert(pager->JournalMode == PAGER::JOURNALMODE_OFF || pageInJournal(pg) || pg->ID > pager->DBOrigSize);
				_assert(pg->Flags & PgHdr::PGHDR::DIRTY);
			}

			// If the cache contains a page with page-number pgno, remove it from its hash chain. Also, if the PGHDR_NEED_SYNC flag was set for 
			// page pgno before the 'move' operation, it needs to be retained for the page moved there.
			pg->Flags &= ~PgHdr::PGHDR::NEED_SYNC;
			PgHdr *pgOld = pager_lookup(pager, id); // The page being overwritten.
			_assert(!pgOld || pgOld->Refs == 1);
			if (pgOld)
			{
				pg->Flags |= (pgOld->Flags & PgHdr::PGHDR::NEED_SYNC);
				if (pager->MemoryDB)
				{
					// Do not discard pages from an in-memory database since we might need to rollback later.  Just move the page out of the way.
					sqlite3PcacheMove(pgOld, pager->DBSize + 1);
				}
				else
					sqlite3PcacheDrop(pPgOld);
			}

			Pid origID = pg->ID; // The original page number
			PCache_Move(pg, id);
			PCache_MakeDirty(pg);

			// For an in-memory database, make sure the original page continues to exist, in case the transaction needs to roll back.  Use pPgOld
			// as the original page since it has already been allocated.
			if (pager->MemoryDB)
			{
				assert(pgOld);
				PCache_Move(pgOld, origID);
				sqlite3PagerUnref(pgOld);
			}

			if (needSyncPgno)
			{
				// If needSyncPgno is non-zero, then the journal file needs to be sync()ed before any data is written to database file page needSyncPgno.
				// Currently, no such page exists in the page-cache and the "is journaled" bitvec flag has been set. This needs to be remedied by
				// loading the page into the pager-cache and setting the PGHDR_NEED_SYNC flag.
				//
				// If the attempt to load the page into the page-cache fails, (due to a malloc() or IO failure), clear the bit in the pInJournal[]
				// array. Otherwise, if the page is loaded and written again in this transaction, it may be written to the database file before
				// it is synced into the journal file. This way, it may end up in the journal file twice, but that is not a problem.
				PgHdr *pgHdr;
				rc = sqlite3PagerGet(pager, needSyncID, &pgHdr);
				if (rc != RC::OK)
				{
					if (needSyncID <= pager->DBOrigSize)
					{
						_assert(pager->TmpSpace != nullptr);
						pager->InJournal->Clear(needSyncID, pager->TmpSpace);
					}
					return rc;
				}
				pgHdr->Flags |= PgHdr::PGHDR::NEED_SYNC;
				PCache_MakeDirty(pgHdr);
				sqlite3PagerUnref(pgHdr);
			}
			return RC::OK;
		}
#endif

		void *sqlite3PagerGetData(DbPage *pg)
		{
			_assert(pg->Ref > 0 || pg->Pager->MemoryDB);
			return pg->Data;
		}

		void *sqlite3PagerGetExtra(DbPage *pg)
		{
			return pg->Extra;
		}

		int sqlite3PagerLockingMode(Pager *pager, int mode)
		{
			_assert(mode == PAGER::LOCKINGMODE_QUERY ||
				mode == PAGER::LOCKINGMODE_NORMAL ||
				mode == PAGER::LOCKINGMODE_EXCLUSIVE);
			_assert(PAGER::LOCKINGMODE_QUERY < 0);
			_assert(PAGER::LOCKINGMODE_NORMAL >= 0 && PAGER::LOCKINGMODE_EXCLUSIVE >= 0);
			_assert(pager->ExclusiveMode || sqlite3WalHeapMemory(pager->Wal) == 0);
			if (mode >= 0 && !pager->TempFile && !sqlite3WalHeapMemory(pager->Wal))
				pager->ExclusiveMode = (uint8)mode;
			return (int)pager->ExclusiveMode;
		}

		int sqlite3PagerSetJournalMode(Pager *pPager, int mode)
		{
			uint8 old = pager->JournalMode; // Prior journalmode

#ifdef _DEBUG
			// The print_pager_state() routine is intended to be used by the debugger only.  We invoke it once here to suppress a compiler warning.
			print_pager_state(pager);
#endif

			// The mode parameter is always valid
			_assert(mode == IPager::JOURNALMODE::DELETE ||
				mode == IPager::JOURNALMODE::TRUNCATE ||
				mode == IPager::JOURNALMODE::PERSIST ||
				mode == IPager::JOURNALMODE::OFF ||
				mode == IPager::JOURNALMODE::WAL ||
				mode == IPager::JOURNALMODE::JMEMORY);

			// This routine is only called from the OP_JournalMode opcode, and the logic there will never allow a temporary file to be changed to WAL mode.
			_assert(pager->TempFile == nullptr || mode != IPager::JOURNALMODE::WAL);

			// Do allow the journalmode of an in-memory database to be set to anything other than MEMORY or OFF
			if (pager->MemoryDB)
			{
				_assert(old == IPager::JOURNALMODE::JMEMORY || old == IPager::JOURNALMODE::OFF);
				if (mode != IPager::JOURNALMODE::JMEMORY && mode != IPager::JOURNALMODE::OFF)
					mode = old;
			}

			if (mode != old)
			{
				// Change the journal mode
				_assert(pager->State != PAGER::ERROR);
				pager->JournalMode = (uint8)mode;

				// When transistioning from TRUNCATE or PERSIST to any other journal mode except WAL, unless the pager is in locking_mode=exclusive mode,
				// delete the journal file.
				_assert((IPager::JOURNALMODE::TRUNCATE & 5) == 1);
				_assert((IPager::JOURNALMODE::PERSIST & 5) == 1);
				_assert((IPager::JOURNALMODE::DELETE & 5) == 0);
				_assert((IPager::JOURNALMODE::MEMORY & 5) == 4);
				_assert((IPager::JOURNALMODE::OFF & 5) == 0);
				_assert((IPager::JOURNALMODE::WAL & 5) == 5);

				_assert(pager->File->Opened || pager->ExclusiveMode);
				if (!pager->ExclusiveMode && (eOld & 5) == 1 && (eMode & 1) == 0)
				{
					// In this case we would like to delete the journal file. If it is not possible, then that is not a problem. Deleting the journal file
					// here is an optimization only.
					//
					// Before deleting the journal file, obtain a RESERVED lock on the database file. This ensures that the journal file is not deleted
					// while it is in use by some other client.
					pPager->JournalFile->Close();
					if (pager->Lock >= VFile::LOCK::RESERVED)
						pager->Vfs->Delete(pager->Journal, 0);
					else
					{
						RCt rc = RC::OK;
						PAGER state = pager->State;
						_assert(state == PAGER::OPEN || state == PAGER::READER);
						if (state == PAGER::OPEN)
							rc = sqlite3PagerSharedLock(pager);
						if (pager->State == PAGER::READER)
						{
							_assert(rc == RC::OK);
							rc = pagerLockDb(pager, VFile::LOCK::RESERVED);
						}
						if (rc == RC::OK)
							pager->Vfs->Delete(pager->Journal, 0);
						if (rc == RC::OK && state == PAGER::READER)
							pagerUnlockDb(pager, VFile::LOCK::SHARED);
						else if (state == PAGER_OPEN)
							pager_unlock(pager);
						assert(state == pager->State);
					}
				}
			}

			// Return the new journal mode
			return (int)pager->JournalMode;
		}

		int sqlite3PagerGetJournalMode(Pager *pager)
		{
			return (int)pager->JournalMode;
		}

		int sqlite3PagerOkToChangeJournalMode(Pager *pager)
		{
			_assert(assert_pager_state(pager));
			if (pager->State >= PAGER::WRITER_CACHEMOD) return 0;
			return (SysEx::NEVER(pager->JournalFile->Opened && pager->JournalOffset > 0) ? 0 : 1);
		}

		int64 sqlite3PagerJournalSizeLimit(Pager *pager, int64 limit)
		{
			if (limit >= -1)
			{
				pager->JournalSizeLimit = limit;
				sqlite3WalLimit(pager->Wal, limit);
			}
			return pager->JournalSizeLimit;
		}

		IBackup **sqlite3PagerBackupPtr(Pager *pager)
		{
			return &pager->Backup;
		}

#ifndef OMIT_VACUUM
		void sqlite3PagerClearCache(Pager *pager)
		{
			if (!pager->MemoryDB && pager->TempFile == nullptr)
				pager_reset(pPager);
		}
#endif

#pragma endregion
#endif

	};
}