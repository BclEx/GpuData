// pager.c
#include "Core+Pager.cu.h"
using namespace Core;

namespace Core
{
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
		PagerSavepoint *Savepoint;	// Array of active savepoints
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
		char *TmpSpace;				// Pager.pageSize bytes of space for tmp use
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

#ifndef NDEBUG 

		static int assert_pager_state(Pager *p){
			Pager *pPager = p;

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
			_assert( p->TempFile == 0 || p->Lock == VFile::LOCK::EXCLUSIVE);
			_assert( p->TempFile == 0 || pager->ChangeCountDone);

			// If the useJournal flag is clear, the journal-mode must be "OFF". And if the journal-mode is "OFF", the journal file must not be open.
			_assert( p->journalMode==PAGER_JOURNALMODE_OFF || p->useJournal );
			_assert( p->journalMode!=PAGER_JOURNALMODE_OFF || !isOpen(p->jfd) );

			/* Check that MEMDB implies noSync. And an in-memory journal. Since 
			** this means an in-memory pager performs no IO at all, it cannot encounter 
			** either SQLITE_IOERR or SQLITE_FULL during rollback or while finalizing 
			** a journal file. (although the in-memory journal implementation may 
			** return SQLITE_IOERR_NOMEM while the journal file is being written). It 
			** is therefore not possible for an in-memory pager to enter the ERROR 
			** state.
			*/
			if( MEMDB ){
				assert( p->noSync );
				assert( p->journalMode==PAGER_JOURNALMODE_OFF 
					|| p->journalMode==PAGER_JOURNALMODE_MEMORY 
					);
				assert( p->eState!=PAGER_ERROR && p->eState!=PAGER_OPEN );
				assert( pagerUseWal(p)==0 );
			}

			/* If changeCountDone is set, a RESERVED lock or greater must be held
			** on the file.
			*/
			assert( pPager->changeCountDone==0 || pPager->eLock>=RESERVED_LOCK );
			assert( p->eLock!=PENDING_LOCK );

			switch( p->eState ){
			case PAGER_OPEN:
				assert( !MEMDB );
				assert( pPager->errCode==SQLITE_OK );
				assert( sqlite3PcacheRefCount(pPager->pPCache)==0 || pPager->tempFile );
				break;

			case PAGER_READER:
				assert( pPager->errCode==SQLITE_OK );
				assert( p->eLock!=UNKNOWN_LOCK );
				assert( p->eLock>=SHARED_LOCK );
				break;

			case PAGER_WRITER_LOCKED:
				assert( p->eLock!=UNKNOWN_LOCK );
				assert( pPager->errCode==SQLITE_OK );
				if( !pagerUseWal(pPager) ){
					assert( p->eLock>=RESERVED_LOCK );
				}
				assert( pPager->dbSize==pPager->dbOrigSize );
				assert( pPager->dbOrigSize==pPager->dbFileSize );
				assert( pPager->dbOrigSize==pPager->dbHintSize );
				assert( pPager->setMaster==0 );
				break;

			case PAGER_WRITER_CACHEMOD:
				assert( p->eLock!=UNKNOWN_LOCK );
				assert( pPager->errCode==SQLITE_OK );
				if( !pagerUseWal(pPager) ){
					/* It is possible that if journal_mode=wal here that neither the
					** journal file nor the WAL file are open. This happens during
					** a rollback transaction that switches from journal_mode=off
					** to journal_mode=wal.
					*/
					assert( p->eLock>=RESERVED_LOCK );
					assert( isOpen(p->jfd) 
						|| p->journalMode==PAGER_JOURNALMODE_OFF 
						|| p->journalMode==PAGER_JOURNALMODE_WAL 
						);
				}
				assert( pPager->dbOrigSize==pPager->dbFileSize );
				assert( pPager->dbOrigSize==pPager->dbHintSize );
				break;

			case PAGER_WRITER_DBMOD:
				assert( p->eLock==EXCLUSIVE_LOCK );
				assert( pPager->errCode==SQLITE_OK );
				assert( !pagerUseWal(pPager) );
				assert( p->eLock>=EXCLUSIVE_LOCK );
				assert( isOpen(p->jfd) 
					|| p->journalMode==PAGER_JOURNALMODE_OFF 
					|| p->journalMode==PAGER_JOURNALMODE_WAL 
					);
				assert( pPager->dbOrigSize<=pPager->dbHintSize );
				break;

			case PAGER_WRITER_FINISHED:
				assert( p->eLock==EXCLUSIVE_LOCK );
				assert( pPager->errCode==SQLITE_OK );
				assert( !pagerUseWal(pPager) );
				assert( isOpen(p->jfd) 
					|| p->journalMode==PAGER_JOURNALMODE_OFF 
					|| p->journalMode==PAGER_JOURNALMODE_WAL 
					);
				break;

			case PAGER_ERROR:
				/* There must be at least one outstanding reference to the pager if
				** in ERROR state. Otherwise the pager should have already dropped
				** back to OPEN state.
				*/
				assert( pPager->errCode!=SQLITE_OK );
				assert( sqlite3PcacheRefCount(pPager->pPCache)>0 );
				break;
			}

			return 1;
		}
#endif /* ifndef NDEBUG */

#pragma endregion


#pragma region X



#pragma endregion



	};
}