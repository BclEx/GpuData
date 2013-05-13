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

		typedef struct PagerSavepoint PagerSavepoint;
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
		VFile::SYNC CkptSyncFlags;	// SYNC_NORMAL or SYNC_FULL for checkpoint
		VFile::SYNC WalSyncFlags;	// SYNC_NORMAL or SYNC_FULL for wal writes
		VFile::SYNC SyncFlags;		// SYNC_NORMAL or SYNC_FULL otherwise
		bool TempFile;				// zFilename is a temporary file
		bool ReadOnly;				// True for a read-only database
		bool MemoryDB;				// True to inhibit all file I/O
		// The following block contains those class members that change during routine opertion.  Class members not in this block are either fixed
		// when the pager is first created or else only change when there is a significant mode change (such as changing the page_size, locking_mode,
		// or the journal_mode).  From another view, these class members describe the "state" of the pager, while other class members describe the "configuration" of the pager.
		PAGER State;                // Pager state (OPEN, READER, WRITER_LOCKED..) */
		VFile::LOCK Lock;           // Current lock held on database file */
		bool changeCountDone;       // Set after incrementing the change-counter */
		bool SetMaster;             // True if a m-j name has been written to jrnl */
		byte DoNotSpill;            // Do not spill the cache when non-zero */
		byte DoNotSyncSpill;        // Do not do a spill that requires jrnl sync */
		byte SubjInMemory;          // True to use in-memory sub-journals */
		Pid DBSize;					// Number of pages in the database */
		Pid DBOrigSize;				// dbSize before the current transaction */
		Pid DBFileSize;				// Number of pages in the database file */
		Pid DBHintSize;				// Value passed to FCNTL_SIZE_HINT call */
		RC ErrorCode;               // One of several kinds of errors */
		int _nRec;                  // Pages journalled since last j-header written */
		uint32 CksumInit;           // Quasi-random value added to every checksum */
		uint32 SubRecords;          // Number of records written to sub-journal */
		Bitvec *InJournal;			// One bit for each page in the database file */
		VFile *File;				// File descriptor for database */
		VFile *JournalFile;			// File descriptor for main journal */
		VFile *SubJournalFile;		// File descriptor for sub-journal */
		int64 JournalOff;           // Current write offset in the journal file */
		int64 JournalHdr;           // Byte offset to previous journal header */
		IBackup *Backup;			// Pointer to list of ongoing backup processes */
		PagerSavepoint *Savepoint;	// Array of active savepoints */
		int nSavepoint;             // Number of elements in aSavepoint[] */
		char DBFileVersions[16];    // Changes whenever database file changes */
		// End of the routinely-changing class members
		u16 nExtra;                 // Add this many bytes to each in-memory page
		i16 nReserve;               // Number of unused bytes at end of each page
		u32 vfsFlags;               // Flags for sqlite3_vfs.xOpen()
		u32 sectorSize;             // Assumed sector size during rollback
		int pageSize;               // Number of bytes in a page
		Pgno mxPgno;                // Maximum allowed size of the database
		i64 journalSizeLimit;       // Size limit for persistent journal files
		char *zFilename;            // Name of the database file
		char *zJournal;             // Name of the journal file
		int (*xBusyHandler)(void*); // Function to call when busy
		void *pBusyHandlerArg;      // Context argument for xBusyHandler
		int aStat[3];               // Total cache hits, misses and writes
#ifdef TEST
		int nRead;                  // Database pages read
#endif
		void (*Reiniter)(IPage *);	// Call this routine when reloading pages
#ifdef HAS_CODEC
		void *(*Codec)(void*,void*,Pgno,int); // Routine for en/decoding data
		void (*CodecSizeChng)(void*,int,int); // Notify of page size changes
		void (*CodecFree)(void*);             // Destructor for the codec
		void *CodecArg;             // First argument to xCodec... methods
#endif
		char *TmpSpace;				// Pager.pageSize bytes of space for tmp use
		PCache *PCache;				// Pointer to page cache object
#ifndef OMIT_WAL
		Wal *Wal;					// Write-ahead log used by "journal_mode=wal"
		char *WalName;              // File name for write-ahead log
#endif
	};
}