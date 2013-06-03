// pager.h
namespace Core
{
	// sqliteLimit.h
#define MAX_PAGE_SIZE 65536

	typedef struct PagerSavepoint PagerSavepoint;
	typedef struct PgHdr IPage;

	class IPager
	{
	public:
		// NOTE: These values must match the corresponding BTREE_ values in btree.h.
		enum PAGEROPEN : char
		{
			OMIT_JOURNAL = 0x0001,	// Do not use a rollback journal
			MEMORY = 0x0002,		// In-memory database
		};

		enum LOCKINGMODE : char
		{
			QUERY = -1,
			NORMAL = 0,
			EXCLUSIVE = 1,
		};

		enum JOURNALMODE : char
		{
			JQUERY = -1,     // Query the value of journalmode
			DELETE = 0,     // Commit by deleting journal file
			PERSIST = 1,    // Commit by zeroing journal header
			OFF = 2,        // Journal omitted.
			TRUNCATE = 3,   // Commit by truncating journal
			JMEMORY = 4,     // In-memory journal file
			WAL = 5,        // Use write-ahead logging
		};

		// sqlite3.h
		enum CHECKPOINT : byte
		{
			PASSIVE = 0,
			FULL = 1,
			RESTART = 2,
		};
	};

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

		VFileSystem *Vfs;			// OS functions to use for IO
		bool ExclusiveMode;			// Boolean. True if locking_mode==EXCLUSIVE
		uint8 JournalMode;			// One of the PAGER_JOURNALMODE_* values
		bool UseJournal;			// Use a rollback journal on this file
		bool NoSync;				// Do not sync the journal if true
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
		bool DoNotSpill;            // Do not spill the cache when non-zero
		bool DoNotSyncSpill;        // Do not do a spill that requires jrnl sync
		bool SubjInMemory;          // True to use in-memory sub-journals
		Pid DBSize;					// Number of pages in the database
		Pid DBOrigSize;				// dbSize before the current transaction
		Pid DBFileSize;				// Number of pages in the database file
		Pid DBHintSize;				// Value passed to FCNTL_SIZE_HINT call
		RC ErrorCode;               // One of several kinds of errors
		int Records;                // Pages journalled since last j-header written
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
		char DBFileVersion[16];		// Changes whenever database file changes
		// End of the routinely-changing class members
		uint16 ExtraBytes;          // Add this many bytes to each in-memory page
		int16 ReserveBytes;         // Number of unused bytes at end of each page
		VFileSystem::OPEN VfsFlags; // Flags for sqlite3_vfs.xOpen()
		uint32 SectorSize;          // Assumed sector size during rollback
		int PageSize;               // Number of bytes in a page
		Pid MaxPid;					// Maximum allowed size of the database
		int64 JournalSizeLimit;     // Size limit for persistent journal files
		char *Filename;				// Name of the database file
		char *Journal;				// Name of the journal file
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
		void *TmpSpace;				// Pager.pageSize bytes of space for tmp use
		PCache *PCache;				// Pointer to page cache object
#ifndef OMIT_WAL
		Wal *Wal;					// Write-ahead log used by "journal_mode=wal"
		char *WalName;              // File name for write-ahead log
#endif
		// Open and close a Pager connection. 
		static RC Open(VFileSystem *vfs, Pager **pagerOut, const char *filename, int extraBytes, IPager::PAGEROPEN flags, VFileSystem::OPEN vfsFlags, void (*reinit)(IPage *));
		static RC Close(Pager *pager);
		RC ReadFileheader(int n, unsigned char *dest);
		// Functions used to configure a Pager object.
		void SetBusyhandler(int (*busyHandler)(void *), void *busyHandlerArg);
		RC SetPageSize(uint32 *pageSizeRef, int reserveBytes);
		int MaxPages(int maxPages);
		void SetCacheSize(int maxPages);
		void Shrink();
		void SetSafetyLevel(int level, bool fullFsync, bool checkpointFullFsync);
		int LockingMode(IPager::LOCKINGMODE mode);
		IPager::JOURNALMODE SetJournalMode(IPager::JOURNALMODE mode);
		IPager::JOURNALMODE Pager::GetJournalMode();
		bool OkToChangeJournalMode();
		int64 JournalSizeLimit(int64 limit);
		IBackup **BackupPtr();
		// Functions used to obtain and release page references.
		//#define Acquire(A,B,C) Acquire(A,B,C,false)
		RC Acquire(Pid id, IPage **pageOut, bool noContent);
		IPage *Lookup(Pid id);
		static void Ref(IPage *pg);
		static void Unref(IPage *pg);
		// Operations on page references.
		static RC Write(IPage *page);
		static void DontWrite(PgHdr *pg);

	};

	//		int sqlite3PagerMovepage(Pager*,DbPage*,Pgno,int);
	//		int sqlite3PagerPageRefcount(DbPage*);
	//		void *sqlite3PagerGetData(DbPage *); 
	//		void *sqlite3PagerGetExtra(DbPage *); 
	//
	//		/* Functions used to manage pager transactions and savepoints. */
	//		void sqlite3PagerPagecount(Pager*, int*);
	//		int sqlite3PagerBegin(Pager*, int exFlag, int);
	//		int sqlite3PagerCommitPhaseOne(Pager*,const char *zMaster, int);
	//		int sqlite3PagerExclusiveLock(Pager*);
	//		int sqlite3PagerSync(Pager *pPager);
	//		int sqlite3PagerCommitPhaseTwo(Pager*);
	//		int sqlite3PagerRollback(Pager*);
	//		int sqlite3PagerOpenSavepoint(Pager *pPager, int n);
	//		int sqlite3PagerSavepoint(Pager *pPager, int op, int iSavepoint);
	//		int sqlite3PagerSharedLock(Pager *pPager);
	//
	//#ifndef SQLITE_OMIT_WAL
	//		int sqlite3PagerCheckpoint(Pager *pPager, int, int*, int*);
	//		int sqlite3PagerWalSupported(Pager *pPager);
	//		int sqlite3PagerWalCallback(Pager *pPager);
	//		int sqlite3PagerOpenWal(Pager *pPager, int *pisOpen);
	//		int sqlite3PagerCloseWal(Pager *pPager);
	//#endif
	//
	//#ifdef SQLITE_ENABLE_ZIPVFS
	//		int sqlite3PagerWalFramesize(Pager *pPager);
	//#endif
	//
	//		/* Functions used to query pager state and configuration. */
	//		u8 sqlite3PagerIsreadonly(Pager*);
	//		int sqlite3PagerRefcount(Pager*);
	//		int sqlite3PagerMemUsed(Pager*);
	//		const char *sqlite3PagerFilename(Pager*, int);
	//		const sqlite3_vfs *sqlite3PagerVfs(Pager*);
	//		sqlite3_file *sqlite3PagerFile(Pager*);
	//		const char *sqlite3PagerJournalname(Pager*);
	//		int sqlite3PagerNosync(Pager*);
	//		void *sqlite3PagerTempSpace(Pager*);
	//		int sqlite3PagerIsMemdb(Pager*);
	//		void sqlite3PagerCacheStat(Pager *, int, int, int *);
	//		void sqlite3PagerClearCache(Pager *);
	//		int sqlite3SectorSize(sqlite3_file *);
	//
	//		/* Functions used to truncate the database file. */
	//		void sqlite3PagerTruncateImage(Pager*,Pgno);
	//
	//#if defined(SQLITE_HAS_CODEC) && !defined(SQLITE_OMIT_WAL)
	//		void *sqlite3PagerCodec(DbPage *);
	//#endif
	//
	//		/* Functions to support testing and debugging. */
	//#if !defined(NDEBUG) || defined(SQLITE_TEST)
	//		Pgno sqlite3PagerPagenumber(DbPage*);
	//		int sqlite3PagerIswriteable(DbPage*);
	//#endif
	//#ifdef SQLITE_TEST
	//		int *sqlite3PagerStats(Pager*);
	//		void sqlite3PagerRefdump(Pager*);
	//		void disable_simulated_io_errors(void);
	//		void enable_simulated_io_errors(void);
	//#else
	//# define disable_simulated_io_errors()
	//# define enable_simulated_io_errors()
	//#endif
}