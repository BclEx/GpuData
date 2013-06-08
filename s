[1mdiff --git a/src/GpuData/Core+Pager/Pager.cu.h b/src/GpuData/Core+Pager/Pager.cu.h[m
[1mindex b4adc92..9c124e4 100644[m
[1m--- a/src/GpuData/Core+Pager/Pager.cu.h[m
[1m+++ b/src/GpuData/Core+Pager/Pager.cu.h[m
[36m@@ -5,7 +5,7 @@[m [mnamespace Core[m
 #define MAX_PAGE_SIZE 65536[m
 [m
 	typedef struct PagerSavepoint PagerSavepoint;[m
[31m-	typedef struct PgHdr IPage;[m
[32m+[m	[32mtypedef struct PgHdr IPage;[m[41m[m
 [m
 	class IPager[m
 	{[m
[36m@@ -131,94 +131,94 @@[m [mnamespace Core[m
 		static RC Open(VFileSystem *vfs, Pager **pagerOut, const char *filename, int extraBytes, IPager::PAGEROPEN flags, VFileSystem::OPEN vfsFlags, void (*reinit)(IPage *));[m
 		static RC Close(Pager *pager);[m
 		RC ReadFileheader(int n, unsigned char *dest);[m
[31m-		// Functions used to configure a Pager object.[m
[31m-		void SetBusyhandler(int (*busyHandler)(void *), void *busyHandlerArg);[m
[31m-		RC SetPageSize(uint32 *pageSizeRef, int reserveBytes);[m
[31m-		int MaxPages(int maxPages);[m
[31m-		void SetCacheSize(int maxPages);[m
[31m-		void Shrink();[m
[31m-		void SetSafetyLevel(int level, bool fullFsync, bool checkpointFullFsync);[m
[31m-		int LockingMode(IPager::LOCKINGMODE mode);[m
[31m-		IPager::JOURNALMODE SetJournalMode(IPager::JOURNALMODE mode);[m
[31m-		IPager::JOURNALMODE Pager::GetJournalMode();[m
[31m-		bool OkToChangeJournalMode();[m
[31m-		int64 JournalSizeLimit(int64 limit);[m
[31m-		IBackup **BackupPtr();[m
[31m-		// Functions used to obtain and release page references.[m
[31m-		//#define Acquire(A,B,C) Acquire(A,B,C,false)[m
[31m-		RC Acquire(Pid id, IPage **pageOut, bool noContent);[m
[31m-		IPage *Lookup(Pid id);[m
[31m-		static void Ref(IPage *pg);[m
[31m-		static void Unref(IPage *pg);[m
[31m-		// Operations on page references.[m
[31m-		static RC Write(IPage *page);[m
[32m+[m		[32m// Functions used to configure a Pager object.[m[41m[m
[32m+[m		[32mvoid SetBusyhandler(int (*busyHandler)(void *), void *busyHandlerArg);[m[41m[m
[32m+[m		[32mRC SetPageSize(uint32 *pageSizeRef, int reserveBytes);[m[41m[m
[32m+[m		[32mint MaxPages(int maxPages);[m[41m[m
[32m+[m		[32mvoid SetCacheSize(int maxPages);[m[41m[m
[32m+[m		[32mvoid Shrink();[m[41m[m
[32m+[m		[32mvoid SetSafetyLevel(int level, bool fullFsync, bool checkpointFullFsync);[m[41m[m
[32m+[m		[32mint LockingMode(IPager::LOCKINGMODE mode);[m[41m[m
[32m+[m		[32mIPager::JOURNALMODE SetJournalMode(IPager::JOURNALMODE mode);[m[41m[m
[32m+[m		[32mIPager::JOURNALMODE Pager::GetJournalMode();[m[41m[m
[32m+[m		[32mbool OkToChangeJournalMode();[m[41m[m
[32m+[m		[32mint64 JournalSizeLimit(int64 limit);[m[41m[m
[32m+[m		[32mIBackup **BackupPtr();[m[41m[m
[32m+[m		[32m// Functions used to obtain and release page references.[m[41m[m
[32m+[m		[32m//#define Acquire(A,B,C) Acquire(A,B,C,false)[m[41m[m
[32m+[m		[32mRC Acquire(Pid id, IPage **pageOut, bool noContent);[m[41m[m
[32m+[m		[32mIPage *Lookup(Pid id);[m[41m[m
[32m+[m		[32mstatic void Ref(IPage *pg);[m[41m[m
[32m+[m		[32mstatic void Unref(IPage *pg);[m[41m[m
[32m+[m		[32m// Operations on page references.[m[41m[m
[32m+[m		[32mstatic RC Write(IPage *page);[m[41m[m
 		static void DontWrite(PgHdr *pg);[m
[31m-[m
[32m+[m[41m[m
 	};[m
 [m
[31m-	//		int sqlite3PagerMovepage(Pager*,DbPage*,Pgno,int);[m
[31m-	//		int sqlite3PagerPageRefcount(DbPage*);[m
[31m-	//		void *sqlite3PagerGetData(DbPage *); [m
[31m-	//		void *sqlite3PagerGetExtra(DbPage *); [m
[31m-	//[m
[31m-	//		/* Functions used to manage pager transactions and savepoints. */[m
[31m-	//		void sqlite3PagerPagecount(Pager*, int*);[m
[31m-	//		int sqlite3PagerBegin(Pager*, int exFlag, int);[m
[31m-	//		int sqlite3PagerCommitPhaseOne(Pager*,const char *zMaster, int);[m
[31m-	//		int sqlite3PagerExclusiveLock(Pager*);[m
[31m-	//		int sqlite3PagerSync(Pager *pPager);[m
[31m-	//		int sqlite3PagerCommitPhaseTwo(Pager*);[m
[31m-	//		int sqlite3PagerRollback(Pager*);[m
[31m-	//		int sqlite3PagerOpenSavepoint(Pager *pPager, int n);[m
[31m-	//		int sqlite3PagerSavepoint(Pager *pPager, int op, int iSavepoint);[m
[31m-	//		int sqlite3PagerSharedLock(Pager *pPager);[m
[31m-	//[m
[31m-	//#ifndef SQLITE_OMIT_WAL[m
[31m-	//		int sqlite3PagerCheckpoint(Pager *pPager, int, int*, int*);[m
[31m-	//		int sqlite3PagerWalSupported(Pager *pPager);[m
[31m-	//		int sqlite3PagerWalCallback(Pager *pPager);[m
[31m-	//		int sqlite3PagerOpenWal(Pager *pPager, int *pisOpen);[m
[31m-	//		int sqlite3PagerCloseWal(Pager *pPager);[m
[31m-	//#endif[m
[31m-	//[m
[31m-	//#ifdef SQLITE_ENABLE_ZIPVFS[m
[31m-	//		int sqlite3PagerWalFramesize(Pager *pPager);[m
[31m-	//#endif[m
[31m-	//[m
[31m-	//		/* Functions used to query pager state and configuration. */[m
[31m-	//		u8 sqlite3PagerIsreadonly(Pager*);[m
[31m-	//		int sqlite3PagerRefcount(Pager*);[m
[31m-	//		int sqlite3PagerMemUsed(Pager*);[m
[31m-	//		const char *sqlite3PagerFilename(Pager*, int);[m
[31m-	//		const sqlite3_vfs *sqlite3PagerVfs(Pager*);[m
[31m-	//		sqlite3_file *sqlite3PagerFile(Pager*);[m
[31m-	//		const char *sqlite3PagerJournalname(Pager*);[m
[31m-	//		int sqlite3PagerNosync(Pager*);[m
[31m-	//		void *sqlite3PagerTempSpace(Pager*);[m
[31m-	//		int sqlite3PagerIsMemdb(Pager*);[m
[31m-	//		void sqlite3PagerCacheStat(Pager *, int, int, int *);[m
[31m-	//		void sqlite3PagerClearCache(Pager *);[m
[31m-	//		int sqlite3SectorSize(sqlite3_file *);[m
[31m-	//[m
[31m-	//		/* Functions used to truncate the database file. */[m
[31m-	//		void sqlite3PagerTruncateImage(Pager*,Pgno);[m
[31m-	//[m
[31m-	//#if defined(SQLITE_HAS_CODEC) && !defined(SQLITE_OMIT_WAL)[m
[31m-	//		void *sqlite3PagerCodec(DbPage *);[m
[31m-	//#endif[m
[31m-	//[m
[31m-	//		/* Functions to support testing and debugging. */[m
[31m-	//#if !defined(NDEBUG) || defined(SQLITE_TEST)[m
[31m-	//		Pgno sqlite3PagerPagenumber(DbPage*);[m
[31m-	//		int sqlite3PagerIswriteable(DbPage*);[m
[31m-	//#endif[m
[31m-	//#ifdef SQLITE_TEST[m
[31m-	//		int *sqlite3PagerStats(Pager*);[m
[31m-	//		void sqlite3PagerRefdump(Pager*);[m
[31m-	//		void disable_simulated_io_errors(void);[m
[31m-	//		void enable_simulated_io_errors(void);[m
[31m-	//#else[m
[31m-	//# define disable_simulated_io_errors()[m
[31m-	//# define enable_simulated_io_errors()[m
[31m-	//#endif[m
[32m+[m	[32m//		int sqlite3PagerMovepage(Pager*,DbPage*,Pgno,int);[m[41m[m
[32m+[m	[32m//		int sqlite3PagerPageRefcount(DbPage*);[m[41m[m
[32m+[m	[32m//		void *sqlite3PagerGetData(DbPage *);[m[41m [m
[32m+[m	[32m//		void *sqlite3PagerGetExtra(DbPage *);[m[41m [m
[32m+[m	[32m//[m[41m[m
[32m+[m	[32m//		/* Functions used to manage pager transactions and savepoints. */[m[41m[m
[32m+[m	[32m//		void sqlite3PagerPagecount(Pager*, int*);[m[41m[m
[32m+[m	[32m//		int sqlite3PagerBegin(Pager*, int exFlag, int);[m[41m[m
[32m+[m	[32m//		int sqlite3PagerCommitPhaseOne(Pager*,const char *zMaster, int);[m[41m[m
[32m+[m	[32m//		int sqlite3PagerExclusiveLock(Pager*);[m[41m[m
[32m+[m	[32m//		int sqlite3PagerSync(Pager *pPager);[m[41m[m
[32m+[m	[32m//		int sqlite3PagerCommitPhaseTwo(Pager*);[m[41m[m
[32m+[m	[32m//		int sqlite3PagerRollback(Pager*);[m[41m[m
[32m+[m	[32m//		int sqlite3PagerOpenSavepoint(Pager *pPager, int n);[m[41m[m
[32m+[m	[32m//		int sqlite3PagerSavepoint(Pager *pPager, int op, int iSavepoint);[m[41m[m
[32m+[m	[32m//		int sqlite3PagerSharedLock(Pager *pPager);[m[41m[m
[32m+[m	[32m//[m[41m[m
[32m+[m	[32m//#ifndef SQLITE_OMIT_WAL[m[41m[m
[32m+[m	[32m//		int sqlite3PagerCheckpoint(Pager *pPager, int, int*, int*);[m[41m[m
[32m+[m	[32m//		int sqlite3PagerWalSupported(Pager *pPager);[m[41m[m
[32m+[m	[32m//		int sqlite3PagerWalCallback(Pager *pPager);[m[41m[m
[32m+[m	[32m//		int sqlite3PagerOpenWal(Pager *pPager, int *pisOpen);[m[41m[m
[32m+[m	[32m//		i