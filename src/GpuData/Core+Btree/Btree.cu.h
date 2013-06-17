// btree.h
namespace Core
{
#define N_BTREE_META 10

#ifndef DEFAULT_AUTOVACUUM
#define DEFAULT_AUTOVACUUM 0
#endif

	typedef struct Btree Btree;
	typedef struct BtCursor BtCursor;
	typedef struct BtShared BtShared;

	class Btree
	{
	public:
		enum AUTOVACUUM : uint8
		{
			NONE = 0,        // Do not do auto-vacuum
			FULL = 1,        // Do full auto-vacuum
			INCR = 2,        // Incremental vacuum
		};
		enum OPEN : uint8
		{
			OMIT_JOURNAL = 1,   // Do not create or use a rollback journal
			MEMORY = 2,         // This is an in-memory DB
			SINGLE = 4,         // The file contains at most 1 b-tree
			UNORDERED = 8,      // Use of a hash implementation is OK
		};

		Context *Ctx;			// The database connection holding this btree
		BtShared *Bt;			// Sharable content of this btree
		TRANS InTrans;			// TRANS_NONE, TRANS_READ or TRANS_WRITE
		bool Sharable;			// True if we can share pBt with another db
		bool Locked;			// True if db currently has pBt locked
		int WantToLock;			// Number of nested calls to sqlite3BtreeEnter()
		int Backups;			// Number of backup operations reading this btree
		Btree *Next;			// List of other sharable Btrees from the same db
		Btree *Prev;			// Back pointer of the same list
#ifndef OMIT_SHARED_CACHE
		BtLock Lock;			// Object used to lock page 1
#endif

		static RC Open(VFileSystem *vfs, const char *filename, Context *ctx, Btree **btree, OPEN flags, VFileSystem::OPEN vfsFlags);
		RC Close();
		RC SetCacheSize(int maxPage);
		RC SetSafetyLevel(int level, bool fullSync, bool ckptFullSync);
		bool SyncDisabled();
		RC SetPageSize(int pageSize, int reserves, bool fix);
		int GetPageSize();
		int MaxPageCount(int maxPage);
		Pid LastPage();
		bool SecureDelete(bool newFlag);
		int GetReserve();
#if defined(HAS_CODEC) || defined(_DEBUG)
		int GetReserveNoMutex();
#endif
		RC SetAutoVacuum(AUTOVACUUM autoVacuum);
		AUTOVACUUM GetAutoVacuum();
		RC BeginTrans(int wrflag);
		RC CommitPhaseOne(const char *master);
		RC CommitPhaseTwo(bool cleanup);
		RC Commit();
		RC Rollback(RC tripCode);
		RC BeginStmt(int statement);
		int sqlite3BtreeCreateTable(Btree*, int*, int flags);
		int sqlite3BtreeIsInTrans(Btree*);
		int sqlite3BtreeIsInReadTrans(Btree*);
		int sqlite3BtreeIsInBackup(Btree*);
		void *sqlite3BtreeSchema(Btree *, int, void(*)(void *));
		int sqlite3BtreeSchemaLocked(Btree *pBtree);
		int sqlite3BtreeLockTable(Btree *pBtree, int iTab, uint8 isWriteLock);
		RC Savepoint(IPager::SAVEPOINT op, int savepoint);

		const char *sqlite3BtreeGetFilename(Btree *);
		const char *sqlite3BtreeGetJournalname(Btree *);
		int sqlite3BtreeCopyFile(Btree *, Btree *);

		RC IncrVacuum();

#define BTREE_INTKEY 1    // Table has only 64-bit signed integer keys
#define BTREE_BLOBKEY 2    // Table has keys only - no data

		int sqlite3BtreeDropTable(Btree*, int, int*);
		int sqlite3BtreeClearTable(Btree*, int, int*);
		void TripAllCursors(RC errCode);

		void sqlite3BtreeGetMeta(Btree *pBtree, int idx, uint32 *pValue);
		int sqlite3BtreeUpdateMeta(Btree*, int idx, uint32 value);

		RC NewDb();

		enum META : uint8
		{
			FREE_PAGE_COUNT = 0,
			SCHEMA_VERSION = 1,
			FILE_FORMAT = 2,
			DEFAULT_CACHE_SIZE  = 3,
			LARGEST_ROOT_PAGE = 4,
			TEXT_ENCODING = 5,
			USER_VERSION = 6,
			INCR_VACUUM = 7,
		};

#define BTREE_BULKLOAD 0x00000001

		RC Cursor(int table, int wrFlag, struct KeyInfo *keyInfo, BtCursor *cur);
		static int CursorSize();
		static void CursorZero(BtCursor *p);

		int sqlite3BtreeCloseCursor(BtCursor*);
		int sqlite3BtreeMovetoUnpacked(
			BtCursor*,
			UnpackedRecord *pUnKey,
			int64 intKey,
			int bias,
			int *pRes
			);
		int sqlite3BtreeCursorHasMoved(BtCursor*, int*);
		int sqlite3BtreeDelete(BtCursor*);
		int sqlite3BtreeInsert(BtCursor*, const void *pKey, int64 nKey,
			const void *pData, int nData,
			int nZero, int bias, int seekResult);
		int sqlite3BtreeFirst(BtCursor*, int *pRes);
		int sqlite3BtreeLast(BtCursor*, int *pRes);
		int sqlite3BtreeNext(BtCursor*, int *pRes);
		int sqlite3BtreeEof(BtCursor*);
		int sqlite3BtreePrevious(BtCursor*, int *pRes);
		static RC KeySize(BtCursor *cur, int64 *size);
		int sqlite3BtreeKeySize(BtCursor*, int64 *pSize);
		int sqlite3BtreeKey(BtCursor*, uint32 offset, uint32 amt, void*);
		const void *sqlite3BtreeKeyFetch(BtCursor*, int *pAmt);
		const void *sqlite3BtreeDataFetch(BtCursor*, int *pAmt);
		static RC DataSize(BtCursor *cur, uint32 *size)
		int sqlite3BtreeData(BtCursor*, uint32 offset, uint32 amt, void*);
		static void SetCachedRowID(BtCursor *cur, int64 rowid);
		static int64 GetCachedRowID(BtCursor *cur);
		static RC CloseCursor(BtCursor *cur);

		char *sqlite3BtreeIntegrityCheck(Btree*, int *aRoot, int nRoot, int, int*);
		struct Pager *sqlite3BtreePager(Btree*);

		int sqlite3BtreePutData(BtCursor*, uint32 offset, uint32 amt, void*);
		void sqlite3BtreeCacheOverflow(BtCursor *);
		void sqlite3BtreeClearCursor(BtCursor *);
		int sqlite3BtreeSetVersion(Btree *pBt, int iVersion);
		void sqlite3BtreeCursorHints(BtCursor *, unsigned int mask);

#ifndef DEBUG
		static bool CursorIsValid(BtCursor *cur);
#endif

#ifndef OMIT_BTREECOUNT
		int sqlite3BtreeCount(BtCursor *, int64 *);
#endif

#ifdef TEST
		int sqlite3BtreeCursorInfo(BtCursor*, int*, int);
		void sqlite3BtreeCursorList(Btree*);
#endif

#ifndef OMIT_WAL
		int sqlite3BtreeCheckpoint(Btree*, int, int *, int *);
#endif

#ifndef SHARED_CACHE
		void sqlite3BtreeEnter(Btree*);
		void sqlite3BtreeEnterAll(Context*);
#else
#define Enter(X) 
#define EnterAll(X)
#endif

#if !defined(OMIT_SHARED_CACHE) && THREADSAFE
		int sqlite3BtreeSharable(Btree*);
		void sqlite3BtreeLeave(Btree*);
		void sqlite3BtreeEnterCursor(BtCursor*);
		void sqlite3BtreeLeaveCursor(BtCursor*);
		void sqlite3BtreeLeaveAll(sqlite3*);
#ifndef _DEBUG
		// These routines are used inside assert() statements only.
		int sqlite3BtreeHoldsMutex(Btree*);
		int sqlite3BtreeHoldsAllMutexes(sqlite3*);
		int sqlite3SchemaMutexHeld(sqlite3*,int,Schema*);
#endif
#else

#define sqlite3BtreeSharable(X) 0
#define sqlite3BtreeLeave(X)
#define sqlite3BtreeEnterCursor(X)
#define sqlite3BtreeLeaveCursor(X)
#define sqlite3BtreeLeaveAll(X)

#define sqlite3BtreeHoldsMutex(X) 1
#define sqlite3BtreeHoldsAllMutexes(X) 1
#define sqlite3SchemaMutexHeld(X,Y,Z) 1
#endif


	};
}