// btree.h
namespace Core
{
#define N_BTREE_META 10
#ifndef DEFAULT_AUTOVACUUM
#define DEFAULT_AUTOVACUUM 0
#endif

	class Btree
	{
	public:
		enum AUTOVACUUM : char
		{
			NONE = 0,        // Do not do auto-vacuum
			FULL = 1,        // Do full auto-vacuum
			INCR = 2,        // Incremental vacuum
		};
	};

	typedef struct Btree Btree;
	typedef struct BtCursor BtCursor;
	typedef struct BtShared BtShared;

	int sqlite3BtreeOpen(sqlite3_vfs *pVfs, const char *zFilename, sqlite3 *db, Btree **ppBtree, int flags, int vfsFlags);

#define BTREE_OMIT_JOURNAL  1  /* Do not create or use a rollback journal */
#define BTREE_MEMORY        2  /* This is an in-memory DB */
#define BTREE_SINGLE        4  /* The file contains at most 1 b-tree */
#define BTREE_UNORDERED     8  /* Use of a hash implementation is OK */

	int sqlite3BtreeClose(Btree*);
	int sqlite3BtreeSetCacheSize(Btree*,int);
	int sqlite3BtreeSetSafetyLevel(Btree*,int,int,int);
	int sqlite3BtreeSyncDisabled(Btree*);
	int sqlite3BtreeSetPageSize(Btree *p, int nPagesize, int nReserve, int eFix);
	int sqlite3BtreeGetPageSize(Btree*);
	int sqlite3BtreeMaxPageCount(Btree*,int);
	u32 sqlite3BtreeLastPage(Btree*);
	int sqlite3BtreeSecureDelete(Btree*,int);
	int sqlite3BtreeGetReserve(Btree*);
#if defined(SQLITE_HAS_CODEC) || defined(SQLITE_DEBUG)
	int sqlite3BtreeGetReserveNoMutex(Btree *p);
#endif
	int sqlite3BtreeSetAutoVacuum(Btree *, int);
	int sqlite3BtreeGetAutoVacuum(Btree *);
	int sqlite3BtreeBeginTrans(Btree*,int);
	int sqlite3BtreeCommitPhaseOne(Btree*, const char *zMaster);
	int sqlite3BtreeCommitPhaseTwo(Btree*, int);
	int sqlite3BtreeCommit(Btree*);
	int sqlite3BtreeRollback(Btree*,int);
	int sqlite3BtreeBeginStmt(Btree*,int);
	int sqlite3BtreeCreateTable(Btree*, int*, int flags);
	int sqlite3BtreeIsInTrans(Btree*);
	int sqlite3BtreeIsInReadTrans(Btree*);
	int sqlite3BtreeIsInBackup(Btree*);
	void *sqlite3BtreeSchema(Btree *, int, void(*)(void *));
	int sqlite3BtreeSchemaLocked(Btree *pBtree);
	int sqlite3BtreeLockTable(Btree *pBtree, int iTab, u8 isWriteLock);
	int sqlite3BtreeSavepoint(Btree *, int, int);

	const char *sqlite3BtreeGetFilename(Btree *);
	const char *sqlite3BtreeGetJournalname(Btree *);
	int sqlite3BtreeCopyFile(Btree *, Btree *);

	int sqlite3BtreeIncrVacuum(Btree *);

#define BTREE_INTKEY     1    /* Table has only 64-bit signed integer keys */
#define BTREE_BLOBKEY    2    /* Table has keys only - no data */

	int sqlite3BtreeDropTable(Btree*, int, int*);
	int sqlite3BtreeClearTable(Btree*, int, int*);
	void sqlite3BtreeTripAllCursors(Btree*, int);

	void sqlite3BtreeGetMeta(Btree *pBtree, int idx, u32 *pValue);
	int sqlite3BtreeUpdateMeta(Btree*, int idx, u32 value);

	int sqlite3BtreeNewDb(Btree *p);

#define BTREE_FREE_PAGE_COUNT     0
#define BTREE_SCHEMA_VERSION      1
#define BTREE_FILE_FORMAT         2
#define BTREE_DEFAULT_CACHE_SIZE  3
#define BTREE_LARGEST_ROOT_PAGE   4
#define BTREE_TEXT_ENCODING       5
#define BTREE_USER_VERSION        6
#define BTREE_INCR_VACUUM         7

#define BTREE_BULKLOAD 0x00000001

	int sqlite3BtreeCursor(
		Btree*,                              /* BTree containing table to open */
		int iTable,                          /* Index of root page */
		int wrFlag,                          /* 1 for writing.  0 for read-only */
	struct KeyInfo*,                     /* First argument to compare function */
		BtCursor *pCursor                    /* Space to write cursor structure */
		);
	int sqlite3BtreeCursorSize(void);
	void sqlite3BtreeCursorZero(BtCursor*);

	int sqlite3BtreeCloseCursor(BtCursor*);
	int sqlite3BtreeMovetoUnpacked(
		BtCursor*,
		UnpackedRecord *pUnKey,
		i64 intKey,
		int bias,
		int *pRes
		);
	int sqlite3BtreeCursorHasMoved(BtCursor*, int*);
	int sqlite3BtreeDelete(BtCursor*);
	int sqlite3BtreeInsert(BtCursor*, const void *pKey, i64 nKey,
		const void *pData, int nData,
		int nZero, int bias, int seekResult);
	int sqlite3BtreeFirst(BtCursor*, int *pRes);
	int sqlite3BtreeLast(BtCursor*, int *pRes);
	int sqlite3BtreeNext(BtCursor*, int *pRes);
	int sqlite3BtreeEof(BtCursor*);
	int sqlite3BtreePrevious(BtCursor*, int *pRes);
	int sqlite3BtreeKeySize(BtCursor*, i64 *pSize);
	int sqlite3BtreeKey(BtCursor*, u32 offset, u32 amt, void*);
	const void *sqlite3BtreeKeyFetch(BtCursor*, int *pAmt);
	const void *sqlite3BtreeDataFetch(BtCursor*, int *pAmt);
	int sqlite3BtreeDataSize(BtCursor*, u32 *pSize);
	int sqlite3BtreeData(BtCursor*, u32 offset, u32 amt, void*);
	void sqlite3BtreeSetCachedRowid(BtCursor*, sqlite3_int64);
	sqlite3_int64 sqlite3BtreeGetCachedRowid(BtCursor*);

	char *sqlite3BtreeIntegrityCheck(Btree*, int *aRoot, int nRoot, int, int*);
	struct Pager *sqlite3BtreePager(Btree*);

	int sqlite3BtreePutData(BtCursor*, u32 offset, u32 amt, void*);
	void sqlite3BtreeCacheOverflow(BtCursor *);
	void sqlite3BtreeClearCursor(BtCursor *);
	int sqlite3BtreeSetVersion(Btree *pBt, int iVersion);
	void sqlite3BtreeCursorHints(BtCursor *, unsigned int mask);

#ifndef NDEBUG
	int sqlite3BtreeCursorIsValid(BtCursor*);
#endif

#ifndef SQLITE_OMIT_BTREECOUNT
	int sqlite3BtreeCount(BtCursor *, i64 *);
#endif

#ifdef SQLITE_TEST
	int sqlite3BtreeCursorInfo(BtCursor*, int*, int);
	void sqlite3BtreeCursorList(Btree*);
#endif

#ifndef SQLITE_OMIT_WAL
	int sqlite3BtreeCheckpoint(Btree*, int, int *, int *);
#endif

#ifndef OMIT_SHARED_CACHE
	void sqlite3BtreeEnter(Btree*);
	void sqlite3BtreeEnterAll(sqlite3*);
#else
# define sqlite3BtreeEnter(X) 
# define sqlite3BtreeEnterAll(X)
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
}