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
		RC CreateTable(int *tableID, int flags);
		bool IsInTrans();
		bool IsInReadTrans();
		bool IsInBackup();
		void *Schema(int bytes, void (*free)(void *));
		RC SchemaLocked();
		RC LockTable(int tableID, bool isWriteLock);
		RC Savepoint(IPager::SAVEPOINT op, int savepoint);

		const char *GetFilename();
		const char *GetJournalname();
		//int sqlite3BtreeCopyFile(Btree *, Btree *);

		RC IncrVacuum();

#define BTREE_INTKEY 1 // Table has only 64-bit signed integer keys
#define BTREE_BLOBKEY 2 // Table has keys only - no data

		RC DropTable(int tableID, int *movedID);
		RC ClearTable(int tableID, int *changes);
		void TripAllCursors(RC errCode);

		void GetMeta(int idx, uint32 *meta);
		RC UpdateMeta(int idx, uint32 meta);

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

		static RC CloseCursor(BtCursor *cur);
		static RC MovetoUnpacked(BtCursor *cur, UnpackedRecord *idxKey, int64 intKey, int biasRight, int *res);
		static RC CursorHasMoved(BtCursor *cur, bool *hasMoved);
		static RC Delete(BtCursor *cur);
		static RC Insert(BtCursor *cur, const void *key, int64 keyLength, const void *data, int dataLength, int zero, int appendBias, int seekResult);
		static RC First(BtCursor *cur, int *res);
		static RC Last(BtCursor *cur, int *res);
		static RC Next(BtCursor *cur, int *res);
		static bool Eof(BtCursor *cur);
		static RC Previous(BtCursor *cur, int *res);
		static RC KeySize(BtCursor *cur, int64 *size);
		static RC Key(BtCursor *cur, uint32 offset, uint32 amount, void *buf);
		static const void *KeyFetch(BtCursor *cur, int *amount);
		static const void *DataFetch(BtCursor *cur, int *amount);
		static RC DataSize(BtCursor *cur, uint32 *size);
		static RC Data(BtCursor *cur, uint32 offset, uint32 amount, void *buf);
		static void SetCachedRowID(BtCursor *cur, int64 rowid);
		static int64 GetCachedRowID(BtCursor *cur);
		static RC CloseCursor(BtCursor *cur);

		char *sqlite3BtreeIntegrityCheck(Btree*, int *aRoot, int nRoot, int, int*);
		Pager *Pager();

		static RC PutData(BtCursor *cur, uint32 offset, uint32 amount, void *z);
		static void CacheOverflow(BtCursor *cur);
		static void ClearCursor(BtCursor *cur);
		RC SetVersion(int version);
		static void CursorHints(BtCursor *cur, unsigned int mask);

#ifndef DEBUG
		static bool CursorIsValid(BtCursor *cur);
#endif

#ifndef OMIT_BTREECOUNT
		static RC Count(BtCursor *cur, int64 *entrysOut);
#endif

#ifdef TEST
		//int sqlite3BtreeCursorInfo(BtCursor*, int*, int);
		//void sqlite3BtreeCursorList(Btree*);
#endif

#ifndef OMIT_WAL
		RC Checkpoint(int mode, int *logs, int *checkpoints);
#endif

#ifndef SHARED_CACHE
		//void sqlite3BtreeEnter(Btree*);
		//void sqlite3BtreeEnterAll(Context*);
#else
		//#define Enter(X) 
		//#define EnterAll(X)
#endif

#if !defined(OMIT_SHARED_CACHE) && THREADSAFE
		//int sqlite3BtreeSharable(Btree*);
		//void sqlite3BtreeLeave(Btree*);
		//void sqlite3BtreeEnterCursor(BtCursor*);
		//void sqlite3BtreeLeaveCursor(BtCursor*);
		//void sqlite3BtreeLeaveAll(sqlite3*);
#ifndef _DEBUG
		// These routines are used inside assert() statements only.
		//int sqlite3BtreeHoldsMutex(Btree*);
		//int sqlite3BtreeHoldsAllMutexes(sqlite3*);
		//int sqlite3SchemaMutexHeld(sqlite3*,int,Schema*);
#endif
#else
		//#define sqlite3BtreeSharable(X) 0
		//#define sqlite3BtreeLeave(X)
		//#define sqlite3BtreeEnterCursor(X)
		//#define sqlite3BtreeLeaveCursor(X)
		//#define sqlite3BtreeLeaveAll(X)
		// These routines are used inside assert() statements only.
		//#define sqlite3BtreeHoldsMutex(X) 1
		//#define sqlite3BtreeHoldsAllMutexes(X) 1
		//#define sqlite3SchemaMutexHeld(X,Y,Z) 1
#endif


	};
}