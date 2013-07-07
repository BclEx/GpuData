// btree.h
typedef struct Mem Mem;
namespace Core
{
#define N_BTREE_META 10

#ifndef DEFAULT_AUTOVACUUM
#define DEFAULT_AUTOVACUUM AUTOVACUUM::NONE
#endif

	//enum class LOCK : uint8;
	//enum class TRANS : uint8;
	typedef struct KeyInfo KeyInfo;
	typedef struct UnpackedRecord UnpackedRecord;
	typedef struct Btree Btree;
	typedef struct BtCursor BtCursor;
	typedef struct BtShared BtShared;

	struct KeyInfo
	{
		Context *Ctx;		// The database connection
		uint8 Enc;			// Text encoding - one of the SQLITE_UTF* values
		uint16 Fields;      // Number of entries in aColl[]
		uint8 *SortOrders;  // Sort order for each column.  May be NULL
		CollSeq *Colls[1];  // Collating sequence for each term of the key
	};

	enum class UNPACKED : uint8
	{
		INCRKEY = 0x01,			// Make this key an epsilon larger
		PREFIX_MATCH = 0x02,	// A prefix match is considered OK
		PREFIX_SEARCH = 0x04,	// Ignore final (rowid) field
	};

	struct UnpackedRecord
	{
		KeyInfo *KeyInfo;	// Collation and sort-order information
		uint16 Fields;      // Number of entries in apMem[]
		UNPACKED Flags;     // Boolean settings.  UNPACKED_... below
		int64 Rowid;        // Used by UNPACKED_PREFIX_SEARCH
		Mem *Mems;          // Values
	};

	enum class LOCK : uint8
	{
		READ = 1,
		WRITE = 2,
	};

	enum class TRANS : uint8
	{
		NONE = 0,
		READ = 1,
		WRITE = 2,
	};

	class Btree
	{
	public:
		enum class AUTOVACUUM : uint8
		{
			NONE = 0,        // Do not do auto-vacuum
			FULL = 1,        // Do full auto-vacuum
			INCR = 2,        // Incremental vacuum
		};

		enum class OPEN : uint8
		{
			OMIT_JOURNAL = 1,   // Do not create or use a rollback journal
			MEMORY = 2,         // This is an in-memory DB
			SINGLE = 4,         // The file contains at most 1 b-tree
			UNORDERED = 8,      // Use of a hash implementation is OK
		};

		struct BtLock
		{
			Btree *Btree;			// Btree handle holding this lock
			Pid Table;				// Root page of table
			LOCK Lock;				// READ_LOCK or WRITE_LOCK
			BtLock *Next;			// Next in BtShared.pLock list
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
		ISchema *Schema(int bytes, void (*free)(void *));
		RC SchemaLocked();
		RC LockTable(Pid tableID, bool isWriteLock);
		RC Savepoint(IPager::SAVEPOINT op, int savepoint);

		const char *get_Filename();
		const char *get_Journalname();
		//int sqlite3BtreeCopyFile(Btree *, Btree *);

		RC IncrVacuum();

#define BTREE_INTKEY 1 // Table has only 64-bit signed integer keys
#define BTREE_BLOBKEY 2 // Table has keys only - no data

		RC DropTable(int tableID, int *movedID);
		RC ClearTable(int tableID, int *changes);
		void TripAllCursors(RC errCode);

		enum class META : uint8
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
		void GetMeta(META id, uint32 *meta);
		RC UpdateMeta(META id, uint32 meta);

		RC NewDb();

#define BTREE_BULKLOAD 0x00000001

		RC Cursor(Pid tableID, bool wrFlag, struct KeyInfo *keyInfo, BtCursor *cur);
		static int CursorSize();
		static void CursorZero(BtCursor *p);

		static RC CloseCursor(BtCursor *cur);
		static RC MovetoUnpacked(BtCursor *cur, UnpackedRecord *idxKey, int64 intKey, int biasRight, int *res);
		static RC CursorHasMoved(BtCursor *cur, bool *hasMoved);
		static RC Delete(BtCursor *cur);
		static RC Insert(BtCursor *cur, const void *key, int64 keyLength, const void *data, int dataLength, int zero, int appendBias, int seekResult);
		static RC First(BtCursor *cur, int *res);
		static RC Last(BtCursor *cur, int *res);
		static RC Next_(BtCursor *cur, int *res);
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

		char *IntegrityCheck(Pid *roots, int rootsLength, int maxErrors, int *errors);
		Pager *get_Pager();

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

#ifndef OMIT_SHARED_CACHE
		void Enter();
		//static void EnterAll(Context *);
#else
#define Enter(X) 
		//#define EnterAll(X)
#endif

#ifndef defined(OMIT_SHARED_CACHE)
		//int Sharable();
		inline void Leave() { }
		//static void EnterCursor(BtCursor *);
		//static void LeaveCursor(BtCursor *);
		//static void LeaveAll(Context *);
		//#ifndef _DEBUG
		// These routines are used inside assert() statements only.
		inline bool HoldsMutex() { return true; }
		//int HoldsAllMutexes(sqlite3*);
		//int sqlite3SchemaMutexHeld(sqlite3*,int,Schema*);
		//#endif
#else
#define Sharable(X) 0
#define Leave(X)
#define EnterCursor(X)
#define LeaveCursor(X)
#define LeaveAll(X)
#ifndef _DEBUG
		// These routines are used inside assert() statements only.
#define HoldsMutex(X) 1
#define HoldsAllMutexes(X) 1
#define sqlite3SchemaMutexHeld(X,Y,Z) 1
#endif
#endif

	};

	typedef struct Btree::BtLock BtLock;
	Btree::OPEN inline operator |= (Btree::OPEN a, Btree::OPEN b) { return (Btree::OPEN)((uint8)a | (uint8)b); }
	uint8 inline operator & (Btree::OPEN a, Btree::OPEN b) { return ((uint8)a & (uint8)b); }
}