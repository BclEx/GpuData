// btreeInt.h
namespace Core
{

#define MX_CELL_SIZE(pBt)  ((int)(pBt->pageSize-8))
#define MX_CELL(pBt) ((pBt->pageSize-8)/6)

	typedef struct MemPage MemPage;
	typedef struct BtLock BtLock;

#ifndef FILE_HEADER
#define FILE_HEADER "SQLite format 3"
#endif

#define PTF_INTKEY    0x01
#define PTF_ZERODATA  0x02
#define PTF_LEAFDATA  0x04
#define PTF_LEAF      0x08

	struct MemPage
	{
		bool IsInit;			// True if previously initialized. MUST BE FIRST!
		uint8 OverflowsUsed;    // Number of overflow cell bodies in aCell[]
		bool IntKey;			// True if intkey flag is set
		bool Leaf;				// True if leaf flag is set
		bool HasData;			// True if this page stores data
		uint8 HdrOffset;        // 100 for page 1.  0 otherwise
		uint8 ChildPtrSize;     // 0 if leaf==1.  4 if leaf==0
		uint8 Max1bytePayload;  // min(maxLocal,127)
		uint16 MaxLocal;        // Copy of BtShared.maxLocal or BtShared.maxLeaf
		uint16 MinLocal;        // Copy of BtShared.minLocal or BtShared.minLeaf
		uint16 CellOffset;      // Index in aData of first cell pointer
		uint16 Frees;           // Number of free bytes on the page
		uint16 Cells;           // Number of cells on this page, local and ovfl
		uint16 MaskPage;        // Mask for page offset
		uint16 OverflowIdxs[5]; // Insert the i-th overflow cell before the aiOvfl-thnon-overflow cell
		uint8 *Overflows[5];    // Pointers to the body of overflow cells
		BtShared *Bt;			// Pointer to BtShared that this page is part of
		uint8 *Data;			// Pointer to disk image of the page data
		uint8 *DataEnd;			// One byte past the end of usable data
		uint8 *CellIdx;			// The cell index area
		IPage *DBPage;			// Pager page handle
		Pid ID;					// Page number for this page
	};

#define EXTRA_SIZE sizeof(MemPage)

	enum LOCK : uint8
	{
		READ = 1,
		WRITE = 2,
	};

	struct BtLock
	{
		Btree *Btree;			// Btree handle holding this lock
		Pid Table;				// Root page of table
		LOCK Lock;				// READ_LOCK or WRITE_LOCK
		BtLock *Next;			// Next in BtShared.pLock list
	};

	enum TRANS : uint8
	{
		NONE = 0,
		READ = 1,
		WRITE = 2,
	};

	struct Btree
	{
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
	};

	enum BTS : uint16
	{
		READ_ONLY = 0x0001,		// Underlying file is readonly
		PAGESIZE_FIXED = 0x0002,// Page size can no longer be changed
		SECURE_DELETE = 0x0004, // PRAGMA secure_delete is enabled
		INITIALLY_EMPTY  = 0x0008, // Database was empty at trans start
		NO_WAL = 0x0010,		// Do not open write-ahead-log files
		EXCLUSIVE = 0x0020,		// pWriter has an exclusive lock
		PENDING = 0x0040,		// Waiting for read-locks to clear
	};

	struct BtShared
	{
		Pager *Pager;			// The page cache
		Context *Ctx;			// Database connection currently using this Btree
		BtCursor *Cursor;		// A list of all open cursors
		MemPage *Page1;			// First page of the database
		byte OpenFlags;			// Flags to sqlite3BtreeOpen()
#ifndef OMIT_AUTOVACUUM
		bool AutoVacuum;		// True if auto-vacuum is enabled
		bool IncrVacuum;		// True if incr-vacuum is enabled
		bool DoTruncate;		// True to truncate db on commit
#endif
		TRANS InTransaction;	// Transaction state
		uint8 Max1bytePayload;	// Maximum first byte of cell for a 1-byte payload
		BTS BtsFlags;			// Boolean parameters.  See BTS_* macros below
		uint16 MaxLocal;		// Maximum local payload in non-LEAFDATA tables
		uint16 MinLocal;		// Minimum local payload in non-LEAFDATA tables
		uint16 MaxLeaf;			// Maximum local payload in a LEAFDATA table
		uint16 MinLeaf;			// Minimum local payload in a LEAFDATA table
		uint32 PageSize;		// Total number of bytes on a page
		uint32 UsableSize;		// Number of usable bytes on each page
		int Transactions;		// Number of open transactions (read + write)
		uint32 Pages;			// Number of pages in the database
		void *Schema;			// Pointer to space allocated by sqlite3BtreeSchema()
		void (*FreeSchema)(void *);  // Destructor for BtShared.pSchema
		MutexEx Mutex;			// Non-recursive mutex required to access this object
		Bitvec *HasContent;		// Set of pages moved to free-list this transaction
#ifndef OMIT_SHARED_CACHE
		int Refs;				// Number of references to this structure
		BtShared *Next;			// Next on a list of sharable BtShared structs
		BtLock *Lock;			// List of locks held on this shared-btree struct
		Btree *Writer;			// Btree with currently open write transaction
#endif
		uint8 *TmpSpace;		// BtShared.pageSize bytes of space for tmp use
	};

	typedef struct CellInfo CellInfo;
	struct CellInfo
	{
		int64 Key;				// The key for INTKEY tables, or number of bytes in key
		uint8 *Cell;			// Pointer to the start of cell content
		uint32 Data;			// Number of bytes of data
		uint32 Payload;			// Total amount of payload
		uint16 Header;			// Size of the cell content header in bytes
		uint16 Local;			// Amount of payload held locally
		uint16 OverflowIndex;	// Offset to overflow page number.  Zero if no overflow
		uint16 Size;			// Size of the cell content on the main b-tree page
	};

#define BTCURSOR_MAX_DEPTH 20

	enum CURSOR : uint8
	{
		INVALID = 0,
		VALID = 1,
		REQUIRESEEK = 2,
		FAULT = 3,
	};

	struct BtCursor
	{
		Btree *Btree;           // The Btree to which this cursor belongs
		BtShared *Bt;           // The BtShared this cursor points to
		BtCursor *Next, *Prev;	// Forms a linked list of all cursors
		struct KeyInfo *KeyInfo; // Argument passed to comparison function
#ifndef OMIT_INCRBLOB
		uint32 *Overflows;		// Cache of overflow page locations
#endif
		Pid IDRoot;				// The root page of this tree
		int64 CachedRowID;		// Next rowid cache.  0 means not valid
		CellInfo Info;          // A parse of the cell we are pointing at
		int64 nKey;				// Size of pKey, or last integer key
		void *pKey;				// Saved key that was cursor's last known position
		int SkipNext;			// Prev() is noop if negative. Next() is noop if positive
		bool WrFlag;			// True if writable
		uint8 AtLast;			// Cursor pointing to the last entry
		bool ValidNKey;			// True if info.nKey is valid
		CURSOR State;			// One of the CURSOR_XXX constants (see below)
#ifndef OMIT_INCRBLOB
		bool IsIncrblobHandle;  // True if this cursor is an incr. io handle
#endif
		uint8 Hints;                             // As configured by CursorSetHints()
		int16 PageIdx;                            // Index of current page in apPage
		uint16 aiIdx[BTCURSOR_MAX_DEPTH];        // Current index in apPage[i]
		MemPage *apPage[BTCURSOR_MAX_DEPTH];  // Pages from root to current page
	};

#define PENDING_BYTE_PAGE(pBt) PAGER_MJ_PGNO(pBt)

#define PTRMAP_PAGENO(pBt, pgno) ptrmapPageno(pBt, pgno)
#define PTRMAP_PTROFFSET(pgptrmap, pgno) (5*(pgno-pgptrmap-1))
#define PTRMAP_ISPAGE(pBt, pgno) (PTRMAP_PAGENO((pBt),(pgno))==(pgno))

#define PTRMAP_ROOTPAGE 1
#define PTRMAP_FREEPAGE 2
#define PTRMAP_OVERFLOW1 3
#define PTRMAP_OVERFLOW2 4
#define PTRMAP_BTREE 5

#define btreeIntegrity(p) \
	assert( p->pBt->inTransaction!=TRANS_NONE || p->pBt->nTransaction==0 ); \
	assert( p->pBt->inTransaction>=p->inTrans ); 

#ifndef OMIT_AUTOVACUUM
#define ISAUTOVACUUM (pBt->autoVacuum)
#else
#define ISAUTOVACUUM 0
#endif

	typedef struct IntegrityCk IntegrityCk;
	struct IntegrityCk
	{
		BtShared *pBt;    // The tree being checked out
		Pager *pPager;    // The associated pager.  Also accessible by pBt->pPager
		uint8 *aPgRef;       // 1 bit per page in the db (see above)
		Pid nPage;       // Number of pages in the database
		int mxErr;        // Stop accumulating errors when this reaches zero
		int nErr;         // Number of messages written to zErrMsg so far
		int mallocFailed; // A memory allocation error has occurred
		StrAccum errMsg;  // Accumulate the error message text here
	};

#define get2byte(x)   ((x)[0]<<8 | (x)[1])
#define put2byte(p,v) ((p)[0] = (u8)((v)>>8), (p)[1] = (u8)(v))
#define get4byte sqlite3Get4byte
#define put4byte sqlite3Put4byte

}