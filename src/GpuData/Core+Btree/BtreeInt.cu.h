// btreeInt.h
namespace Core
{

#define MX_CELL_SIZE(bt)  ((int)(bt->PageSize-8))
#define MX_CELL(bt) ((bt->PageSize-8)/6)

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
		uint8 Overflows;		// Number of overflow cell bodies in aCell[]
		bool IntKey;			// True if intkey flag is set
		byte Leaf;				// 1 if leaf flag is set
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
		uint16 OvflIdxs[5];		// Insert the i-th overflow cell before the aiOvfl-thnon-overflow cell
		uint8 *Ovfls[5];		// Pointers to the body of overflow cells
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
		ISchema *Schema;		// Pointer to space allocated by sqlite3BtreeSchema()
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
		uint16 Overflow;	// Offset to overflow page number.  Zero if no overflow
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
		Pid RootID;				// The root page of this tree
		int64 CachedRowID;		// Next rowid cache.  0 means not valid
		CellInfo Info;          // A parse of the cell we are pointing at
		int64 KeyLength;		// Size of pKey, or last integer key
		void *Key;				// Saved key that was cursor's last known position
		int SkipNext;			// Prev() is noop if negative. Next() is noop if positive
		bool WrFlag;			// True if writable
		uint8 AtLast;			// Cursor pointing to the last entry
		bool ValidNKey;			// True if info.nKey is valid
		CURSOR State;			// One of the CURSOR_XXX constants (see below)
#ifndef OMIT_INCRBLOB
		bool IsIncrblobHandle;  // True if this cursor is an incr. io handle
#endif
		uint8 Hints;			// As configured by CursorSetHints()
		int16 ID;				// Index of current page in apPage
		uint16 Idxs[BTCURSOR_MAX_DEPTH]; // Current index in apPage[i]
		MemPage *Pages[BTCURSOR_MAX_DEPTH]; // Pages from root to current page
	};

#define MJ_PID(x) ((Pid)((PENDING_BYTE / ((x)->PageSize)) + 1))
#define PENDING_BYTE_PAGE(bt) MJ_PID(bt)

#define PTRMAP_PAGENO(bt, id) ptrmapPageno(bt, id)
#define PTRMAP_PTROFFSET(ptrmapID, id) (5 * (id - ptrmapID - 1))
#define PTRMAP_ISPAGE(bt, id) (PTRMAP_PAGENO((bt), (id)) == (id))

	enum PTRMAP : uint8
	{
		ROOTPAGE = 1,
		FREEPAGE = 2,
		OVERFLOW1 = 3,
		OVERFLOW2 = 4,
		BTREE = 5,
	};

#define btreeIntegrity(p) \
	_assert(p->Bt->InTransaction != TRANS::NONE || p->Bt->Transactions == 0); \
	_assert(p->Bt->InTransaction >= p->InTrans); 

	typedef struct IntegrityCk IntegrityCk;
	struct IntegrityCk
	{
		BtShared *Bt;		// The tree being checked out
		Pager *Pager;		// The associated pager.  Also accessible by pBt->pPager
		uint8 *PgRefs;		// 1 bit per page in the db (see above)
		Pid Pages;			// Number of pages in the database
		int MaxErrors;		// Stop accumulating errors when this reaches zero
		int Errors;			// Number of messages written to zErrMsg so far
		bool MallocFailed;	// A memory allocation error has occurred
		StrAccum ErrMsg;	// Accumulate the error message text here
	};

	BTS inline operator |= (BTS a, BTS b) { return (BTS)(a | b); }
	BTS inline operator &= (BTS a, BTS b) { return (BTS)(a & b); }
	BTS inline operator &= (BTS a, int b) { return (BTS)(a & b); }
}