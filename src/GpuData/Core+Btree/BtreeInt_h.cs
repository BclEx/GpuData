using Pid = System.UInt32;
using IPage = Core.PgHdr;
using System;
using System.Diagnostics;

namespace Core
{
    public partial class Btree
    {
        const string FILE_HEADER = "SQLite format 3\0";

        const byte PTF_INTKEY = 0x01;
        const byte PTF_ZERODATA = 0x02;
        const byte PTF_LEAFDATA = 0x04;
        const byte PTF_LEAF = 0x08;

        public struct OverflowCell // Cells that will not fit on aData[]
        {
            public byte[] Cell;     // Pointers to the body of the overflow cell
            public ushort Idx;      // Insert this cell before idx-th non-overflow cell

            public OverflowCell Copy()
            {
                var cp = new OverflowCell();
                if (Cell != null)
                {
                    cp.Cell = SysEx.Alloc(Cell.Length);
                    Buffer.BlockCopy(Cell, 0, cp.Cell, 0, Cell.Length);
                }
                cp.Idx = Idx;
                return cp;
            }
        };

        public class MemPage
        {
            public bool IsInit;             // True if previously initialized. MUST BE FIRST!
            public byte OverflowsUsed;      // Number of overflow cell bodies in aCell[]
            public byte IntKey;             // True if u8key flag is set
            public byte Leaf;               // 1 if leaf flag is set
            public byte HasData;            // True if this page stores data
            public byte HdrOffset;          // 100 for page 1.  0 otherwise
            public byte ChildPtrSize;       // 0 if leaf==1.  4 if leaf==0
            public ushort MaxLocal;         // Copy of BtShared.maxLocal or BtShared.maxLeaf
            public ushort MinLocal;         // Copy of BtShared.minLocal or BtShared.minLeaf
            public ushort CellOffset;       // Index in aData of first cell pou16er
            public ushort Frees;            // Number of free bytes on the page
            public ushort Cells;            // Number of cells on this page, local and ovfl
            public ushort MaskPage;         // Mask for page offset
            public OverflowCell[] Overflows = new OverflowCell[5];
            public BtShared Bt;             // Pointer to BtShared that this page is part of
            public byte[] Data;             // Pointer to disk image of the page data
            public IPage DBPage;            // Pager page handle
            public Pid ID;                  // Page number for this page

            public MemPage Copy()
            {
                var cp = (MemPage)MemberwiseClone();
                if (Overflows != null)
                {
                    cp.Overflows = new OverflowCell[Overflows.Length];
                    for (int i = 0; i < Overflows.Length; i++)
                        cp.Overflows[i] = Overflows[i].Copy();
                }
                if (Data != null)
                {
                    cp.Data = SysEx.Alloc(Data.Length);
                    Buffer.BlockCopy(Data, 0, cp.Data, 0, Data.Length);
                }
                return cp;
            }
        };

        const int EXTRA_SIZE = 0; // No used in C#, since we use create a class; was MemPage.Length;

        public enum LOCK : byte
        {
            READ = 1,
            WRITE = 2,
        }

        public class BtLock
        {
            public Btree Btree;            // Btree handle holding this lock
            public Pid Table;              // Root page of table
            public LOCK Lock;              // READ_LOCK or WRITE_LOCK
            public BtLock Next;            // Next in BtShared.pLock list
        }

        public enum TRANS : byte
        {
            NONE = 0,
            READ = 1,
            WRITE = 2,
        }

        public class Btree
        {
            public Context Ctx;     // The database connection holding this Btree
            public BtShared Bt;     // Sharable content of this Btree
            public TRANS InTrans;   // TRANS_NONE, TRANS_READ or TRANS_WRITE
            public bool Sharable;   // True if we can share pBt with another db
            public bool Locked;     // True if db currently has pBt locked
            public int WantToLock;  // Number of nested calls to sqlite3BtreeEnter()
            public int Backups;     // Number of backup operations reading this btree
            public Btree Next;      // List of other sharable Btrees from the same db
            public Btree Prev;      // Back pointer of the same list
#if !OMIT_SHARED_CACHE
            public BtLock Lock;     // Object used to lock page 1
#endif
        }

        enum BTS : ushort
        {
            READ_ONLY = 0x0001,		// Underlying file is readonly
            PAGESIZE_FIXED = 0x0002,// Page size can no longer be changed
            SECURE_DELETE = 0x0004, // PRAGMA secure_delete is enabled
            INITIALLY_EMPTY = 0x0008, // Database was empty at trans start
            NO_WAL = 0x0010,		// Do not open write-ahead-log files
            EXCLUSIVE = 0x0020,		// pWriter has an exclusive lock
            PENDING = 0x0040,		// Waiting for read-locks to clear
        }

        public class BtShared
        {
            public Pager Pager;             // The page cache
            public Context Ctx;             // Database connection currently using this Btree
            public BtCursor Cursor;         // A list of all open cursors
            public MemPage Page1;           // First page of the database
            //public bool ReadOnly;           // True if the underlying file is readonly
            //public bool PageSizeFixed;      // True if the page size can no longer be changed
            //public bool SecureDelete;       // True if secure_delete is enabled
            //public bool InitiallyEmpty;     // Database is empty at start of transaction
            public byte OpenFlags;          // Flags to sqlite3BtreeOpen()
#if !OMIT_AUTOVACUUM
            public bool AutoVacuum;         // True if auto-vacuum is enabled
            public bool IncrVacuum;         // True if incr-vacuum is enabled
#endif
            public TRANS InTransaction;      // Transaction state
            //public bool DoNotUseWAL;        // If true, do not open write-ahead-log file
            public BTS BtsFlags;			// Boolean parameters.  See BTS_* macros below
            public ushort MaxLocal;         // Maximum local payload in non-LEAFDATA tables
            public ushort MinLocal;         // Minimum local payload in non-LEAFDATA tables
            public ushort MaxLeaf;          // Maximum local payload in a LEAFDATA table
            public ushort MinLeaf;          // Minimum local payload in a LEAFDATA table
            public uint PageSize;           // Total number of bytes on a page
            public uint UsableSize;         // Number of usable bytes on each page
            public int Transactions;        // Number of open transactions (read + write)
            public Pid Pages;               // Number of pages in the database
            public Schema Schema;           // Pointer to space allocated by sqlite3BtreeSchema()
            public dxFreeSchema FreeSchema; // Destructor for BtShared.pSchema
            public MutexEx Mutex;           // Non-recursive mutex required to access this object
            public Bitvec HasContent;       // Set of pages moved to free-list this transaction
#if !OMIT_SHARED_CACHE
            public int Refs;                // Number of references to this structure
            public BtShared Next;           // Next on a list of sharable BtShared structs
            public BtLock Lock;             // List of locks held on this shared-btree struct
            public Btree Writer;            // Btree with currently open write transaction
#endif
            public byte[] TmpSpace;         // BtShared.pageSize bytes of space for tmp use
        }

        public struct CellInfo
        {
            public int iCell;     // Offset to start of cell content -- Needed for C#
            public byte[] pCell;  // Pointer to the start of cell content
            public long nKey;      // The key for INTKEY tables, or number of bytes in key
            public uint nData;     // Number of bytes of data
            public uint nPayload;  // Total amount of payload
            public ushort nHeader;   // Size of the cell content header in bytes
            public ushort nLocal;    // Amount of payload held locally
            public ushort iOverflow; // Offset to overflow page number.  Zero if no overflow
            public ushort nSize;     // Size of the cell content on the main b-tree page
            public bool Equals(CellInfo ci)
            {
                if (ci.iCell >= ci.pCell.Length || iCell >= this.pCell.Length)
                    return false;
                if (ci.pCell[ci.iCell] != this.pCell[iCell])
                    return false;
                if (ci.nKey != this.nKey || ci.nData != this.nData || ci.nPayload != this.nPayload)
                    return false;
                if (ci.nHeader != this.nHeader || ci.nLocal != this.nLocal)
                    return false;
                if (ci.iOverflow != this.iOverflow || ci.nSize != this.nSize)
                    return false;
                return true;
            }
        }

        const int BTCURSOR_MAX_DEPTH = 20;

        enum CURSOR : byte
        {
            INVALID = 0,
            VALID = 1,
            REQUIRESEEK = 2,
            FAULT = 3,
        }

        public class BtCursor
        {
            public Btree Btree;            // The Btree to which this cursor belongs
            public BtShared Bt;            // The BtShared this cursor points to
            public BtCursor Next;
            public BtCursor Prev;          // Forms a linked list of all cursors
            public KeyInfo KeyInfo;        // Argument passed to comparison function
            public Pid IDRoot;            // The root page of this tree
            public long CachedRowid; // Next rowid cache.  0 means not valid
            public CellInfo Info = new CellInfo();           // A parse of the cell we are pointing at
            public byte[] pKey;             // Saved key that was cursor's last known position
            public long nKey;                // Size of pKey, or last integer key
            public int SkipNext;            // Prev() is noop if negative. Next() is noop if positive
            public byte WrFlag;               // True if writable
            public byte AtLast;               // VdbeCursor pointing to the last entry
            public bool ValidNKey;          // True if info.nKey is valid
            public CURSOR State;              // One of the CURSOR_XXX constants (see below)
#if !OMIT_INCRBLOB
            public Pid[] Overflows;         // Cache of overflow page locations
            public bool IsIncrblobHandle;   // True if this cursor is an incr. io handle
#endif
            public short PageIdx;                                          // Index of current page in apPage
            public ushort[] aiIdx = new ushort[BTCURSOR_MAX_DEPTH];           // Current index in apPage[i]
            public MemPage[] apPage = new MemPage[BTCURSOR_MAX_DEPTH]; // Pages from root to current page

            public void Clear()
            {
                Next = null;
                Prev = null;
                KeyInfo = null;
                IDRoot = 0;
                CachedRowid = 0;
                Info = new CellInfo();
                WrFlag = 0;
                AtLast = 0;
                ValidNKey = false;
                State = 0;
                pKey = null;
                nKey = 0;
                SkipNext = 0;
#if !OMIT_INCRBLOB
                IsIncrblobHandle = false;
                Overflows = null;
#endif
                PageIdx = 0;
            }
            public BtCursor Copy()
            {
                BtCursor cp = (BtCursor)MemberwiseClone();
                return cp;
            }
        }

        const int CURSOR_INVALID = 0;
        const int CURSOR_VALID = 1;
        const int CURSOR_REQUIRESEEK = 2;
        const int CURSOR_FAULT = 3;

        static uint PENDING_BYTE_PAGE(BtShared pBt)
        {
            return (uint)PAGER_MJ_PGNO(pBt.pPager);
        }

        static Pid PTRMAP_PAGENO(BtShared pBt, Pid pgno)
        {
            return ptrmapPageno(pBt, pgno);
        }
        static Pid PTRMAP_PTROFFSET(uint pgptrmap, Pid pgno)
        {
            return (5 * (pgno - pgptrmap - 1));
        }
        static bool PTRMAP_ISPAGE(BtShared pBt, Pid pgno)
        {
            return (PTRMAP_PAGENO((pBt), (pgno)) == (pgno));
        }
        const int PTRMAP_ROOTPAGE = 1;
        const int PTRMAP_FREEPAGE = 2;
        const int PTRMAP_OVERFLOW1 = 3;
        const int PTRMAP_OVERFLOW2 = 4;
        const int PTRMAP_BTREE = 5;

#if DEBUG
        static void btreeIntegrity(Btree p)
        {
            Debug.Assert(p.Bt.inTransaction != TRANS_NONE || p.Bt.nTransaction == 0);
            Debug.Assert(p.Bt.inTransaction >= p.InTrans);
        }
#else
static void btreeIntegrity(Btree p) { }
#endif

#if !OMIT_AUTOVACUUM
        //#define ISAUTOVACUUM (pBt.autoVacuum)
#else
//#define ISAUTOVACUUM 0
public static bool ISAUTOVACUUM =false;
#endif

        public class IntegrityCk
        {
            public BtShared pBt;      // The tree being checked out
            public Pager pPager;      // The associated pager.  Also accessible by pBt.pPager
            public Pid nPage;        // Number of pages in the database
            public int[] anRef;       // Number of times each page is referenced
            public int mxErr;         // Stop accumulating errors when this reaches zero
            public int nErr;          // Number of messages written to zErrMsg so far
            //public int mallocFailed;  // A memory allocation error has occurred
            public StrAccum errMsg = new StrAccum(100); // Accumulate the error message text here
        };

        static int get2byte(byte[] p, int offset)
        {
            return p[offset + 0] << 8 | p[offset + 1];
        }

        static void put2byte(byte[] pData, int Offset, uint v)
        {
            pData[Offset + 0] = (byte)(v >> 8);
            pData[Offset + 1] = (byte)v;
        }
        static void put2byte(byte[] pData, int Offset, int v)
        {
            pData[Offset + 0] = (byte)(v >> 8);
            pData[Offset + 1] = (byte)v;
        }
    }
}
