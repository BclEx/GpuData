// btree.h
using System;
namespace Core
{
    public partial class Btree
    {
        const int SQLITE_N_BTREE_META = 10;
        const AUTOVACUUM DEFAULT_AUTOVACUUM = AUTOVACUUM.NONE;

        enum AUTOVACUUM : byte
        {
            NONE = 0,           // Do not do auto-vacuum
            FULL = 1,           // Do full auto-vacuum
            INCR = 2,           // Incremental vacuum
        }

        [Flags]
        enum OPEN : byte
        {
            OMIT_JOURNAL = 1,   // Do not create or use a rollback journal
            MEMORY = 2,         // This is an in-memory DB
            SINGLE = 4,         // The file contains at most 1 b-tree
            UNORDERED = 8,      // Use of a hash implementation is OK
        }

        const int BTREE_INTKEY = 1;
        const int BTREE_BLOBKEY = 2;

        enum META : byte
        {
            FREE_PAGE_COUNT = 0,
            SCHEMA_VERSION = 1,
            FILE_FORMAT = 2,
            DEFAULT_CACHE_SIZE = 3,
            LARGEST_ROOT_PAGE = 4,
            TEXT_ENCODING = 5,
            USER_VERSION = 6,
            INCR_VACUUM = 7,
        }

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


        //#if !OMIT_SHARED_CACHE
        //        //void sqlite3BtreeEnter(Btree);
        //        //void sqlite3BtreeEnterAll(sqlite3);
        //#else
        //    static void sqlite3BtreeEnter( Btree bt )
        //    {
        //    }
        //    static void sqlite3BtreeEnterAll( sqlite3 p )
        //    {
        //    }
        //#endif

        //#if !(OMIT_SHARED_CACHE) && THREADSAFE
        ////int sqlite3BtreeSharable(Btree);
        ////void sqlite3BtreeLeave(Btree);
        ////void sqlite3BtreeEnterCursor(BtCursor);
        ////void sqlite3BtreeLeaveCursor(BtCursor);
        ////void sqlite3BtreeLeaveAll(sqlite3);
        //#if !DEBUG
        ///* These routines are used inside Debug.Assert() statements only. */
        //int sqlite3BtreeHoldsMutex(Btree);
        //int sqlite3BtreeHoldsAllMutexes(sqlite3);
        //int sqlite3SchemaMutexHeld(sqlite3*,int,Schema);
        //#endif
        //#else
        //        static bool sqlite3BtreeSharable(Btree X)
        //        {
        //            return false;
        //        }

        //        static void sqlite3BtreeLeave(Btree X)
        //        {
        //        }

        //        static void sqlite3BtreeEnterCursor(BtCursor X)
        //        {
        //        }

        //        static void sqlite3BtreeLeaveCursor(BtCursor X)
        //        {
        //        }

        //        static void sqlite3BtreeLeaveAll(object X)
        //        {
        //        }

        //        static bool sqlite3BtreeHoldsMutex(Btree X)
        //        {
        //            return true;
        //        }

        //        static bool sqlite3BtreeHoldsAllMutexes(object X)
        //        {
        //            return true;
        //        }
        //        static bool sqlite3SchemaMutexHeld(object X, int y, Schema z)
        //        {
        //            return true;
        //        }
        //#endif
    }
}
