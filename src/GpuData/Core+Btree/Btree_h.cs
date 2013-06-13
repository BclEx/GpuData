// btree.h
namespace Core
{
    public partial class Sqlite3
    {
        const int SQLITE_N_BTREE_META = 10;

        const int DEFAULT_AUTOVACUUM = 0;

        const int BTREE_AUTOVACUUM_NONE = 0;        // Do not do auto-vacuum
        const int BTREE_AUTOVACUUM_FULL = 1;        // Do full auto-vacuum
        const int BTREE_AUTOVACUUM_INCR = 2;        // Incremental vacuum

        const int BTREE_OMIT_JOURNAL = 1; // Do not create or use a rollback journal
        const int BTREE_NO_READLOCK = 2;  // Omit readlocks on readonly files
        const int BTREE_MEMORY = 4;       // This is an in-memory DB
        const int BTREE_SINGLE = 8;       // The file contains at most 1 b-tree
        const int BTREE_UNORDERED = 16;   // Use of a hash implementation is OK

        const int BTREE_INTKEY = 1;
        const int BTREE_BLOBKEY = 2;
        const int BTREE_FREE_PAGE_COUNT = 0;
        const int BTREE_SCHEMA_VERSION = 1;
        const int BTREE_FILE_FORMAT = 2;
        const int BTREE_DEFAULT_CACHE_SIZE = 3;
        const int BTREE_LARGEST_ROOT_PAGE = 4;
        const int BTREE_TEXT_ENCODING = 5;
        const int BTREE_USER_VERSION = 6;
        const int BTREE_INCR_VACUUM = 7;

#if !OMIT_SHARED_CACHE
        //void sqlite3BtreeEnter(Btree);
        //void sqlite3BtreeEnterAll(sqlite3);
#else
    static void sqlite3BtreeEnter( Btree bt )
    {
    }
    static void sqlite3BtreeEnterAll( sqlite3 p )
    {
    }
#endif

#if !(OMIT_SHARED_CACHE) && THREADSAFE
//int sqlite3BtreeSharable(Btree);
//void sqlite3BtreeLeave(Btree);
//void sqlite3BtreeEnterCursor(BtCursor);
//void sqlite3BtreeLeaveCursor(BtCursor);
//void sqlite3BtreeLeaveAll(sqlite3);
#if !DEBUG
/* These routines are used inside Debug.Assert() statements only. */
int sqlite3BtreeHoldsMutex(Btree);
int sqlite3BtreeHoldsAllMutexes(sqlite3);
int sqlite3SchemaMutexHeld(sqlite3*,int,Schema);
#endif
#else
        static bool sqlite3BtreeSharable(Btree X)
        {
            return false;
        }

        static void sqlite3BtreeLeave(Btree X)
        {
        }

        static void sqlite3BtreeEnterCursor(BtCursor X)
        {
        }

        static void sqlite3BtreeLeaveCursor(BtCursor X)
        {
        }

        static void sqlite3BtreeLeaveAll(object X)
        {
        }

        static bool sqlite3BtreeHoldsMutex(Btree X)
        {
            return true;
        }

        static bool sqlite3BtreeHoldsAllMutexes(object X)
        {
            return true;
        }
        static bool sqlite3SchemaMutexHeld(object X, int y, Schema z)
        {
            return true;
        }
#endif
    }
}
