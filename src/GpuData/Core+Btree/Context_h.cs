using System;
namespace Core
{
    public class Context
    {
        const int MAX_ATTACHED = 10;

        public class BusyHandlerType
        {
            public Func<object, int, int> Func; // The busy callback
            public object Arg;                  // First arg to busy callback
            public int Busys;                   // Incremented with each busy call
        }

        public class DB
        {
            public string Name;					// Name of this database
            public Btree Bt;					// The B*Tree structure for this database file
            public byte InTrans;				// 0: not writable.  1: Transaction.  2: Checkpoint
            public byte SafetyLevel;			// How aggressive at syncing data to disk
            public ISchema Schema;			    // Pointer to database schema (possibly shared)
        }

        [Flags]
        public enum FLAG : uint
        {
            VdbeTrace = 0x00000100,
            InternChanges = 0x00000200,
            FullColNames = 0x00000400,
            ShortColNames = 0x00000800,
            CountRows = 0x00001000,
            NullCallback = 0x00002000,
            SqlTrace = 0x00004000,
            VdbeListing = 0x00008000,
            WriteSchema = 0x00010000,
            NoReadlock = 0x00020000,
            IgnoreChecks = 0x00040000,
            ReadUncommitted = 0x0080000,
            LegacyFileFmt = 0x00100000,
            FullFSync = 0x00200000,
            CkptFullFSync = 0x00400000,
            RecoveryMode = 0x00800000,
            ReverseOrder = 0x01000000,
            RecTriggers = 0x02000000,
            ForeignKeys = 0x04000000,
            AutoIndex = 0x08000000,
            PreferBuiltin = 0x10000000,
            LoadExtension = 0x20000000,
            EnableTrigger = 0x40000000,
        }

        public MutexEx Mutex;
        public FLAG Flags;
        public BusyHandlerType BusyHandler;
        public int Savepoints;                      // Number of non-transaction savepoints
        public int ActiveVdbeCnt;
        public DB[] DBs = new DB[MAX_ATTACHED];     // All backends
        public int DBsUsed;                          // Number of backends currently in use

        public int InvokeBusyHandler()
        {
            if (SysEx.NEVER(BusyHandler == null) || BusyHandler.Func == null || BusyHandler.Busys < 0)
                return 0;
            var rc = BusyHandler.Func(BusyHandler.Arg, BusyHandler.Busys);
            if (rc == 0)
                BusyHandler.Busys = -1;
            else
                BusyHandler.Busys++;
            return rc;
        }

        // HOOKS
#if ENABLE_UNLOCK_NOTIFY
        public void sqlite3ConnectionBlocked(sqlite3 *, sqlite3 );
        internal void sqlite3ConnectionUnlocked(sqlite3 db);
        internal void sqlite3ConnectionClosed(sqlite3 db);
#else
        public static void ConnectionBlocked(Context a, Context b) { }
        //internal static void sqlite3ConnectionUnlocked(sqlite3 x) { }
        //internal static void sqlite3ConnectionClosed(sqlite3 x) { }
#endif

        public bool TempInMemory()
        {
            return true;
            //if (SQLITE_TEMP_STORE == 1) return (temp_store == 2);
            //if (SQLITE_TEMP_STORE == 2) return (temp_store != 1);
            //if (SQLITE_TEMP_STORE == 3) return true;
            //if (SQLITE_TEMP_STORE < 1 || SQLITE_TEMP_STORE > 3) return false;
            //return false;
        }
    }
}