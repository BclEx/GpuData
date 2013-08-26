using System.Diagnostics;
namespace Core
{
    public class StatusEx
    {
        public enum STATUS
        {
            MEMORY_USED = 0,
            PAGECACHE_USED = 1,
            PAGECACHE_OVERFLOW = 2,
            SCRATCH_USED = 3,
            SCRATCH_OVERFLOW = 4,
            MALLOC_SIZE = 5,
            PARSER_STACK = 6,
            PAGECACHE_SIZE = 7,
            SCRATCH_SIZE = 8,
            MALLOC_COUNT = 9,
        }

        public class StatType
        {
            public int[] nowValue = new int[10];        // Current value
            public int[] mxValue = new int[10];         // Maximum value
        }

        public static StatType Stat = new StatType();

        internal static int StatusValue(STATUS op)
        {
            Debug.Assert(op >= 0 && (int)op < Stat.nowValue.Length);
            return Stat.nowValue[(int)op];
        }

        internal static void StatusAdd(STATUS op, int N)
        {
            Debug.Assert(op >= 0 && (int)op < Stat.nowValue.Length);
            Stat.nowValue[(int)op] += N;
            if (Stat.nowValue[(int)op] > Stat.mxValue[(int)op])
                Stat.mxValue[(int)op] = Stat.nowValue[(int)op];
        }

        internal static void StatusSet(STATUS op, int X)
        {
            Debug.Assert(op >= 0 && (int)op < Stat.nowValue.Length);
            Stat.nowValue[(int)op] = X;
            if (Stat.nowValue[(int)op] > Stat.mxValue[(int)op])
                Stat.mxValue[(int)op] = Stat.nowValue[(int)op];
        }

        public static RC Status(STATUS op, ref int pCurrent, ref int pHighwater, int resetFlag)
        {
            if (op < 0 || (int)op >= Stat.nowValue.Length)
                return SysEx.MISUSE_BKPT();
            pCurrent = Stat.nowValue[(int)op];
            pHighwater = Stat.mxValue[(int)op];
            if (resetFlag != 0)
                Stat.mxValue[(int)op] = Stat.nowValue[(int)op];
            return RC.OK;
        }

        //public static SQLITE sqlite3_db_status(sqlite3 db, int op, ref int pCurrent, ref int pHighwater, int resetFlag)
        //{
        //    var rc = SQLITE.OK;
        //    MutexEx.sqlite3_mutex_enter(db.mutex);
        //    switch (op)
        //    {
        //        case SQLITE_DBSTATUS_LOOKASIDE_USED:
        //            {
        //                pCurrent = db.lookaside.nOut;
        //                pHighwater = db.lookaside.mxOut;
        //                if (resetFlag != 0)
        //                    db.lookaside.mxOut = db.lookaside.nOut;
        //                break;
        //            }
        //        case SQLITE_DBSTATUS_LOOKASIDE_HIT:
        //        case SQLITE_DBSTATUS_LOOKASIDE_MISS_SIZE:
        //        case SQLITE_DBSTATUS_LOOKASIDE_MISS_FULL:
        //            {
        //                Debug.Assert((op - SQLITE_DBSTATUS_LOOKASIDE_HIT) >= 0);
        //                Debug.Assert((op - SQLITE_DBSTATUS_LOOKASIDE_HIT) < 3);
        //                pCurrent = 0;
        //                pHighwater = db.lookaside.anStat[op - SQLITE_DBSTATUS_LOOKASIDE_HIT];
        //                if (resetFlag != 0)
        //                    db.lookaside.anStat[op - SQLITE_DBSTATUS_LOOKASIDE_HIT] = 0;
        //                break;
        //            }
        //        // Return an approximation for the amount of memory currently used by all pagers associated with the given database connection.  The
        //        // highwater mark is meaningless and is returned as zero.
        //        case SQLITE_DBSTATUS_CACHE_USED:
        //            {
        //                int totalUsed = 0;
        //                sqlite3BtreeEnterAll(db);
        //                for (var i = 0; i < db.nDb; i++)
        //                {
        //                    var pBt = db.aDb[i].pBt;
        //                    if (pBt != null)
        //                    {
        //                        Pager pPager = sqlite3BtreePager(pBt);
        //                        totalUsed += sqlite3PagerMemUsed(pPager);
        //                    }
        //                }
        //                sqlite3BtreeLeaveAll(db);
        //                pCurrent = totalUsed;
        //                pHighwater = 0;
        //                break;
        //            }
        //        // *pCurrent gets an accurate estimate of the amount of memory used to store the schema for all databases (main, temp, and any ATTACHed
        //        // databases.  *pHighwater is set to zero.
        //        case SQLITE_DBSTATUS_SCHEMA_USED:
        //            {
        //                int nByte = 0;              // Used to accumulate return value
        //                sqlite3BtreeEnterAll(db);
        //                for (var i = 0; i < db.nDb; i++)
        //                {
        //                    var pSchema = db.aDb[i].pSchema;
        //                    if (Check.ALWAYS(pSchema != null))
        //                    {
        //                        HashElem p;
        //                        for (p = sqliteHashFirst(pSchema.trigHash); p != null; p = sqliteHashNext(p))
        //                        {
        //                            var t = (ITrigger)sqliteHashData(p);
        //                            sqlite3DeleteTrigger(db, ref t);
        //                        }
        //                        for (p = sqliteHashFirst(pSchema.tblHash); p != null; p = sqliteHashNext(p))
        //                        {
        //                            var t = (ITable)sqliteHashData(p);
        //                            sqlite3DeleteTable(db, ref t);
        //                        }
        //                    }
        //                }
        //                db.pnBytesFreed = 0;
        //                sqlite3BtreeLeaveAll(db);
        //                pHighwater = 0;
        //                pCurrent = nByte;
        //                break;
        //            }
        //        // *pCurrent gets an accurate estimate of the amount of memory used to store all prepared statements.
        //        // *pHighwater is set to zero.
        //        case SQLITE_DBSTATUS_STMT_USED:
        //            {
        //                int nByte = 0;              // Used to accumulate return value
        //                for (var pVdbe = db.pVdbe; pVdbe != null; pVdbe = pVdbe.pNext)
        //                    sqlite3VdbeDeleteObject(db, ref pVdbe);
        //                db.pnBytesFreed = 0;
        //                pHighwater = 0;
        //                pCurrent = nByte;
        //                break;
        //            }

        //        default:
        //            {
        //                rc = SQLITE.ERROR;
        //                break;
        //            }
        //    }
        //    MutexEx.sqlite3_mutex_leave(db.mutex);
        //    return rc;
        //}
    }
}
