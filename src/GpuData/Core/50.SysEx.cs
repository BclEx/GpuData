using System;
using System.Diagnostics;
using System.Runtime.InteropServices;
namespace Core
{
    public class SysEx
    {
        internal static RC OSError(RC rc, string func, string path)
        {
            var sf = new StackTrace(new StackFrame(true)).GetFrame(0);
            var errorID = (uint)Marshal.GetLastWin32Error();
            var message = Marshal.GetLastWin32Error().ToString();
            Debug.Assert(rc != RC.OK);
            if (path == null)
                path = string.Empty;
            int i;
            for (i = 0; i < message.Length && message[i] != '\r' && message[i] != '\n'; i++) ;
            message = message.Substring(0, i);
            //sqlite3_log("os_win.c:%d: (%d) %s(%s) - %s", sf.GetFileLineNumber(), errorID, func, sf.GetFileName(), message);
            return rc;
        }


//#if DEBUG || TRACE
//        internal static bool OSTrace = false;
//        internal static void OSTRACE(string x, params object[] args) { if (OSTrace) Console.WriteLine("a:" + string.Format(x, args)); }
//#else
//        internal static void OSTRACE(string x, params object[] args) { }
//#endif

//#if IOTRACE
//        internal static bool IOTrace = true;
//        public static void IOTRACE(string x, params object[] args) { if (IOTrace) Console.WriteLine("i:" + string.Format(x, args)); }
//#else
//        public static void IOTRACE(string x, params object[] args) { }
//#endif




        [Flags]
        public enum MEMTYPE : byte
        {
            HEAP = 0x01,         // General heap allocations
            LOOKASIDE = 0x02,    // Might have been lookaside memory
            SCRATCH = 0x04,      // Scratch allocations
            PCACHE = 0x08,       // Page cache allocations
            DB = 0x10,           // Uses sqlite3DbMalloc, not sqlite_malloc
        }
        public static void BeginBenignMalloc() { }
        public static void EndBenignMalloc() { }
        public static byte[] Alloc(int size) { return new byte[size]; }
        public static byte[] Alloc(int size, bool clear) { return new byte[size]; }
        public static int AllocSize(byte[] p)
        {
            Debug.Assert(MemdebugHasType(p, MEMTYPE.HEAP));
            Debug.Assert(MemdebugNoType(p, MEMTYPE.DB));
            return p.Length;
        }
        public static void Free(ref byte[] p) { p = null; }
        public static byte[] StackAlloc(int size) { return new byte[size]; }
        public static void StackFree(ref byte[] p) { p = null; }
        public static bool HeapNearlyFull() { return false; }
        //
#if MEMDEBUG
        //public static void MemdebugSetType<T>(T X, MEMTYPE Y);
        //public static bool MemdebugHasType<T>(T X, MEMTYPE Y);
        //public static bool MemdebugNoType<T>(T X, MEMTYPE Y);
#else
        public static void MemdebugSetType<T>(T X, MEMTYPE Y) { }
        public static bool MemdebugHasType<T>(T X, MEMTYPE Y) { return true; }
        public static bool MemdebugNoType<T>(T X, MEMTYPE Y) { return true; }
#endif

        ///////////////////

        public static bool ALWAYS(bool x) { if (x != true) Debug.Assert(false); return x; }
        public static bool NEVER(bool x) { return x; }

        public static int ROUND8(int x) { return (x + 7) & ~7; }
        public static int ROUNDDOWN8(int x) { return x & ~7; }

#if BYTEALIGNED4
        public static bool HASALIGNMENT8(int x) { return true; }
#else
        public static bool HASALIGNMENT8(int x) { return true; }
#endif

#if DEBUG
        internal static RC CORRUPT_BKPT()
        {
            var sf = new StackTrace(new StackFrame(true)).GetFrame(0);
            Console.WriteLine("database corruption at line {0} of [{1}]", sf.GetFileLineNumber(), sf.GetFileName());
            return RC.CORRUPT;
        }
        internal static RC MISUSE_BKPT()
        {
            var sf = new StackTrace(new StackFrame(true)).GetFrame(0);
            Console.WriteLine("misuse at line {0} of [{1}]", sf.GetFileLineNumber(), sf.GetFileName());
            return RC.MISUSE;
        }
        internal static RC CANTOPEN_BKPT()
        {
            var sf = new StackTrace(new StackFrame(true)).GetFrame(0);
            Console.WriteLine("cannot open file at line {0} of [{1}]", sf.GetFileLineNumber(), sf.GetFileName());
            return RC.CANTOPEN;
        }
#else
        internal static RC CORRUPT_BKPT() { return SQLITE_CORRUPT; }
        internal static RC MISUSE_BKPT() { return SQLITE_MISUSE; }
        internal static RC CANTOPEN_BKPT() { return SQLITE_CANTOPEN; }
#endif


    }
}