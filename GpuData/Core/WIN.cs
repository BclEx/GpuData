using System;
using System.Diagnostics;
using System.Runtime.InteropServices;
namespace Core
{
    public class WIN
    {
        internal const long ERROR_FILE_NOT_FOUND = 2L;
        internal const long ERROR_HANDLE_DISK_FULL = 39L;
        internal const long ERROR_NOT_SUPPORTED = 50L;
        internal const long ERROR_DISK_FULL = 112L;

        internal static RC _Error(RC rc, string func, string path)
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

        public static bool IsRunningMediumTrust() { return false; }

#if DEBUG
        internal static RC SQLITE_CORRUPT_BKPT()
        {
            var sf = new StackTrace(new StackFrame(true)).GetFrame(0);
            Console.WriteLine("database corruption at line {0} of [{1}]", sf.GetFileLineNumber(), sf.GetFileName());
            return RC.CORRUPT;
        }
        internal static RC SQLITE_MISUSE_BKPT()
        {
            var sf = new StackTrace(new StackFrame(true)).GetFrame(0);
            Console.WriteLine("misuse at line {0} of [{1}]", sf.GetFileLineNumber(), sf.GetFileName());
            return RC.MISUSE;
        }
        internal static RC SQLITE_CANTOPEN_BKPT()
        {
            var sf = new StackTrace(new StackFrame(true)).GetFrame(0);
            Console.WriteLine("cannot open file at line {0} of [{1}]", sf.GetFileLineNumber(), sf.GetFileName());
            return RC.CANTOPEN;
        }
#else
        internal static SQLITE SQLITE_CORRUPT_BKPT() { return SQLITE_CORRUPT; }
        internal static SQLITE SQLITE_MISUSE_BKPT() { return SQLITE_MISUSE; }
        internal static SQLITE SQLITE_CANTOPEN_BKPT() { return SQLITE_CANTOPEN; }
#endif

        public static bool ALWAYS(bool x) { if (x != true) Debug.Assert(false); return x; }
    }
}