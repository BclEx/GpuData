using System;
namespace Core
{
	public class WIN
	{
		internal const long ERROR_FILE_NOT_FOUND = 2L;
		internal const long ERROR_HANDLE_DISK_FULL = 39L;
		internal const long ERROR_NOT_SUPPORTED = 50L;
		internal const long ERROR_DISK_FULL = 112L;


		internal static RC winLogError(RC a, string b, string c) { var sf = new StackTrace(new StackFrame(true)).GetFrame(0); return winLogErrorAtLine(a, b, c, sf.GetFileLineNumber()); }
		private static RC winLogErrorAtLine(RC errcode, string zFunc, string zPath, int iLine)
		{
			var iErrno = (uint)Marshal.GetLastWin32Error();
			var zMsg = Marshal.GetLastWin32Error().ToString();
			Debug.Assert(errcode != RC.OK);
			if (zPath == null)
				zPath = string.Empty;
			int i;
			for (i = 0; i < zMsg.Length && zMsg[i] != '\r' && zMsg[i] != '\n'; i++) ;
			zMsg = zMsg.Substring(0, i);
			//sqlite3_log(errcode, "os_win.c:%d: (%d) %s(%s) - %s", iLine, iErrno, zFunc, zPath, zMsg);
			return errcode;
		}

		public static bool IsRunningMediumTrust() { return false; }
	}
}