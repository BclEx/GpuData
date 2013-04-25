using System;
using System.Diagnostics;
using System.IO;
using System.Runtime.InteropServices;
using System.Threading;
namespace Core.IO
{
    public class CoreVFileSystem : VFileSystem
    {
        const int SQLITE_DEFAULT_SECTOR_SIZE = 512;
        const int MAX_PATH = 260;
        static int MX_DELETION_ATTEMPTS = 5;

        //public CoreVFileSystem() { }
        //public CoreVFileSystem(int szOsFile, int mxPathname, VFileSystem pNext, string zName, object pAppData)
        //{
        //    this.szOsFile = szOsFile;
        //    this.mxPathname = mxPathname;
        //    this.Next = pNext;
        //    this.Name = zName;
        //    this.AppData = pAppData;
        //}

        public override RC Open(string path, VFile file, OPEN flags, out OPEN outFlags)
        {
            outFlags = 0;
            // if argument zPath is a NULL pointer, this function is required to open a temporary file. Use this buffer to store the file name in.
            //var zTmpname = new StringBuilder(MAX_PATH + 1);        // Buffer used to create temp filename
            var rc = RC.OK;
            var type = (OPEN)((int)flags & 0xFFFFFF00);  // Type of file to open
            var exclusive = (flags & OPEN.EXCLUSIVE) != 0;
            var delete = (flags & OPEN.DELETEONCLOSE) != 0;
            var create = (flags & OPEN.CREATE) != 0;
            var readOnly = (flags & OPEN.READONLY) != 0;
            var readWrite = (flags & OPEN.READWRITE) != 0;
            var openJournal = (create && (type == OPEN.MASTER_JOURNAL || type == OPEN.MAIN_JOURNAL || type == OPEN.WAL));
            // Check the following statements are true:
            //   (a) Exactly one of the READWRITE and READONLY flags must be set, and
            //   (b) if CREATE is set, then READWRITE must also be set, and
            //   (c) if EXCLUSIVE is set, then CREATE must also be set.
            //   (d) if DELETEONCLOSE is set, then CREATE must also be set.
            Debug.Assert((!readOnly || !readWrite) && (readWrite || readOnly));
            Debug.Assert(!create || readWrite);
            Debug.Assert(!exclusive || create);
            Debug.Assert(!delete || create);
            // The main DB, main journal, WAL file and master journal are never automatically deleted. Nor are they ever temporary files.
            //Debug.Assert((!isDelete && !string.IsNullOrEmpty(zName)) || eType != OPEN.MAIN_DB);
            Debug.Assert((!delete && !string.IsNullOrEmpty(path)) || type != OPEN.MAIN_JOURNAL);
            Debug.Assert((!delete && !string.IsNullOrEmpty(path)) || type != OPEN.MASTER_JOURNAL);
            Debug.Assert((!delete && !string.IsNullOrEmpty(path)) || type != OPEN.WAL);
            // Assert that the upper layer has set one of the "file-type" flags.
            Debug.Assert(type == OPEN.MAIN_DB || type == OPEN.TEMP_DB || type == OPEN.MAIN_JOURNAL || type == OPEN.TEMP_JOURNAL ||
                         type == OPEN.SUBJOURNAL || type == OPEN.MASTER_JOURNAL || type == OPEN.TRANSIENT_DB || type == OPEN.WAL);
            file.S = null;
            // If the second argument to this function is NULL, generate a temporary file name to use
            if (string.IsNullOrEmpty(path))
            {
                Debug.Assert(delete && !openJournal);
                path = Path.GetRandomFileName();
            }
            // Convert the filename to the system encoding.
            if (path.StartsWith("/") && !path.StartsWith("//"))
                path = path.Substring(1);
            var desiredAccess = (readWrite ? FileAccess.Read | FileAccess.Write : FileAccess.Read);
            // SQLITE_OPEN_EXCLUSIVE is used to make sure that a new file is created. SQLite doesn't use it to indicate "exclusive access" as it is usually understood.
            FileMode creationDisposition;
            if (exclusive)
                // Creates a new file, only if it does not already exist. */ If the file exists, it fails.
                creationDisposition = FileMode.CreateNew;
            else if (create)
                // Open existing file, or create if it doesn't exist
                creationDisposition = FileMode.OpenOrCreate;
            else
                // Opens a file, only if it exists.
                creationDisposition = FileMode.Open;
            var shareMode = FileShare.Read | FileShare.Write;
            FileOptions flagsAndAttributes;
            if (delete)
                flagsAndAttributes = FileOptions.DeleteOnClose;
            else
                flagsAndAttributes = FileOptions.None;
            // Reports from the internet are that performance is always better if FILE_FLAG_RANDOM_ACCESS is used.
            FileStream fs = null;
            if (Environment.OSVersion.Platform >= PlatformID.Win32NT)
            {
                // retry opening the file a few times; this is because of a racing condition between a delete and open call to the FS
                var retries = 3;
                while (fs == null && retries > 0)
                    try
                    {
                        retries--;
                        fs = new FileStream(path, creationDisposition, desiredAccess, shareMode, 4096, flagsAndAttributes);
                        Console.WriteLine("OPEN {0} ({1})", fs.GetHashCode(), fs.Name);
                    }
                    catch (Exception) { Thread.Sleep(100); }
            }
            Console.WriteLine("OPEN {0} {1} 0x{2:x} {3}", file.GetHashCode(), path, desiredAccess, fs == null ? "failed" : "ok");
            if (fs == null || fs.SafeFileHandle.IsInvalid)
            {
                file.LastErrorID = (uint)Marshal.GetLastWin32Error();
                SysEx._Error(RC.CANTOPEN, "winOpen", path);
                return (readWrite ? Open(path, file, ((flags | OPEN.READONLY) & ~(OPEN.CREATE | OPEN.READWRITE)), out outFlags) : SysEx.SQLITE_CANTOPEN_BKPT());
            }
            outFlags = (readWrite ? OPEN.READWRITE : OPEN.READONLY);
            file.Clear();
            file.Open = true;
            file.S = fs;
            file.LastErrorID = 0;
            file.Vfs = this;
            file.Shm = null;
            file.Path = path;
            file.SectorSize = (uint)getSectorSize(path);
            return rc;
        }

        private ulong getSectorSize(string name) { return SQLITE_DEFAULT_SECTOR_SIZE; }

        public override RC Delete(string path, bool syncDirectory)
        {
            int cnt = 0;
            int error;
            RC rc;
                do
                {
                    if (!File.Exists(path)) { rc = RC.IOERR; break; }
                    try { File.Delete(path); rc = RC.OK; }
                    catch (IOException) { rc = RC.IOERR; Thread.Sleep(100); }
                } while (rc != RC.OK && ++cnt < MX_DELETION_ATTEMPTS);
            Console.WriteLine("DELETE \"{0}\"", path);
            if (rc == RC.OK)
                return rc;
            error = Marshal.GetLastWin32Error();
            return (rc == RC.INVALID && error == SysEx.ERROR_FILE_NOT_FOUND ? RC.OK : SysEx._Error(RC.IOERR_DELETE, "winDelete", path));
        }

        public override RC Access(string path, ACCESS flags, out int outRC)
        {
            var rc = RC.OK;
            // Do a quick test to prevent the try/catch block
            if (flags == ACCESS.EXISTS)
            {
                outRC = (File.Exists(path) ? 1 : 0);
                return RC.OK;
            }
            FileAttributes attr = 0;
            try
            {
                attr = File.GetAttributes(path);
                if (attr == FileAttributes.Directory)
                    try
                    {
                        var name2 = Path.Combine(Path.GetTempPath(), Path.GetTempFileName());
                        File.Create(name2).Close();
                        File.Delete(name2);
                        attr = FileAttributes.Normal;
                    }
                    catch (IOException) { attr = FileAttributes.ReadOnly; }
            }
            catch (IOException) { SysEx._Error(RC.IOERR_ACCESS, "winAccess", path); }
            switch (flags)
            {
                case ACCESS.READ:
                case ACCESS.EXISTS: rc = (attr != 0 ? RC.ERROR : RC.OK); break;
                case ACCESS.READWRITE: rc = (attr == 0 ? RC.OK : (attr & FileAttributes.ReadOnly) != 0 ? RC.OK : RC.ERROR); break;
                default: Debug.Assert(false); rc = RC.OK; break;
            }
            outRC = (int)rc;
            return RC.OK;
        }

        public override RC FullPathname(string path, out string outPath)
        {
            if (path[0] == '/' && Char.IsLetter(path[1]) && path[2] == ':')
                path = path.Substring(1);
            try { outPath = Path.GetFullPath(path); }
            catch (Exception) { outPath = path; }
            return RC.OK;
        }

        public static int Randomness(int bufferLength, byte[] buffer)
        {
            var b = BitConverter.GetBytes(DateTime.Now.Ticks);
            buffer[0] = b[0];
            buffer[1] = b[1];
            buffer[2] = b[2];
            buffer[3] = b[3];
            var n = 16;
            if (sizeof(ulong) <= bufferLength - n)
            {
                var processId = (uint)Process.GetCurrentProcess().Id;
                ConvertEx.Put4(buffer, n, processId);
                n += 4;
            }
            if (sizeof(ulong) <= bufferLength - n)
            {
                var i = (uint)new DateTime().Ticks;
                ConvertEx.Put4(buffer, n, i);
                n += 4;
            }
            if (sizeof(long) <= bufferLength - n)
            {
                long i = DateTime.UtcNow.Millisecond;
                ConvertEx.Put4(buffer, n, (uint)(i & 0xFFFFFFFF));
                ConvertEx.Put4(buffer, n, (uint)(i >> 32));
                n += sizeof(long);
            }
            return n;
        }

        public static int Sleep(int microseconds)
        {
            var millisecondsTimeout = ((microseconds + 999) / 1000);
            Thread.Sleep(millisecondsTimeout);
            return millisecondsTimeout * 1000;
        }

        public static RC CurrentTime(ref double currenttime)
        {
            long r = 0;
            var rc = CurrentTimeInt64(ref r);
            if (rc == RC.OK)
                currenttime = r / 86400000.0;
            return rc;
        }

        public static RC GetLastError(int bufferLength, ref string buffer)
        {
            buffer = Marshal.GetLastWin32Error().ToString();
            return RC.OK;
        }

        public static RC CurrentTimeInt64(ref long time)
        {
            const long winFiletimeEpoch = 23058135 * (long)8640000;
            time = winFiletimeEpoch + DateTime.UtcNow.ToFileTimeUtc() / (long)10000;
            return RC.OK;
        }
    }
}