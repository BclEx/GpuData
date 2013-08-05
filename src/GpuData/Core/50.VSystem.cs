using System;
using System.Diagnostics;
namespace Core.IO
{
    public abstract class VSystem
    {
        internal static VSystem _vfsList;
        internal static bool isInit = false;

        public enum OPEN : uint
        {
            READONLY = 0x00000001,          // Ok for sqlite3_open_v2() 
            READWRITE = 0x00000002,         // Ok for sqlite3_open_v2() 
            CREATE = 0x00000004,            // Ok for sqlite3_open_v2() 
            DELETEONCLOSE = 0x00000008,     // VFS only 
            EXCLUSIVE = 0x00000010,         // VFS only 
            AUTOPROXY = 0x00000020,         // VFS only 
            URI = 0x00000040,               // Ok for sqlite3_open_v2() 
            MEMORY = 0x00000080,            // Ok for sqlite3_open_v2()
            MAIN_DB = 0x00000100,           // VFS only 
            TEMP_DB = 0x00000200,           // VFS only 
            TRANSIENT_DB = 0x00000400,      // VFS only 
            MAIN_JOURNAL = 0x00000800,      // VFS only 
            TEMP_JOURNAL = 0x00001000,      // VFS only 
            SUBJOURNAL = 0x00002000,        // VFS only 
            MASTER_JOURNAL = 0x00004000,    // VFS only 
            NOMUTEX = 0x00008000,           // Ok for sqlite3_open_v2() 
            FULLMUTEX = 0x00010000,         // Ok for sqlite3_open_v2() 
            SHAREDCACHE = 0x00020000,       // Ok for sqlite3_open_v2() 
            PRIVATECACHE = 0x00040000,      // Ok for sqlite3_open_v2() 
            WAL = 0x00080000,               // VFS only 
        }

        public enum ACCESS : byte
        {
            EXISTS = 0,
            READWRITE = 1,  // Used by PRAGMA temp_store_directory
            READ = 2,       // Unused
        }

        public VSystem Next;        // Next registered VFS
        public string Name = "win32";   // Name of this virtual file system
        public object Tag;              // Pointer to application-specific data
        public int SizeOsFile = -1;     // Size of subclassed VirtualFile
        public int MaxPathname = 256;   // Maximum file pathname length

        static VSystem()
        {
            RegisterVfs(new WinVSystem(), true);
        }

        public void CopyTo(VSystem ct)
        {
            ct.SizeOsFile = this.SizeOsFile;
            ct.MaxPathname = this.MaxPathname;
            ct.Next = this.Next;
            ct.Name = this.Name;
            ct.Tag = this.Tag;
        }

        public abstract RC Open(string path, VFile file, OPEN flags, out OPEN outFlags);
        public abstract RC Delete(string path, bool syncDirectory);
        public abstract RC Access(string path, ACCESS flags, out int outRC);
        public abstract RC FullPathname(string path, out string outPath);

        public static VSystem FindVfs(string name)
        {
            if (string.IsNullOrEmpty(name))
                return null;
            VSystem vfs = null;
            var mutex = MutexEx.Alloc(MutexEx.MUTEX.STATIC_MASTER);
            MutexEx.Enter(mutex);
            for (vfs = _vfsList; vfs != null && name != vfs.Name; vfs = vfs.Next) { }
            MutexEx.Leave(mutex);
            return vfs;
        }

        internal static void UnlinkVfs(VSystem vfs)
        {
            Debug.Assert(MutexEx.Held(MutexEx.Alloc(MutexEx.MUTEX.STATIC_MASTER)));
            if (vfs == null) { }
            else if (_vfsList == vfs)
                _vfsList = vfs.Next;
            else if (_vfsList != null)
            {
                var p = _vfsList;
                while (p.Next != null && p.Next != vfs)
                    p = p.Next;
                if (p.Next == vfs)
                    p.Next = vfs.Next;
            }
        }

        public static RC RegisterVfs(VSystem vfs, bool @default)
        {
            var mutex = MutexEx.Alloc(MutexEx.MUTEX.STATIC_MASTER);
            MutexEx.Enter(mutex);
            UnlinkVfs(vfs);
            if (@default || _vfsList == null)
            {
                vfs.Next = _vfsList;
                _vfsList = vfs;
            }
            else
            {
                vfs.Next = _vfsList.Next;
                _vfsList.Next = vfs;
            }
            Debug.Assert(_vfsList != null);
            MutexEx.Leave(mutex);
            return RC.OK;
        }

        public static RC UnregisterVfs(VSystem vfs)
        {
            var mutex = MutexEx.Alloc(MutexEx.MUTEX.STATIC_MASTER);
            MutexEx.Enter(mutex);
            UnlinkVfs(vfs);
            MutexEx.Leave(mutex);
            return RC.OK;
        }


#if TEST || DEBUG
        static bool OsTrace = false;
        protected static void OSTRACE(string x, params object[] args) { if (OsTrace) Console.WriteLine("b:" + string.Format(x, args)); }
#else
        protected static void OSTRACE(string x, params object[] args) { }
#endif

#if TEST
        static int io_error_hit = 0;            // Total number of I/O Errors
        static int io_error_hardhit = 0;        // Number of non-benign errors
        static int io_error_pending = 0;        // Count down to first I/O error
        static bool io_error_persist = false;   // True if I/O errors persist
        static bool io_error_benign = false;    // True if errors are benign
        static int diskfull_pending = 0;
        static bool diskfull = false;
        protected static void SimulateIOErrorBenign(bool X) { io_error_benign = X; }
        protected static bool SimulateIOError() { if ((io_error_persist && io_error_hit > 0) || io_error_pending-- == 1) { local_ioerr(); return true; } return false; }
        protected static void local_ioerr() { OSTRACE("IOERR\n"); io_error_hit++; if (!io_error_benign) io_error_hardhit++; }
        protected static bool SimulateDiskfullError() { if (diskfull_pending > 0) { if (diskfull_pending == 1) { local_ioerr(); diskfull = true; io_error_hit = 1; return true; } else diskfull_pending--; } return false; }
#else
        protected static void SimulateIOErrorBenign(bool X) { }
        protected static bool SimulateIOError() { return false; }
        protected static bool SimulateDiskfullError() { return false; }
#endif

        // When testing, keep a count of the number of open files.
#if TEST
        static int open_file_count = 0;
        protected static void OpenCounter(int X) { open_file_count += X; }
#else
        protected static void OpenCounter(int X) { }
#endif
    }
}