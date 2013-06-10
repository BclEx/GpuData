using System.Diagnostics;
namespace Core.IO
{
    public abstract class VFileSystem
    {
        internal static VFileSystem _vfsList;
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

        public VFileSystem Next;        // Next registered VFS
        public string Name = "win32";   // Name of this virtual file system
        public object Tag;              // Pointer to application-specific data
        public int SizeOsFile = -1;     // Size of subclassed VirtualFile
        public int MaxPathname = 256;   // Maximum file pathname length

        static VFileSystem()
        {
            //RegisterVfs(new CoreVFileSystem(), true);
        }

        public void CopyTo(VFileSystem ct)
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

        public static VFileSystem FindVfs(string name)
        {
            if (string.IsNullOrEmpty(name))
                return null;
            VFileSystem vfs = null;
            var mutex = MutexEx.Alloc(MutexEx.MUTEX.STATIC_MASTER);
            MutexEx.Enter(mutex);
            for (vfs = _vfsList; vfs != null && name != vfs.Name; vfs = vfs.Next) { }
            MutexEx.Leave(mutex);
            return vfs;
        }

        internal static void UnlinkVfs(VFileSystem vfs)
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

        public static RC RegisterVfs(VFileSystem vfs, bool @default)
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

        public static RC UnregisterVfs(VFileSystem vfs)
        {
            var mutex = MutexEx.Alloc(MutexEx.MUTEX.STATIC_MASTER);
            MutexEx.Enter(mutex);
            UnlinkVfs(vfs);
            MutexEx.Leave(mutex);
            return RC.OK;
        }
    }
}