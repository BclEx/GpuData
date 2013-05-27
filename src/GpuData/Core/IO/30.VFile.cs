using System;
using System.IO;
namespace Core.IO
{
    public abstract class VFile
    {
        public static int PENDING_BYTE = 0x40000000;

        public enum LOCK : byte
        {
            NO = 0,
            SHARED = 1,
            RESERVED = 2,
            PENDING = 3,
            EXCLUSIVE = 4,
            UNKNOWN = 5,
        }

        // sqlite3.h
        [Flags]
        public enum SYNC : byte
        {
            NORMAL = 0x00002,
            FULL = 0x00003,
            DATAONLY = 0x00010,
        }

        // sqlite3.h
        public enum FCNTL : uint
        {
            LOCKSTATE = 1,
            GET_LOCKPROXYFILE = 2,
            SET_LOCKPROXYFILE = 3,
            LAST_ERRNO = 4,
            SIZE_HINT = 5,
            CHUNK_SIZE = 6,
            FILE_POINTER = 7,
            SYNC_OMITTED = 8,
            WIN32_AV_RETRY = 9,
            PERSIST_WAL = 10,
            OVERWRITE = 11,
            VFSNAME = 12,
            POWERSAFE_OVERWRITE = 13,
            PRAGMA = 14,
            BUSYHANDLER = 15,
            TEMPFILENAME = 16,
            MMAP_SIZE = 18,
            // os.h
            DB_UNCHANGED = 0xca093fa0,
        }

        // sqlite3.h
        [Flags]
        public enum IOCAP : uint
        {
            ATOMIC = 0x00000001,
            ATOMIC512 = 0x00000002,
            ATOMIC1K = 0x00000004,
            ATOMIC2K = 0x00000008,
            ATOMIC4K = 0x00000010,
            ATOMIC8K = 0x00000020,
            ATOMIC16K = 0x00000040,
            ATOMIC32K = 0x00000080,
            ATOMIC64K = 0x00000100,
            SAFE_APPEND = 0x00000200,
            SEQUENTIAL = 0x00000400,
            UNDELETABLE_WHEN_OPEN = 0x00000800,
            POWERSAFE_OVERWRITE = 0x00001000,
        }

        protected ulong _sectorSize;        // Sector size of the device file is on
        public bool Opened;
        public VFileSystem Vfs;        // The VFS used to open this file
        public FileStream S;           // Filestream access to this file
        // public HANDLE H;             // Handle for accessing the file
        public LOCK LockType;            // Type of lock currently held on this file
        public int SharedLockByte;      // Randomly chosen byte used as a shared lock
        public ulong LastErrorID;         // The Windows errno from the last I/O error
        public object Shm;             // DUMMY Instance of shared memory on this file
        public string Path;            // Full pathname of this file
        public int Chunk;             // Chunk size configured by FCNTL_CHUNK_SIZE

        public void Clear()
        {
            S = null;
            LockType = 0;
            SharedLockByte = 0;
            LastErrorID = 0;
            _sectorSize = 0;
        }

        public abstract RC Read(byte[] buffer, int amount, long offset);
        public abstract RC Write(byte[] buffer, int amount, long offset);
        public abstract RC Truncate(long size);
        public abstract RC Close();
        public abstract RC Sync(SYNC flags);
        public abstract RC get_FileSize(out long size);
        public virtual RC Lock(LOCK lockType) { return RC.OK; }
        public virtual RC Unlock(LOCK lockType) { return RC.OK; }
        public virtual RC CheckReservedLock(ref int outRC) { return RC.OK; }
        public virtual RC FileControl(FCNTL op, ref long arg) { return RC.NOTFOUND; }

        public virtual uint SectorSize
        {
            get { return (uint)_sectorSize; }
            set { _sectorSize = value; }
        }

        public virtual IOCAP get_DeviceCharacteristics()
        {
            return 0;
        }

        public virtual RC ShmLock(int offset, int n, int flags) { return RC.OK; }
        public virtual void ShmBarrier() { }
        public virtual RC ShmUnmap(int deleteFlag) { return RC.OK; }
        public virtual RC ShmMap(int iPg, int pgsz, int pInt, out object pvolatile) { pvolatile = null; return RC.OK; }

        public RC Read4(int offset, out int valueOut)
        {
            uint u32_pRes = 0;
            var rc = Read4(offset, out u32_pRes);
            valueOut = (int)u32_pRes;
            return rc;
        }
        public RC Read4(long offset, out uint valueOut) { return Read4((int)offset, out valueOut); }
        public RC Read4(int offset, out uint valueOut)
        {
            var b = new byte[4];
            var rc = Read(b, b.Length, offset);
            valueOut = (rc == RC.OK ? ConvertEx.Get4(b) : 0);
            return rc;
        }

        public RC Write4(long offset, uint val)
        {
            var ac = new byte[4];
            ConvertEx.Put4(ac, val);
            return Write(ac, 4, offset);
        }
    }
}