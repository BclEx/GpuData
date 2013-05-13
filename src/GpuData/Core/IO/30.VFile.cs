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

        [Flags]
        public enum SYNC : byte
        {
            NORMAL = 0x00002,
            FULL = 0x00003,
            DATAONLY = 0x00010,
        }

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
            DB_UNCHANGED = 0xca093fa0,
        }

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
        }

        protected ulong _sectorSize;        // Sector size of the device file is on
        public bool Open;
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

        public abstract RC Close();
        public abstract RC Read(byte[] buffer, int amount, long offset);
        public abstract RC Write(byte[] buffer, int amount, long offset);
        public abstract RC Truncate(long size);
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

        public virtual IOCAP DeviceCharacteristics
        {
            get { return 0; }
        }

        public virtual RC ShmLock(int offset, int n, int flags) { return RC.OK; }
        public virtual void ShmBarrier() { }
        public virtual RC ShmUnmap(int deleteFlag) { return RC.OK; }
        public virtual RC ShmMap(int iPg, int pgsz, int pInt, out object pvolatile) { pvolatile = null; return RC.OK; }

        // Read a 32-bit integer from the given file descriptor.  Store the integer that is read in pRes.  Return SQLITE.OK if everything worked, or an
        // error code is something goes wrong.
        // All values are stored on disk as big-endian.
        public RC ReadByte(int offset, ref int pRes)
        {
            uint u32_pRes = 0;
            var rc = ReadByte(offset, ref u32_pRes);
            pRes = (int)u32_pRes;
            return rc;
        }
        public RC ReadByte(long offset, ref uint pRes) { return ReadByte((int)offset, ref pRes); }
        public RC ReadByte(int offset, ref uint pRes)
        {
            var ac = new byte[4];
            var rc = Read(ac, ac.Length, offset);
            pRes = (rc == RC.OK ? ConvertEx.Get4(ac) : 0);
            return rc;
        }

        // Write a 32-bit integer into the given file descriptor.  Return SQLITE.OK on success or an error code is something goes wrong.
        public RC WriteByte(long offset, uint val)
        {
            var ac = new byte[4];
            ConvertEx.Put4(ac, val);
            return Write(ac, 4, offset);
        }
    }
}