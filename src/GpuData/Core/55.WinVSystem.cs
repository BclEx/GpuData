using System;
using System.Diagnostics;
using System.IO;
using System.Runtime.InteropServices;
using System.Text;
using System.Threading;
#if WINRT
using System.Threading.Tasks;
using Windows.Storage;
using Windows.Storage.Streams;
#elif WINDOWS_PHONE || SILVERLIGHT
using System.IO.IsolatedStorage;
#endif
using Core.IO;
namespace Core
{
    public class WinVSystem : VSystem
    {
        #region Polyfill

        const int INVALID_FILE_ATTRIBUTES = -1;
        const int INVALID_SET_FILE_POINTER = -1;

#if OS_WINCE
#elif WINRT
        static bool isNT() { return true; }
#else
        static bool isNT() { return Environment.OSVersion.Platform >= PlatformID.Win32NT; }
#endif

        #endregion

        #region WinVFile

        public partial class WinVFile : VFile
        {
            public VSystem Vfs;             // The VFS used to open this file
#if WINRT
            public IRandomAccessStream H;   // Filestream access to this file
#else
            public FileStream H;            // Filestream access to this file
#endif
            public LOCK Lock;            // Type of lock currently held on this file
            public int SharedLockByte;      // Randomly chosen byte used as a shared lock
            public uint LastErrno;         // The Windows errno from the last I/O error
            public uint SectorSize;        // Sector size of the device file is on
#if !OMIT_WAL
            public winShm Shm;              // Instance of shared memory on this file
#else
            public object Shm;              // DUMMY Instance of shared memory on this file
#endif
            public string Path;             // Full pathname of this file
            public int SizeChunk;           // Chunk size configured by FCNTL_CHUNK_SIZE

            public void memset()
            {
                H = null;
                Lock = 0;
                SharedLockByte = 0;
                LastErrno = 0;
                SectorSize = 0;
            }
        };

        #endregion

        #region OS Errors

        static RC getLastErrorMsg(ref string buf)
        {
#if SILVERLIGHT || WINRT
            buf = "Unknown error";
#else
            buf = Marshal.GetLastWin32Error().ToString();
#endif
            return RC.OK;
        }

        static RC winLogError(RC a, string b, string c)
        {
#if !WINRT
            var st = new StackTrace(new StackFrame(true)); var sf = st.GetFrame(0); return winLogErrorAtLine(a, b, c, sf.GetFileLineNumber());
#else
            return winLogErrorAtLine(a, b, c, 0);
#endif
        }
        static RC winLogErrorAtLine(RC errcode, string func, string path, int line)
        {
#if SILVERLIGHT || WINRT
            uint errno = (uint)ERROR_NOT_SUPPORTED; // Error code
#else
            uint errno = (uint)Marshal.GetLastWin32Error(); // Error code
#endif
            string msg = null; // Human readable error text
            getLastErrorMsg(ref msg);
            Debug.Assert(errcode != RC.OK);
            if (path == null) path = string.Empty;
            int i;
            for (i = 0; i < msg.Length && msg[i] != '\r' && msg[i] != '\n'; i++) { }
            msg = msg.Substring(0, i);
            SysEx.LOG(errcode, "os_win.c:%d: (%d) %s(%s) - %s", line, errno, func, path, msg);
            return errcode;
        }

        #endregion

        #region Locking

        public static bool IsRunningMediumTrust() { return false; }

        private static int RESERVED_BYTE = (VFile.PENDING_BYTE + 1);
        private static int SHARED_FIRST = (VFile.PENDING_BYTE + 2);
        private static int SHARED_SIZE = 510;

        private static LockingStrategy _lockingStrategy = (IsRunningMediumTrust() ? new MediumTrustLockingStrategy() : new LockingStrategy());

        /// <summary>
        /// Basic locking strategy for Console/Winform applications
        /// </summary>
        private class LockingStrategy
        {
            const int LOCKFILE_FAIL_IMMEDIATELY = 1;
            [DllImport("kernel32.dll")]
            static extern bool LockFileEx(IntPtr hFile, uint dwFlags, uint dwReserved, uint nNumberOfBytesToLockLow, uint nNumberOfBytesToLockHigh, [In] ref System.Threading.NativeOverlapped lpOverlapped);

            public virtual void LockFile(WinVFile file, long offset, long length)
            {
                file.H.Lock(offset, length);
            }

            public virtual int SharedLockFile(WinVFile file, long offset, long length)
            {
                Debug.Assert(length == SHARED_SIZE);
                Debug.Assert(offset == SHARED_FIRST);
                var ovlp = new NativeOverlapped();
                ovlp.OffsetLow = (int)offset;
                ovlp.OffsetHigh = 0;
                ovlp.EventHandle = IntPtr.Zero;
                //SafeFileHandle.DangerousGetHandle().ToInt32()
                return (LockFileEx(file.H.Handle, LOCKFILE_FAIL_IMMEDIATELY, 0, (uint)length, 0, ref ovlp) ? 1 : 0);
            }

            public virtual void UnlockFile(WinVFile file, long offset, long length)
            {
                file.H.Unlock(offset, length);
            }
        }

        /// <summary>
        /// Locking strategy for Medium Trust. It uses the same trick used in the native code for WIN_CE
        /// which doesn't support LockFileEx as well.
        /// </summary>
        private class MediumTrustLockingStrategy : LockingStrategy
        {
            public override int SharedLockFile(WinVFile file, long offset, long length)
            {
                Debug.Assert(length == SHARED_SIZE);
                Debug.Assert(offset == SHARED_FIRST);
                try { file.H.Lock(offset + file.SharedLockByte, 1); }
                catch (IOException) { return 0; }
                return 1;
            }
        }

        #endregion

        #region WinVFile

        public partial class WinVFile : VFile
        {
            static int seekWinFile(WinVFile file, long offset)
            {
                try
                {
#if WINRT
                    file.H.Seek((ulong)offset); 
#else
                    file.H.Seek(offset, SeekOrigin.Begin);
#endif
                }
                catch (Exception)
                {
#if SILVERLIGHT || WINRT
                    file.LastErrno = 1;
#else
                    file.LastErrno = (uint)Marshal.GetLastWin32Error();
#endif
                    winLogError(RC.IOERR_SEEK, "seekWinFile", file.Path);
                    return 1;
                }
                return 0;
            }

            public static int MX_CLOSE_ATTEMPT = 3;
            public override RC Close()
            {
#if !OMIT_WAL
                Debug.Assert(Shm == null);
#endif
                OSTRACE("CLOSE %d (%s)\n", H.GetHashCode(), H.Name);
                bool rc;
                int cnt = 0;
                do
                {
#if WINRT
                    H.Dispose();
#else
                    H.Close();
#endif
                    rc = true;
                } while (!rc && ++cnt < MX_CLOSE_ATTEMPT);
                OSTRACE("CLOSE %d %s\n", H.GetHashCode(), rc ? "ok" : "failed");
                if (rc)
                    H = null;
                OpenCounter(-1);
                return (rc ? RC.OK : winLogError(RC.IOERR_CLOSE, "winClose", Path));
            }

            public override RC Read(byte[] buffer, int amount, long offset)
            {
                //if (buffer == null)
                //    buffer = new byte[amount];
                if (SimulateIOError())
                    return RC.IOERR_READ;
                OSTRACE("READ %d lock=%d\n", H.GetHashCode(), Lock);
                if (!H.CanRead)
                    return RC.IOERR_READ;
                if (seekWinFile(this, offset) != 0)
                    return RC.FULL;
                int read; // Number of bytes actually read from file
                try
                {
#if WINRT
                    var stream = H.AsStreamForRead();
                    read = stream.Read(buffer, 0, amount);
#else
                    read = H.Read(buffer, 0, amount);
#endif
                }
                catch (Exception)
                {
#if SILVERLIGHT || WINRT
                    LastErrno = 1;
#else
                    LastErrno = (uint)Marshal.GetLastWin32Error();
#endif
                    return winLogError(RC.IOERR_READ, "winRead", Path);
                }
                if (read < amount)
                {
                    // Unread parts of the buffer must be zero-filled
                    Array.Clear(buffer, (int)read, (int)(amount - read));
                    return RC.IOERR_SHORT_READ;
                }
                return RC.OK;
            }

            public override RC Write(byte[] buffer, int amount, long offset)
            {
                Debug.Assert(amount > 0);
                if (SimulateIOError())
                    return RC.IOERR_WRITE;
                if (SimulateDiskfullError())
                    return RC.FULL;
                OSTRACE("WRITE %d lock=%d\n", H.GetHashCode(), Lock);
                int rc = seekWinFile(this, offset); // True if error has occured, else false
#if WINRT
                ulong wrote = H.Position;
#else
                long wrote = H.Position;
#endif
                try
                {
                    Debug.Assert(buffer.Length >= amount);
#if WINRT
                    var stream = H.AsStreamForWrite();
                    stream.Write(buffer, 0, amount);
#else
                    H.Write(buffer, 0, amount);
#endif
                    rc = 1;
                    wrote = H.Position - wrote;
                }
                catch (IOException) { return RC.READONLY; }
                if (rc == 0 || amount > (int)wrote)
                {
#if SILVERLIGHT || WINRT
                    LastErrno  = 1;
#else
                    LastErrno = (uint)Marshal.GetLastWin32Error();
#endif
                    if (LastErrno == ERROR_HANDLE_DISK_FULL || LastErrno == ERROR_DISK_FULL)
                        return RC.FULL;
                    else
                        return winLogError(RC.IOERR_WRITE, "winWrite", Path);
                }
                return RC.OK;
            }

            public override RC Truncate(long size)
            {
                RC rc = RC.OK;
                OSTRACE("TRUNCATE %d %lld\n", H.Name, size);
                if (SimulateIOError())
                    return RC.IOERR_TRUNCATE;
                // If the user has configured a chunk-size for this file, truncate the file so that it consists of an integer number of chunks (i.e. the
                // actual file size after the operation may be larger than the requested size).
                if (SizeChunk > 0)
                    size = ((size + SizeChunk - 1) / SizeChunk) * SizeChunk;
                try
                {
#if WINRT
                    H.Size = (ulong)size;
#else
                    H.SetLength(size);
#endif
                    rc = RC.OK;
                }
                catch (IOException)
                {
#if SILVERLIGHT || WINRT
                    LastErrno = 1;
#else
                    LastErrno = (uint)Marshal.GetLastWin32Error();
#endif
                    rc = winLogError(RC.IOERR_TRUNCATE, "winTruncate2", Path);
                }
                OSTRACE("TRUNCATE %d %lld %s\n", H.GetHashCode(), size, rc == RC.OK ? "ok" : "failed");
                return rc;
            }

#if TEST
            // Count the number of fullsyncs and normal syncs.  This is used to test that syncs and fullsyncs are occuring at the right times.
#if !TCLSH
            static int sync_count = 0;
            static int fullsync_count = 0;
#else
            static tcl.lang.Var.SQLITE3_GETSET sync_count = new tcl.lang.Var.SQLITE3_GETSET("sync_count");
            static tcl.lang.Var.SQLITE3_GETSET fullsync_count = new tcl.lang.Var.SQLITE3_GETSET("fullsync_count");
#endif
#endif

            public override RC Sync(SYNC flags)
            {
                // Check that one of SQLITE_SYNC_NORMAL or FULL was passed
                Debug.Assert(((int)flags & 0x0F) == (int)SYNC.NORMAL || ((int)flags & 0x0F) == (int)SYNC.FULL);
                OSTRACE("SYNC %d lock=%d\n", H.GetHashCode(), Lock);
                // Unix cannot, but some systems may return SQLITE_FULL from here. This line is to test that doing so does not cause any problems.
                if (SimulateDiskfullError())
                    return RC.FULL;
#if TEST
                if (((int)flags & 0x0F) == (int)SYNC.FULL)
#if !TCLSH
                    fullsync_count++;
                sync_count++;
#else
                    fullsync_count.iValue++;
                sync_count.iValue++;
#endif
#endif
#if NO_SYNC // If we compiled with the SQLITE_NO_SYNC flag, then syncing is a no-op
                return RC::OK;
#elif WINRT
                var stream = H.AsStreamForWrite();
                stream.Flush();
                return RC.OK;
#else
                H.Flush();
                return RC.OK;
#endif
            }

            public override RC get_FileSize(out long size)
            {
                if (SimulateIOError())
                {
                    size = 0;
                    return RC.IOERR_FSTAT;
                }
#if WINRT
                size = (H.CanRead ? (long)H.Size : 0);
#else
                size = (H.CanRead ? H.Length : 0);
#endif
                return RC.OK;
            }

            static int getReadLock(WinVFile file)
            {
                int res = 0;
                if (isNT())
                    res = _lockingStrategy.SharedLockFile(file, SHARED_FIRST, SHARED_SIZE);
                // isNT() is 1 if SQLITE_OS_WINCE==1, so this else is never executed.
#if !OS_WINCE
                else
                {
                    Debugger.Break();
                    //  int lk;
                    //  sqlite3_randomness(lk.Length, lk);
                    //  pFile.sharedLockByte = (u16)((lk & 0x7fffffff)%(SHARED_SIZE - 1));
                    //  res = pFile.fs.Lock( SHARED_FIRST + pFile.sharedLockByte, 0, 1, 0);
                }
#endif
                if (res == 0)
#if SILVERLIGHT || WINRT
                    file.LastErrno = 1;
#else
                    file.LastErrno = (uint)Marshal.GetLastWin32Error();
#endif
                // No need to log a failure to lock
                return res;
            }

            static int unlockReadLock(WinVFile file)
            {
                int res = 1;
                if (isNT())
                    try { _lockingStrategy.UnlockFile(file, SHARED_FIRST, SHARED_SIZE); }
                    catch (Exception) { res = 0; }
                // isNT() is 1 if SQLITE_OS_WINCE==1, so this else is never executed.
#if !OS_WINCE
                else
                    Debugger.Break();
#endif
                if (res == 0)
                {
#if SILVERLIGHT || WINRT
                    file.LastErrno = 1;
#else
                    file.LastErrno = (uint)Marshal.GetLastWin32Error();
#endif
                    winLogError(RC.IOERR_UNLOCK, "unlockReadLock", file.Path);
                }
                return res;
            }

            public override RC Lock(LOCK lock_)
            {
                OSTRACE("LOCK %d %d was %d(%d)\n", H.GetHashCode(), lock_, Lock, SharedLockByte);

                // If there is already a lock of this type or more restrictive on the OsFile, do nothing. Don't use the end_lock: exit path, as
                // sqlite3OsEnterMutex() hasn't been called yet.
                if (Lock >= lock_)
                    return RC.OK;

                // Make sure the locking sequence is correct
                Debug.Assert(lock_ != LOCK.NO || lock_ == LOCK.SHARED);
                Debug.Assert(lock_ != LOCK.PENDING);
                Debug.Assert(lock_ != LOCK.RESERVED || Lock == LOCK.SHARED);

                // Lock the PENDING_LOCK byte if we need to acquire a PENDING lock or a SHARED lock.  If we are acquiring a SHARED lock, the acquisition of
                // the PENDING_LOCK byte is temporary.
                LOCK newLock = Lock; // Set pFile.locktype to this value before exiting
                int res = 1;                // Result of a windows lock call
                bool gotPendingLock = false;// True if we acquired a PENDING lock this time
                uint lastErrno = 0;
                if (Lock == LOCK.NO || (lock_ == LOCK.EXCLUSIVE && Lock == LOCK.RESERVED))
                {
                    res = 0;
                    int cnt = 3;
                    while (cnt-- > 0 && res == 0)
                    {
                        try { _lockingStrategy.LockFile(this, PENDING_BYTE, 1); res = 1; }
                        catch (Exception)
                        {
                            // Try 3 times to get the pending lock.  The pending lock might be held by another reader process who will release it momentarily.
                            OSTRACE("could not get a PENDING lock. cnt=%d\n", cnt);
#if WINRT
                            System.Threading.Tasks.Task.Delay(1).Wait();
#else
                            Thread.Sleep(1);
#endif
                        }
                    }
                    gotPendingLock = (res != 0);
                    if (res == 0)
#if SILVERLIGHT || WINRT
                        lastErrno = 1;
#else
                        lastErrno = (uint)Marshal.GetLastWin32Error();
#endif
                }

                // Acquire a SHARED lock
                if (lock_ == LOCK.SHARED && res != 0)
                {
                    Debug.Assert(Lock == LOCK.NO);
                    res = getReadLock(this);
                    if (res != 0)
                        newLock = LOCK.SHARED;
                    else
#if SILVERLIGHT || WINRT
                        lastErrno = 1;
#else
                        lastErrno = (uint)Marshal.GetLastWin32Error();
#endif
                }

                // Acquire a RESERVED lock
                if (lock_ == LOCK.RESERVED && res != 0)
                {
                    Debug.Assert(Lock == LOCK.SHARED);
                    try { _lockingStrategy.LockFile(this, RESERVED_BYTE, 1); newLock = LOCK.RESERVED; res = 1; }
                    catch (Exception) { res = 0; }
                    if (res != 0)
                        newLock = LOCK.RESERVED;
                    else
#if SILVERLIGHT
                        lastErrno = 1;
#else
                        lastErrno = (uint)Marshal.GetLastWin32Error();
#endif
                }

                // Acquire a PENDING lock
                if (lock_ == LOCK.EXCLUSIVE && res != 0)
                {
                    newLock = LOCK.PENDING;
                    gotPendingLock = false;
                }

                // Acquire an EXCLUSIVE lock
                if (lock_ == LOCK.EXCLUSIVE && res != 0)
                {
                    Debug.Assert(Lock >= LOCK.SHARED);
                    res = unlockReadLock(this);
                    OSTRACE("unreadlock = %d\n", res);
                    try { _lockingStrategy.LockFile(this, SHARED_FIRST, SHARED_SIZE); newLock = LOCK.EXCLUSIVE; res = 1; }
                    catch (Exception) { res = 0; }
                    if (res != 0)
                        newLock = LOCK.EXCLUSIVE;
                    else
                    {
#if SILVERLIGHT || WINRT
                        lastErrno = 1;
#else
                        lastErrno = (uint)Marshal.GetLastWin32Error();
#endif
                        OSTRACE("error-code = %d\n", lastErrno);
                        getReadLock(this);
                    }
                }

                // If we are holding a PENDING lock that ought to be released, then release it now.
                if (gotPendingLock && lock_ == LOCK.SHARED)
                    _lockingStrategy.UnlockFile(this, PENDING_BYTE, 1);

                // Update the state of the lock has held in the file descriptor then return the appropriate result code.
                RC rc;
                if (res != 0)
                    rc = RC.OK;
                else
                {
                    OSTRACE("LOCK FAILED %d trying for %d but got %d\n", H.GetHashCode(), lock_, newLock);
                    LastErrno = lastErrno;
                    rc = RC.BUSY;
                }
                Lock = newLock;
                return rc;
            }

            public override RC CheckReservedLock(ref int resOut)
            {
                if (SimulateIOError())
                    return RC.IOERR_CHECKRESERVEDLOCK;
                int rc;
                if (Lock >= LOCK.RESERVED)
                {
                    rc = 1;
                    OSTRACE("TEST WR-LOCK %d %d (local)\n", H.Name, rc);
                }
                else
                {
                    try { _lockingStrategy.LockFile(this, RESERVED_BYTE, 1); _lockingStrategy.UnlockFile(this, RESERVED_BYTE, 1); rc = 1; }
                    catch (IOException) { rc = 0; }
                    rc = 1 - rc;
                    OSTRACE("TEST WR-LOCK %d %d (remote)\n", H.GetHashCode(), rc);
                }
                resOut = rc;
                return RC.OK;
            }

            public override RC Unlock(LOCK lock_)
            {
                Debug.Assert(lock_ <= LOCK.SHARED);
                OSTRACE("UNLOCK %d to %d was %d(%d)\n", H.GetHashCode(), lock_, Lock, SharedLockByte);
                var rc = RC.OK;
                LOCK type = Lock;
                if (type >= LOCK.EXCLUSIVE)
                {
                    _lockingStrategy.UnlockFile(this, SHARED_FIRST, SHARED_SIZE);
                    if (lock_ == LOCK.SHARED && getReadLock(this) == 0)
                        // This should never happen.  We should always be able to reacquire the read lock
                        rc = winLogError(RC.IOERR_UNLOCK, "winUnlock", Path);
                }
                if (type >= LOCK.RESERVED)
                    try { _lockingStrategy.UnlockFile(this, RESERVED_BYTE, 1); }
                    catch (Exception) { }
                if (lock_ == LOCK.NO && type >= LOCK.SHARED)
                    unlockReadLock(this);
                if (type >= LOCK.PENDING)
                    try { _lockingStrategy.UnlockFile(this, PENDING_BYTE, 1); }
                    catch (Exception) { }
                Lock = lock_;
                return rc;
            }

            public override RC FileControl(FCNTL op, ref long pArg)
            {
                switch (op)
                {
                    case FCNTL.LOCKSTATE:
                        pArg = (int)Lock;
                        return RC.OK;
                    case FCNTL.LAST_ERRNO:
                        pArg = (int)LastErrno;
                        return RC.OK;
                    case FCNTL.CHUNK_SIZE:
                        SizeChunk = (int)pArg;
                        return RC.OK;
                    case FCNTL.SIZE_HINT:
                        var sz = (long)pArg;
                        SimulateIOErrorBenign(true);
                        Truncate(sz);
                        SimulateIOErrorBenign(false);
                        return RC.OK;
                    case FCNTL.SYNC_OMITTED:
                        return RC.OK;
                }
                return RC.NOTFOUND;
            }

            public override uint get_SectorSize()
            {
                return SectorSize;
            }

            public override int DeviceCharacteristics()
            {
                return 0;
            }




            
        

#if !SQLITE_OMIT_WAL


/* 
** Windows will only let you create file view mappings
** on allocation size granularity boundaries.
** During sqlite3_os_init() we do a GetSystemInfo()
** to get the granularity size.
*/
SYSTEM_INFO winSysInfo;

/*
** Helper functions to obtain and relinquish the global mutex. The
** global mutex is used to protect the winLockInfo objects used by 
** this file, all of which may be shared by multiple threads.
**
** Function winShmMutexHeld() is used to Debug.Assert() that the global mutex 
** is held when required. This function is only used as part of Debug.Assert() 
** statements. e.g.
**
**   winShmEnterMutex()
**     Debug.Assert( winShmMutexHeld() );
**   winShmLeaveMutex()
*/
static void winShmEnterMutex(void){
  sqlite3_mutex_enter(sqlite3MutexAlloc(SQLITE_MUTEX_STATIC_MASTER));
}
static void winShmLeaveMutex(void){
  sqlite3_mutex_leave(sqlite3MutexAlloc(SQLITE_MUTEX_STATIC_MASTER));
}
#if SQLITE_DEBUG
static int winShmMutexHeld(void) {
  return sqlite3_mutex_held(sqlite3MutexAlloc(SQLITE_MUTEX_STATIC_MASTER));
}
#endif

/*
** Object used to represent a single file opened and mmapped to provide
** shared memory.  When multiple threads all reference the same
** log-summary, each thread has its own winFile object, but they all
** point to a single instance of this object.  In other words, each
** log-summary is opened only once per process.
**
** winShmMutexHeld() must be true when creating or destroying
** this object or while reading or writing the following fields:
**
**      nRef
**      pNext 
**
** The following fields are read-only after the object is created:
** 
**      fid
**      zFilename
**
** Either winShmNode.mutex must be held or winShmNode.nRef==0 and
** winShmMutexHeld() is true when reading or writing any other field
** in this structure.
**
*/
struct winShmNode {
  sqlite3_mutex *mutex;      /* Mutex to access this object */
  string zFilename;           /* Name of the file */
  winFile hFile;             /* File handle from winOpen */

  int szRegion;              /* Size of shared-memory regions */
  int nRegion;               /* Size of array apRegion */
  struct ShmRegion {
    HANDLE hMap;             /* File handle from CreateFileMapping */
    void *pMap;
  } *aRegion;
  DWORD lastErrno;           /* The Windows errno from the last I/O error */

  int nRef;                  /* Number of winShm objects pointing to this */
  winShm *pFirst;            /* All winShm objects pointing to this */
  winShmNode *pNext;         /* Next in list of all winShmNode objects */
#if SQLITE_DEBUG
  u8 nextShmId;              /* Next available winShm.id value */
#endif
};

/*
** A global array of all winShmNode objects.
**
** The winShmMutexHeld() must be true while reading or writing this list.
*/
static winShmNode *winShmNodeList = 0;

/*
** Structure used internally by this VFS to record the state of an
** open shared memory connection.
**
** The following fields are initialized when this object is created and
** are read-only thereafter:
**
**    winShm.pShmNode
**    winShm.id
**
** All other fields are read/write.  The winShm.pShmNode->mutex must be held
** while accessing any read/write fields.
*/
struct winShm {
  winShmNode *pShmNode;      /* The underlying winShmNode object */
  winShm *pNext;             /* Next winShm with the same winShmNode */
  u8 hasMutex;               /* True if holding the winShmNode mutex */
  u16 sharedMask;            /* Mask of shared locks held */
  u16 exclMask;              /* Mask of exclusive locks held */
#if SQLITE_DEBUG
  u8 id;                     /* Id of this connection with its winShmNode */
#endif
};

/*
** Constants used for locking
*/
//#define WIN_SHM_BASE   ((22+SQLITE_SHM_NLOCK)*4)        /* first lock byte */
//#define WIN_SHM_DMS    (WIN_SHM_BASE+SQLITE_SHM_NLOCK)  /* deadman switch */

/*
** Apply advisory locks for all n bytes beginning at ofst.
*/
//#define _SHM_UNLCK  1
//#define _SHM_RDLCK  2
//#define _SHM_WRLCK  3
static int winShmSystemLock(
  winShmNode *pFile,    /* Apply locks to this open shared-memory segment */
  int lockType,         /* _SHM_UNLCK, _SHM_RDLCK, or _SHM_WRLCK */
  int ofst,             /* Offset to first byte to be locked/unlocked */
  int nByte             /* Number of bytes to lock or unlock */
){
  OVERLAPPED ovlp;
  DWORD dwFlags;
  int rc = 0;           /* Result code form Lock/UnlockFileEx() */

  /* Access to the winShmNode object is serialized by the caller */
  Debug.Assert( sqlite3_mutex_held(pFile->mutex) || pFile->nRef==0 );

  /* Initialize the locking parameters */
  dwFlags = LOCKFILE_FAIL_IMMEDIATELY;
  if( lockType == _SHM_WRLCK ) dwFlags |= LOCKFILE_EXCLUSIVE_LOCK;

  memset(&ovlp, 0, sizeof(OVERLAPPED));
  ovlp.Offset = ofst;

  /* Release/Acquire the system-level lock */
  if( lockType==_SHM_UNLCK ){
    rc = UnlockFileEx(pFile->hFile.h, 0, nByte, 0, &ovlp);
  }else{
    rc = LockFileEx(pFile->hFile.h, dwFlags, 0, nByte, 0, &ovlp);
  }
  
  if( rc!= 0 ){
    rc = SQLITE_OK;
  }else{
    pFile->lastErrno =  GetLastError();
    rc = SQLITE_BUSY;
  }

  OSTRACE(("SHM-LOCK %d %s %s 0x%08lx\n", 
           pFile->hFile.h,
           rc==SQLITE_OK ? "ok" : "failed",
           lockType==_SHM_UNLCK ? "UnlockFileEx" : "LockFileEx",
           pFile->lastErrno));

  return rc;
}

/* Forward references to VFS methods */
static int winOpen(sqlite3_vfs*,const char*,sqlite3_file*,int,int);
static int winDelete(sqlite3_vfs *,const char*,int);

/*
** Purge the winShmNodeList list of all entries with winShmNode.nRef==0.
**
** This is not a VFS shared-memory method; it is a utility function called
** by VFS shared-memory methods.
*/
static void winShmPurge(sqlite3_vfs *pVfs, int deleteFlag){
  winShmNode **pp;
  winShmNode *p;
  BOOL bRc;
  Debug.Assert( winShmMutexHeld() );
  pp = winShmNodeList;
  while( (p = *pp)!=0 ){
    if( p->nRef==0 ){
      int i;
      if( p->mutex ) sqlite3_mutex_free(p->mutex);
      for(i=0; i<p->nRegion; i++){
        bRc = UnmapViewOfFile(p->aRegion[i].pMap);
        OSTRACE(("SHM-PURGE pid-%d unmap region=%d %s\n",
                 (int)GetCurrentProcessId(), i,
                 bRc ? "ok" : "failed"));
        bRc = CloseHandle(p->aRegion[i].hMap);
        OSTRACE(("SHM-PURGE pid-%d close region=%d %s\n",
                 (int)GetCurrentProcessId(), i,
                 bRc ? "ok" : "failed"));
      }
      if( p->hFile.h != INVALID_HANDLE_VALUE ){
        SimulateIOErrorBenign(1);
        winClose((sqlite3_file )&p->hFile);
        SimulateIOErrorBenign(0);
      }
      if( deleteFlag ){
        SimulateIOErrorBenign(1);
        winDelete(pVfs, p->zFilename, 0);
        SimulateIOErrorBenign(0);
      }
      *pp = p->pNext;
      sqlite3_free(p->aRegion);
      sqlite3_free(p);
    }else{
      pp = p->pNext;
    }
  }
}

/*
** Open the shared-memory area associated with database file pDbFd.
**
** When opening a new shared-memory file, if no other instances of that
** file are currently open, in this process or in other processes, then
** the file must be truncated to zero length or have its header cleared.
*/
static int winOpenSharedMemory(winFile *pDbFd){
  struct winShm *p;                  /* The connection to be opened */
  struct winShmNode *pShmNode = 0;   /* The underlying mmapped file */
  int rc;                            /* Result code */
  struct winShmNode *pNew;           /* Newly allocated winShmNode */
  int nName;                         /* Size of zName in bytes */

  Debug.Assert( pDbFd->pShm==null );    /* Not previously opened */

  /* Allocate space for the new sqlite3_shm object.  Also speculatively
  ** allocate space for a new winShmNode and filename.
  */
  p = sqlite3_malloc( sizeof(*p) );
  if( p==0 ) return SQLITE_NOMEM;
  memset(p, 0, sizeof(*p));
  nName = sqlite3Strlen30(pDbFd->zPath);
  pNew = sqlite3_malloc( sizeof(*pShmNode) + nName + 15 );
  if( pNew==0 ){
    sqlite3_free(p);
    return SQLITE_NOMEM;
  }
  memset(pNew, 0, sizeof(*pNew));
  pNew->zFilename = (char)&pNew[1];
  sqlite3_snprintf(nName+15, pNew->zFilename, "%s-shm", pDbFd->zPath);
  sqlite3FileSuffix3(pDbFd->zPath, pNew->zFilename); 

  /* Look to see if there is an existing winShmNode that can be used.
  ** If no matching winShmNode currently exists, create a new one.
  */
  winShmEnterMutex();
  for(pShmNode = winShmNodeList; pShmNode; pShmNode=pShmNode->pNext){
    /* TBD need to come up with better match here.  Perhaps
    ** use FILE_ID_BOTH_DIR_INFO Structure.
    */
    if( sqlite3StrICmp(pShmNode->zFilename, pNew->zFilename)==0 ) break;
  }
  if( pShmNode ){
    sqlite3_free(pNew);
  }else{
    pShmNode = pNew;
    pNew = 0;
    ((winFile)(&pShmNode->hFile))->h = INVALID_HANDLE_VALUE;
    pShmNode->pNext = winShmNodeList;
    winShmNodeList = pShmNode;

    pShmNode->mutex = sqlite3_mutex_alloc(SQLITE_MUTEX_FAST);
    if( pShmNode->mutex==0 ){
      rc = SQLITE_NOMEM;
      goto shm_open_err;
    }

    rc = winOpen(pDbFd->pVfs,
                 pShmNode->zFilename,             /* Name of the file (UTF-8) */
                 (sqlite3_file)&pShmNode->hFile,  /* File handle here */
                 SQLITE_OPEN_WAL | SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE, /* Mode flags */
                 0);
    if( SQLITE_OK!=rc ){
      rc = SQLITE_CANTOPEN_BKPT;
      goto shm_open_err;
    }

    /* Check to see if another process is holding the dead-man switch.
    ** If not, truncate the file to zero length. 
    */
    if( winShmSystemLock(pShmNode, _SHM_WRLCK, WIN_SHM_DMS, 1)==SQLITE_OK ){
      rc = winTruncate((sqlite3_file )&pShmNode->hFile, 0);
      if( rc!=SQLITE_OK ){
        rc = winLogError(SQLITE_IOERR_SHMOPEN, "winOpenShm", pDbFd->zPath);
      }
    }
    if( rc==SQLITE_OK ){
      winShmSystemLock(pShmNode, _SHM_UNLCK, WIN_SHM_DMS, 1);
      rc = winShmSystemLock(pShmNode, _SHM_RDLCK, WIN_SHM_DMS, 1);
    }
    if( rc ) goto shm_open_err;
  }

  /* Make the new connection a child of the winShmNode */
  p->pShmNode = pShmNode;
#if SQLITE_DEBUG
  p->id = pShmNode->nextShmId++;
#endif
  pShmNode->nRef++;
  pDbFd->pShm = p;
  winShmLeaveMutex();

  /* The reference count on pShmNode has already been incremented under
  ** the cover of the winShmEnterMutex() mutex and the pointer from the
  ** new (struct winShm) object to the pShmNode has been set. All that is
  ** left to do is to link the new object into the linked list starting
  ** at pShmNode->pFirst. This must be done while holding the pShmNode->mutex 
  ** mutex.
  */
  sqlite3_mutex_enter(pShmNode->mutex);
  p->pNext = pShmNode->pFirst;
  pShmNode->pFirst = p;
  sqlite3_mutex_leave(pShmNode->mutex);
  return SQLITE_OK;

  /* Jump here on any error */
shm_open_err:
  winShmSystemLock(pShmNode, _SHM_UNLCK, WIN_SHM_DMS, 1);
  winShmPurge(pDbFd->pVfs, 0);      /* This call frees pShmNode if required */
  sqlite3_free(p);
  sqlite3_free(pNew);
  winShmLeaveMutex();
  return rc;
}

/*
** Close a connection to shared-memory.  Delete the underlying 
** storage if deleteFlag is true.
*/
static int winShmUnmap(
  sqlite3_file *fd,          /* Database holding shared memory */
  int deleteFlag             /* Delete after closing if true */
){
  winFile *pDbFd;       /* Database holding shared-memory */
  winShm *p;            /* The connection to be closed */
  winShmNode *pShmNode; /* The underlying shared-memory file */
  winShm **pp;          /* For looping over sibling connections */

  pDbFd = (winFile)fd;
  p = pDbFd->pShm;
  if( p==0 ) return SQLITE_OK;
  pShmNode = p->pShmNode;

  /* Remove connection p from the set of connections associated
  ** with pShmNode */
  sqlite3_mutex_enter(pShmNode->mutex);
  for(pp=&pShmNode->pFirst; (*pp)!=p; pp = (*pp)->pNext){}
  *pp = p->pNext;

  /* Free the connection p */
  sqlite3_free(p);
  pDbFd->pShm = 0;
  sqlite3_mutex_leave(pShmNode->mutex);

  /* If pShmNode->nRef has reached 0, then close the underlying
  ** shared-memory file, too */
  winShmEnterMutex();
  Debug.Assert( pShmNode->nRef>0 );
  pShmNode->nRef--;
  if( pShmNode->nRef==0 ){
    winShmPurge(pDbFd->pVfs, deleteFlag);
  }
  winShmLeaveMutex();

  return SQLITE_OK;
}

/*
** Change the lock state for a shared-memory segment.
*/
static int winShmLock(
  sqlite3_file *fd,          /* Database file holding the shared memory */
  int ofst,                  /* First lock to acquire or release */
  int n,                     /* Number of locks to acquire or release */
  int flags                  /* What to do with the lock */
){
  winFile *pDbFd = (winFile)fd;        /* Connection holding shared memory */
  winShm *p = pDbFd->pShm;              /* The shared memory being locked */
  winShm *pX;                           /* For looping over all siblings */
  winShmNode *pShmNode = p->pShmNode;
  int rc = SQLITE_OK;                   /* Result code */
  u16 mask;                             /* Mask of locks to take or release */

  Debug.Assert( ofst>=0 && ofst+n<=SQLITE_SHM_NLOCK );
  Debug.Assert( n>=1 );
  Debug.Assert( flags==(SQLITE_SHM_LOCK | SQLITE_SHM_SHARED)
       || flags==(SQLITE_SHM_LOCK | SQLITE_SHM_EXCLUSIVE)
       || flags==(SQLITE_SHM_UNLOCK | SQLITE_SHM_SHARED)
       || flags==(SQLITE_SHM_UNLOCK | SQLITE_SHM_EXCLUSIVE) );
  Debug.Assert( n==1 || (flags & SQLITE_SHM_EXCLUSIVE)!=0 );

  mask = (u16)((1U<<(ofst+n)) - (1U<<ofst));
  Debug.Assert( n>1 || mask==(1<<ofst) );
  sqlite3_mutex_enter(pShmNode->mutex);
  if( flags & SQLITE_SHM_UNLOCK ){
    u16 allMask = 0; /* Mask of locks held by siblings */

    /* See if any siblings hold this same lock */
    for(pX=pShmNode->pFirst; pX; pX=pX->pNext){
      if( pX==p ) continue;
      Debug.Assert( (pX->exclMask & (p->exclMask|p->sharedMask))==0 );
      allMask |= pX->sharedMask;
    }

    /* Unlock the system-level locks */
    if( (mask & allMask)==0 ){
      rc = winShmSystemLock(pShmNode, _SHM_UNLCK, ofst+WIN_SHM_BASE, n);
    }else{
      rc = SQLITE_OK;
    }

    /* Undo the local locks */
    if( rc==SQLITE_OK ){
      p->exclMask &= ~mask;
      p->sharedMask &= ~mask;
    } 
  }else if( flags & SQLITE_SHM_SHARED ){
    u16 allShared = 0;  /* Union of locks held by connections other than "p" */

    /* Find out which shared locks are already held by sibling connections.
    ** If any sibling already holds an exclusive lock, go ahead and return
    ** SQLITE_BUSY.
    */
    for(pX=pShmNode->pFirst; pX; pX=pX->pNext){
      if( (pX->exclMask & mask)!=0 ){
        rc = SQLITE_BUSY;
        break;
      }
      allShared |= pX->sharedMask;
    }

    /* Get shared locks at the system level, if necessary */
    if( rc==SQLITE_OK ){
      if( (allShared & mask)==0 ){
        rc = winShmSystemLock(pShmNode, _SHM_RDLCK, ofst+WIN_SHM_BASE, n);
      }else{
        rc = SQLITE_OK;
      }
    }

    /* Get the local shared locks */
    if( rc==SQLITE_OK ){
      p->sharedMask |= mask;
    }
  }else{
    /* Make sure no sibling connections hold locks that will block this
    ** lock.  If any do, return SQLITE_BUSY right away.
    */
    for(pX=pShmNode->pFirst; pX; pX=pX->pNext){
      if( (pX->exclMask & mask)!=0 || (pX->sharedMask & mask)!=0 ){
        rc = SQLITE_BUSY;
        break;
      }
    }
  
    /* Get the exclusive locks at the system level.  Then if successful
    ** also mark the local connection as being locked.
    */
    if( rc==SQLITE_OK ){
      rc = winShmSystemLock(pShmNode, _SHM_WRLCK, ofst+WIN_SHM_BASE, n);
      if( rc==SQLITE_OK ){
        Debug.Assert( (p->sharedMask & mask)==0 );
        p->exclMask |= mask;
      }
    }
  }
  sqlite3_mutex_leave(pShmNode->mutex);
  OSTRACE(("SHM-LOCK shmid-%d, pid-%d got %03x,%03x %s\n",
           p->id, (int)GetCurrentProcessId(), p->sharedMask, p->exclMask,
           rc ? "failed" : "ok"));
  return rc;
}

/*
** Implement a memory barrier or memory fence on shared memory.  
**
** All loads and stores begun before the barrier must complete before
** any load or store begun after the barrier.
*/
static void winShmBarrier(
  sqlite3_file *fd          /* Database holding the shared memory */
){
  UNUSED_PARAMETER(fd);
  /* MemoryBarrier(); // does not work -- do not know why not */
  winShmEnterMutex();
  winShmLeaveMutex();
}

/*
** This function is called to obtain a pointer to region iRegion of the 
** shared-memory associated with the database file fd. Shared-memory regions 
** are numbered starting from zero. Each shared-memory region is szRegion 
** bytes in size.
**
** If an error occurs, an error code is returned and *pp is set to NULL.
**
** Otherwise, if the isWrite parameter is 0 and the requested shared-memory
** region has not been allocated (by any client, including one running in a
** separate process), then *pp is set to NULL and SQLITE_OK returned. If 
** isWrite is non-zero and the requested shared-memory region has not yet 
** been allocated, it is allocated by this function.
**
** If the shared-memory region has already been allocated or is allocated by
** this call as described above, then it is mapped into this processes 
** address space (if it is not already), *pp is set to point to the mapped 
** memory and SQLITE_OK returned.
*/
static int winShmMap(
  sqlite3_file *fd,               /* Handle open on database file */
  int iRegion,                    /* Region to retrieve */
  int szRegion,                   /* Size of regions */
  int isWrite,                    /* True to extend file if necessary */
  void volatile **pp              /* OUT: Mapped memory */
){
  winFile *pDbFd = (winFile)fd;
  winShm *p = pDbFd->pShm;
  winShmNode *pShmNode;
  int rc = SQLITE_OK;

  if( null==p ){
    rc = winOpenSharedMemory(pDbFd);
    if( rc!=SQLITE_OK ) return rc;
    p = pDbFd->pShm;
  }
  pShmNode = p->pShmNode;

  sqlite3_mutex_enter(pShmNode->mutex);
  Debug.Assert( szRegion==pShmNode->szRegion || pShmNode->nRegion==0 );

  if( pShmNode->nRegion<=iRegion ){
    struct ShmRegion *apNew;           /* New aRegion[] array */
    int nByte = (iRegion+1)*szRegion;  /* Minimum required file size */
    sqlite3_int64 sz;                  /* Current size of wal-index file */

    pShmNode->szRegion = szRegion;

    /* The requested region is not mapped into this processes address space.
    ** Check to see if it has been allocated (i.e. if the wal-index file is
    ** large enough to contain the requested region).
    */
    rc = winFileSize((sqlite3_file )&pShmNode->hFile, &sz);
    if( rc!=SQLITE_OK ){
      rc = winLogError(SQLITE_IOERR_SHMSIZE, "winShmMap1", pDbFd->zPath);
      goto shmpage_out;
    }

    if( sz<nByte ){
      /* The requested memory region does not exist. If isWrite is set to
      ** zero, exit early. *pp will be set to NULL and SQLITE_OK returned.
      **
      ** Alternatively, if isWrite is non-zero, use ftruncate() to allocate
      ** the requested memory region.
      */
      if( null==isWrite ) goto shmpage_out;
      rc = winTruncate((sqlite3_file )&pShmNode->hFile, nByte);
      if( rc!=SQLITE_OK ){
        rc = winLogError(SQLITE_IOERR_SHMSIZE, "winShmMap2", pDbFd->zPath);
        goto shmpage_out;
      }
    }

    /* Map the requested memory region into this processes address space. */
    apNew = (struct ShmRegion )sqlite3_realloc(
        pShmNode->aRegion, (iRegion+1)*sizeof(apNew[0])
    );
    if( null==apNew ){
      rc = SQLITE_IOERR_NOMEM;
      goto shmpage_out;
    }
    pShmNode->aRegion = apNew;

    while( pShmNode->nRegion<=iRegion ){
      HANDLE hMap;                /* file-mapping handle */
      void *pMap = 0;             /* Mapped memory region */
     
      hMap = CreateFileMapping(pShmNode->hFile.h, 
          NULL, PAGE_READWRITE, 0, nByte, NULL
      );
      OSTRACE(("SHM-MAP pid-%d create region=%d nbyte=%d %s\n",
               (int)GetCurrentProcessId(), pShmNode->nRegion, nByte,
               hMap ? "ok" : "failed"));
      if( hMap ){
        int iOffset = pShmNode->nRegion*szRegion;
        int iOffsetShift = iOffset % winSysInfo.dwAllocationGranularity;
        pMap = MapViewOfFile(hMap, FILE_MAP_WRITE | FILE_MAP_READ,
            0, iOffset - iOffsetShift, szRegion + iOffsetShift
        );
        OSTRACE(("SHM-MAP pid-%d map region=%d offset=%d size=%d %s\n",
                 (int)GetCurrentProcessId(), pShmNode->nRegion, iOffset, szRegion,
                 pMap ? "ok" : "failed"));
      }
      if( null==pMap ){
        pShmNode->lastErrno = GetLastError();
        rc = winLogError(SQLITE_IOERR_SHMMAP, "winShmMap3", pDbFd->zPath);
        if( hMap ) CloseHandle(hMap);
        goto shmpage_out;
      }

      pShmNode->aRegion[pShmNode->nRegion].pMap = pMap;
      pShmNode->aRegion[pShmNode->nRegion].hMap = hMap;
      pShmNode->nRegion++;
    }
  }

shmpage_out:
  if( pShmNode->nRegion>iRegion ){
    int iOffset = iRegion*szRegion;
    int iOffsetShift = iOffset % winSysInfo.dwAllocationGranularity;
    char *p = (char )pShmNode->aRegion[iRegion].pMap;
    *pp = (void )&p[iOffsetShift];
  }else{
    *pp = 0;
  }
  sqlite3_mutex_leave(pShmNode->mutex);
  return rc;
}

#else
    //# define winShmMap     0
    static int winShmMap(
    sqlite3_file fd,                /* Handle open on database file */
    int iRegion,                    /* Region to retrieve */
    int szRegion,                   /* Size of regions */
    int isWrite,                    /* True to extend file if necessary */
    out object pp                   /* OUT: Mapped memory */
    )
    {
      pp = null;
      return 0;
    }

    //# define winShmLock    0
    static int winShmLock(
    sqlite3_file fd,           /* Database file holding the shared memory */
    int ofst,                  /* First lock to acquire or release */
    int n,                     /* Number of locks to acquire or release */
    int flags                  /* What to do with the lock */
    )
    {
      return 0;
    }

    //# define winShmBarrier 0
    static void winShmBarrier(
    sqlite3_file fd          /* Database holding the shared memory */
    )
    {
    }

    //# define winShmUnmap   0
    static int winShmUnmap(
    sqlite3_file fd,           /* Database holding shared memory */
    int deleteFlag             /* Delete after closing if true */
    )
    {
      return 0;
    }

#endif



























        }


        #endregion












        #region VSystem

        internal const long ERROR_FILE_NOT_FOUND = 2L;
        internal const long ERROR_HANDLE_DISK_FULL = 39L;
        internal const long ERROR_NOT_SUPPORTED = 50L;
        internal const long ERROR_DISK_FULL = 112L;
        //
        const int SQLITE_DEFAULT_SECTOR_SIZE = 512;
        const int MAX_PATH = 260;
        static int MX_DELETION_ATTEMPTS = 5;

        //public WinVSystem() { }
        //public WinVSystem(int szOsFile, int mxPathname, VSystem pNext, string zName, object pAppData)
        //{
        //    this.szOsFile = szOsFile;
        //    this.mxPathname = mxPathname;
        //    this.Next = pNext;
        //    this.Name = zName;
        //    this.AppData = pAppData;
        //}

        public override RC Open(string name, VFile id, OPEN flags, out OPEN outFlags)
        {
            Debug.Assert(id != null);
            outFlags = 0;

            var rc = RC.OK;
            var type = (OPEN)((int)flags & 0xFFFFFF00);  // Type of file to open
            var exclusive = (flags & OPEN.EXCLUSIVE) != 0;
            var delete = (flags & OPEN.DELETEONCLOSE) != 0;
            var create = (flags & OPEN.CREATE) != 0;
            var readOnly = (flags & OPEN.READONLY) != 0;
            var readWrite = (flags & OPEN.READWRITE) != 0;
            var openJournal = (create && (type == OPEN.MASTER_JOURNAL || type == OPEN.MAIN_JOURNAL || type == OPEN.WAL));

            // Check the following statements are true:
            //
            //   (a) Exactly one of the READWRITE and READONLY flags must be set, and
            //   (b) if CREATE is set, then READWRITE must also be set, and
            //   (c) if EXCLUSIVE is set, then CREATE must also be set.
            //   (d) if DELETEONCLOSE is set, then CREATE must also be set.
            Debug.Assert((!readOnly || !readWrite) && (readWrite || readOnly));
            Debug.Assert(!create || readWrite);
            Debug.Assert(!exclusive || create);
            Debug.Assert(!delete || create);

            // The main DB, main journal, WAL file and master journal are never automatically deleted. Nor are they ever temporary files.
            Debug.Assert((!delete && !string.IsNullOrEmpty(name)) || type != OPEN.MAIN_DB);
            Debug.Assert((!delete && !string.IsNullOrEmpty(name)) || type != OPEN.MAIN_JOURNAL);
            Debug.Assert((!delete && !string.IsNullOrEmpty(name)) || type != OPEN.MASTER_JOURNAL);
            Debug.Assert((!delete && !string.IsNullOrEmpty(name)) || type != OPEN.WAL);

            // Assert that the upper layer has set one of the "file-type" flags.
            Debug.Assert(type == OPEN.MAIN_DB || type == OPEN.TEMP_DB ||
                type == OPEN.MAIN_JOURNAL || type == OPEN.TEMP_JOURNAL ||
                type == OPEN.SUBJOURNAL || type == OPEN.MASTER_JOURNAL ||
                type == OPEN.TRANSIENT_DB || type == OPEN.WAL);

            var file = (CoreVFile)id;
            //_memset(this, 0, sizeof(MemoryVFile));
            file.S = null;

            // If the second argument to this function is NULL, generate a temporary file name to use
            if (string.IsNullOrEmpty(name))
            {
                Debug.Assert(delete && !openJournal);
                name = Path.GetRandomFileName();
            }
            // Convert the filename to the system encoding.
            if (name.StartsWith("/") && !name.StartsWith("//"))
                name = name.Substring(1);
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
                        fs = new FileStream(name, creationDisposition, desiredAccess, shareMode, 4096, flagsAndAttributes);
                        Console.WriteLine("OPEN {0} ({1})", fs.GetHashCode(), fs.Name);
                    }
                    catch (Exception) { Thread.Sleep(100); }
            }
            Console.WriteLine("OPEN {0} {1} 0x{2:x} {3}", file.GetHashCode(), name, desiredAccess, fs == null ? "failed" : "ok");
            if (fs == null || fs.SafeFileHandle.IsInvalid)
            {
                file.LastErrorID = (uint)Marshal.GetLastWin32Error();
                SysEx.OSError(RC.CANTOPEN, "winOpen", name);
                return (readWrite ? Open(name, file, ((flags | OPEN.READONLY) & ~(OPEN.CREATE | OPEN.READWRITE)), out outFlags) : SysEx.CANTOPEN_BKPT());
            }
            outFlags = (readWrite ? OPEN.READWRITE : OPEN.READONLY);
            file.memset();
            file.Opened = true;
            file.S = fs;
            file.LastErrorID = 0;
            file.Vfs = this;
            file.Shm = null;
            file.Path = name;
            file.SectorSize = (uint)getSectorSize(name);
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
            return (rc == RC.INVALID && error == CoreVFileSystem.ERROR_FILE_NOT_FOUND ? RC.OK : SysEx.OSError(RC.IOERR_DELETE, "winDelete", path));
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
            catch (IOException) { SysEx.OSError(RC.IOERR_ACCESS, "winAccess", path); }
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



        #endregion












    }
}