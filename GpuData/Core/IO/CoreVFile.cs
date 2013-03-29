using System;
using System.Diagnostics;
using System.IO;
using System.Runtime.InteropServices;
using System.Threading;
namespace Core.IO
{
    public class CoreVFile : VFile
    {
        private static int MX_CLOSE_ATTEMPT = 3;
        private static int RESERVED_BYTE = (PENDING_BYTE + 1);
        private static int SHARED_FIRST = (PENDING_BYTE + 2);
        private static int SHARED_SIZE = 510;

        public override RC Close()
        {
            Debug.Assert(Shm == null);
            Console.WriteLine("CLOSE {0} ({1})", S.GetHashCode(), S.Name);
            bool rc;
            int cnt = 0;
            do
            {
                S.Close();
                rc = true;
            } while (!rc && ++cnt < MX_CLOSE_ATTEMPT);
            return (rc ? RC.OK : WIN._Error(RC.IOERR_CLOSE, "winClose", Path));
        }

        public override RC Read(byte[] buffer, int amount, long offset)
        {
            if (buffer == null)
                buffer = new byte[amount];
            Console.WriteLine("READ {0} lock={1}", S.GetHashCode(), LockType);
            if (!S.CanRead)
                return RC.IOERR_READ;
            if (seekWinFile(offset) != 0)
                return RC.FULL;
            int read; // number of bytes actually read from file
            try { read = S.Read(buffer, 0, amount); }
            catch (Exception) { LastErrorID = (uint)Marshal.GetLastWin32Error(); return WIN._Error(RC.IOERR_READ, "winRead", Path); }
            if (read < amount)
            {
                // unread parts of the buffer must be zero-filled
                Array.Clear(buffer, (int)read, (int)(amount - read));
                return RC.IOERR_SHORT_READ;
            }
            return RC.OK;
        }

        public override RC Write(byte[] buffer, int amount, long offset)
        {
            Debug.Assert(amount > 0);
            Console.WriteLine("WRITE {0} lock={1}", S.GetHashCode(), LockType);
            var rc = seekWinFile(offset);
            long wrote = S.Position;
            try
            {
                Debug.Assert(buffer.Length >= amount);
                S.Write(buffer, 0, amount);
                rc = 1;
                wrote = S.Position - wrote;
            }
            catch (IOException) { return RC.READONLY; }
            if (rc == 0 || amount > (int)wrote)
            {
                LastErrorID = (uint)Marshal.GetLastWin32Error();
                return (LastErrorID == WIN.ERROR_HANDLE_DISK_FULL || LastErrorID == WIN.ERROR_DISK_FULL ? RC.FULL : WIN._Error(RC.IOERR_WRITE, "winWrite", Path));
            }
            return RC.OK;
        }

        public override RC Truncate(long size)
        {
            var rc = RC.OK;
            Console.WriteLine("TRUNCATE {0} {1,11}", S.Name, size);
            // if the user has configured a chunk-size for this file, truncate the file so that it consists of an integer number of chunks (i.e. the
            // actual file size after the operation may be larger than the requested size).
            if (Chunk != 0)
                size = ((size + Chunk - 1) / Chunk) * Chunk;
            try { S.SetLength(size); rc = RC.OK; }
            catch (IOException) { LastErrorID = (uint)Marshal.GetLastWin32Error(); rc = WIN._Error(RC.IOERR_TRUNCATE, "winTruncate2", Path); }
            Console.WriteLine("TRUNCATE {0} {1,%11} {2}", S.GetHashCode(), size, rc == RC.OK ? "ok" : "failed");
            return rc;
        }

        public override RC Sync(SYNC flags)
        {
            // Check that one of SQLITE_SYNC_NORMAL or FULL was passed 
            Debug.Assert(((int)flags & 0x0F) == (int)SYNC.NORMAL || ((int)flags & 0x0F) == (int)SYNC.FULL);
            Console.WriteLine("SYNC {0} lock={1}", S.GetHashCode(), LockType);
#if _NO_SYNC
			return SQLITE.OK;
#else
            S.Flush();
            return RC.OK;
#endif
        }

        public override RC FileSize(ref long size)
        {
            size = (S.CanRead ? S.Length : 0);
            return RC.OK;
        }

        public override RC Lock(LOCK lockType)
        {
            Console.WriteLine("LOCK {0} {1} was {2}({3})", S.GetHashCode(), lockType, LockType, SharedLockByte);
            // if there is already a lock of this type or more restrictive on the OsFile, do nothing. Don't use the end_lock: exit path, as sqlite3OsEnterMutex() hasn't been called yet.
            if (LockType >= lockType)
                return RC.OK;
            // make sure the locking sequence is correct
            Debug.Assert(LockType != LOCK.NO || lockType == LOCK.SHARED);
            Debug.Assert(lockType != LOCK.PENDING);
            Debug.Assert(lockType != LOCK.RESERVED || LockType == LOCK.SHARED);
            // lock the PENDING_LOCK byte if we need to acquire a PENDING lock or a SHARED lock.  if we are acquiring a SHARED lock, the acquisition of
            // the PENDING_LOCK byte is temporary.
            uint errorID = 0;
            var res = 1; // Result of a windows lock call
            var gotPendingLock = false; // True if we acquired a PENDING lock this time
            var newLockType = LockType;
            if (LockType == LOCK.NO || (lockType == LOCK.EXCLUSIVE && LockType == LOCK.RESERVED))
            {
                res = 0;
                var cnt = 3;
                while (cnt-- > 0 && res == 0)
                    try { _lockingStrategy.LockFile(this, PENDING_BYTE, 1); res = 1; }
                    catch (Exception)
                    {
                        // try 3 times to get the pending lock.  the pending lock might be held by another reader process who will release it momentarily.
                        Console.WriteLine("could not get a PENDING lock. cnt={0}", cnt);
                        Thread.Sleep(1);
                    }
                gotPendingLock = (res != 0);
                if (res == 0)
                    errorID = (uint)Marshal.GetLastWin32Error();
            }
            // acquire a shared lock
            if (lockType == LOCK.SHARED && res != 0)
            {
                Debug.Assert(LockType == LOCK.NO);
                res = getReadLock();
                if (res != 0)
                    newLockType = LOCK.SHARED;
                else
                    errorID = (uint)Marshal.GetLastWin32Error();
            }
            // acquire a RESERVED lock
            if (lockType == LOCK.RESERVED && res != 0)
            {
                Debug.Assert(LockType == LOCK.SHARED);
                try { _lockingStrategy.LockFile(this, RESERVED_BYTE, 1); newLockType = LOCK.RESERVED; res = 1; }
                catch (Exception) { res = 0; errorID = (uint)Marshal.GetLastWin32Error(); }
                if (res != 0)
                    newLockType = LOCK.RESERVED;
                else
                    errorID = (uint)Marshal.GetLastWin32Error();
            }
            // acquire a PENDING lock
            if (lockType == LOCK.EXCLUSIVE && res != 0)
            {
                newLockType = LOCK.PENDING;
                gotPendingLock = false;
            }
            // acquire an EXCLUSIVE lock
            if (lockType == LOCK.EXCLUSIVE && res != 0)
            {
                Debug.Assert(this.LockType >= LOCK.SHARED);
                res = unlockReadLock();
                Console.WriteLine("unreadlock = {0}", res);
                try { _lockingStrategy.LockFile(this, SHARED_FIRST, SHARED_SIZE); newLockType = LOCK.EXCLUSIVE; res = 1; }
                catch (Exception) { res = 0; }
                if (res != 0)
                    newLockType = LOCK.EXCLUSIVE;
                else
                {
                    errorID = (uint)Marshal.GetLastWin32Error();
                    Console.WriteLine("error-code = {0}", errorID);
                    getReadLock();
                }
            }
            // if we are holding a PENDING lock that ought to be released, then release it now.
            if (gotPendingLock && lockType == LOCK.SHARED)
                _lockingStrategy.UnlockFile(this, PENDING_BYTE, 1);
            // update the state of the lock has held in the file descriptor then return the appropriate result code.
            var rc = RC.OK;
            if (res != 0)
                rc = RC.OK;
            else
            {
                Console.WriteLine("LOCK FAILED {0} trying for {1} but got {2}", S.GetHashCode(), lockType, newLockType);
                LastErrorID = errorID;
                rc = RC.BUSY;
            }
            LockType = newLockType;
            return rc;
        }

        public override RC Unlock(LOCK lockType)
        {
            Debug.Assert(lockType <= LOCK.SHARED);
            Console.WriteLine("UNLOCK {0} to {1} was {2}({3})", S.GetHashCode(), lockType, LockType, SharedLockByte);
            var rc = RC.OK;
            var type = LockType;
            if (type >= LOCK.EXCLUSIVE)
            {
                _lockingStrategy.UnlockFile(this, SHARED_FIRST, SHARED_SIZE);
                if (lockType == LOCK.SHARED && getReadLock() == 0)
                    // this should never happen.  We should always be able to reacquire the read lock 
                    rc = WIN._Error(RC.IOERR_UNLOCK, "winUnlock", Path);
            }
            if (type >= LOCK.RESERVED)
                try { _lockingStrategy.UnlockFile(this, RESERVED_BYTE, 1); }
                catch (Exception) { }
            if (lockType == LOCK.NO && lockType >= LOCK.SHARED)
                unlockReadLock();
            if (type >= LOCK.PENDING)
            {
                try { _lockingStrategy.UnlockFile(this, PENDING_BYTE, 1); }
                catch (Exception) { }
            }
            LockType = lockType;
            return rc;
        }

        public override RC CheckReservedLock(ref int outRC)
        {
            int rc;
            if (LockType >= LOCK.RESERVED)
            {
                rc = 1;
                Console.WriteLine("TEST WR-LOCK {0} {1} (local)", S.Name, rc);
            }
            else
            {
                try
                {
                    _lockingStrategy.LockFile(this, RESERVED_BYTE, 1);
                    _lockingStrategy.UnlockFile(this, RESERVED_BYTE, 1);
                    rc = 1;
                }
                catch (IOException) { rc = 0; }
                rc = 1 - rc;
                Console.WriteLine("TEST WR-LOCK {0} {1} (remote)", S.GetHashCode(), rc);
            }
            outRC = rc;
            return RC.OK;
        }

        public override RC FileControl(FCNTL op, ref long pArg)
        {
            switch (op)
            {
                case FCNTL.LOCKSTATE:
                    pArg = (int)LockType;
                    return RC.OK;
                case FCNTL.LAST_ERRNO:
                    pArg = (int)LastErrorID;
                    return RC.OK;
                case FCNTL.CHUNK_SIZE:
                    Chunk = (int)pArg;
                    return RC.OK;
                case FCNTL.SIZE_HINT:
                    var sz = (long)pArg;
                    Truncate(sz);
                    return RC.OK;
                case FCNTL.SYNC_OMITTED:
                    return RC.OK;
            }
            return RC.NOTFOUND;
        }

        private int seekWinFile(long iOffset)
        {
            try { S.Seek(iOffset, SeekOrigin.Begin); }
            catch (Exception) { LastErrorID = (uint)Marshal.GetLastWin32Error(); WIN._Error(RC.IOERR_SEEK, "seekWinFile", Path); return 1; }
            return 0;
        }

        private int getReadLock()
        {
            var res = 0;
            if (Environment.OSVersion.Platform >= PlatformID.Win32NT)
                res = _lockingStrategy.SharedLockFile(this, SHARED_FIRST, SHARED_SIZE);
            // isNT() is 1 if SQLITE_OS_WINCE==1, so this else is never executed.
            if (res == 0)
                LastErrorID = (uint)Marshal.GetLastWin32Error();
            // no need to log a failure to lock 
            return res;
        }

        private int unlockReadLock()
        {
            var res = 1;
            if (Environment.OSVersion.Platform >= PlatformID.Win32NT)
                try { _lockingStrategy.UnlockFile(this, SHARED_FIRST, SHARED_SIZE); }
                catch (Exception) { res = 0; }
            if (res == 0) { LastErrorID = (uint)Marshal.GetLastWin32Error(); WIN._Error(RC.IOERR_UNLOCK, "unlockReadLock", Path); }
            return res;
        }

        #region Locking

        private static LockingStrategy _lockingStrategy = (WIN.IsRunningMediumTrust() ? new MediumTrustLockingStrategy() : new LockingStrategy());

        /// <summary>
        /// Basic locking strategy for Console/Winform applications
        /// </summary>
        private class LockingStrategy
        {
            const int LOCKFILE_FAIL_IMMEDIATELY = 1;
            [DllImport("kernel32.dll")]
            static extern bool LockFileEx(IntPtr hFile, uint dwFlags, uint dwReserved, uint nNumberOfBytesToLockLow, uint nNumberOfBytesToLockHigh, [In] ref System.Threading.NativeOverlapped lpOverlapped);
            
            public virtual void LockFile(VFile file, long offset, long length)
            {
                file.S.Lock(offset, length);
            }

            public virtual int SharedLockFile(VFile file, long offset, long length)
            {
                Debug.Assert(length == SHARED_SIZE);
                Debug.Assert(offset == SHARED_FIRST);
                var ovlp = new NativeOverlapped();
                ovlp.OffsetLow = (int)offset;
                ovlp.OffsetHigh = 0;
                ovlp.EventHandle = IntPtr.Zero;
                return (LockFileEx(file.S.Handle, LOCKFILE_FAIL_IMMEDIATELY, 0, (uint)length, 0, ref ovlp) ? 1 : 0);
            }

            public virtual void UnlockFile(VFile file, long offset, long length)
            {
                file.S.Unlock(offset, length);
            }
        }

        /// <summary>
        /// Locking strategy for Medium Trust. It uses the same trick used in the native code for WIN_CE
        /// which doesn't support LockFileEx as well.
        /// </summary>
        private class MediumTrustLockingStrategy : LockingStrategy
        {
            public override int SharedLockFile(VFile file, long offset, long length)
            {
                Debug.Assert(length == SHARED_SIZE);
                Debug.Assert(offset == SHARED_FIRST);
                try { file.S.Lock(offset + file.SharedLockByte, 1); }
                catch (IOException) { return 0; }
                return 1;
            }
        }

        #endregion
    }
}