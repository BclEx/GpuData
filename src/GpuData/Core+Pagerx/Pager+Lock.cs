using System.Diagnostics;
using VFSLOCK = Core.IO.VFile.LOCK;
namespace Core
{
    public partial class Pager
    {
        //private void UnlockIfUnused()
        //{
        //    if (this._pcache.RefCount() == 0)
        //        pagerUnlockAndRollback();
        //}

        //private void pagerUnlockAndRollback()
        //{
        //    if (this._state != PAGER.ERROR && this._state != PAGER.OPEN)
        //    {
        //        Debug.Assert(assert_pager_state());
        //        if (this._state >= PAGER.WRITER_LOCKED)
        //        {
        //            MallocEx.BeginBenignMalloc();
        //            Rollback();
        //            MallocEx.EndBenignMalloc();
        //        }
        //        else if (!this._exclusiveMode)
        //        {
        //            Debug.Assert(this._state == PAGER.READER);
        //            pager_end_transaction(0);
        //        }
        //    }
        //    pager_unlock();
        //}

        //private RC pagerUnlockDb(VFSLOCK eLock)
        //{
        //    var rc = RC.OK;
        //    Debug.Assert(!this._exclusiveMode || this._lock == eLock);
        //    Debug.Assert(eLock == VFSLOCK.NO || eLock == VFSLOCK.SHARED);
        //    Debug.Assert(eLock != VFSLOCK.NO || !this.pagerUseWal());
        //    if (this._file.IsOpen)
        //    {
        //        Debug.Assert(this._lock >= eLock);
        //        rc = this._file.Unlock(eLock);
        //        if (this._lock != VFSLOCK.UNKNOWN)
        //            this._lock = eLock;
        //        SysEx.IOTRACE("UNLOCK {0:x} {1}", this.GetHashCode(), eLock);
        //    }
        //    return rc;
        //}

        //private RC pagerLockDb(VFSLOCK eLock)
        //{
        //    var rc = RC.OK;
        //    Debug.Assert(eLock == VFSLOCK.SHARED || eLock == VFSLOCK.RESERVED || eLock == VFSLOCK.EXCLUSIVE);
        //    if (this._lock < eLock || this._lock == VFSLOCK.UNKNOWN)
        //    {
        //        rc = this._file.Lock(eLock);
        //        if (rc == RC.OK && (this._lock != VFSLOCK.UNKNOWN || eLock == VFSLOCK.EXCLUSIVE))
        //        {
        //            this._lock = eLock;
        //            SysEx.IOTRACE("LOCK {0:x} {1}", this.GetHashCode(), eLock);
        //        }
        //    }
        //    return rc;
        //}

        private RC sqlite3PagerExclusiveLock()
        {
            var rc = RC.OK;
            Debug.Assert(this._state == PAGER.WRITER_CACHEMOD || this._state == PAGER.WRITER_DBMOD || this._state == PAGER.WRITER_LOCKED);
            Debug.Assert(assert_pager_state());
            if (!pagerUseWal())
                rc = pager_wait_on_lock(VFSLOCK.EXCLUSIVE);
            return rc;
        }
    }
}
