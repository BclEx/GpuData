using System;
using System.Diagnostics;
using Pid = System.UInt32;
namespace Core
{
    public partial class Pager
    {
        public RC Get(Pid pid, ref PgHdr page, bool noContent = false)
        {
            Debug.Assert(_state >= PAGER.READER);
            Debug.Assert(assert_pager_state());
            if (pid == 0)
                return SysEx.CORRUPT_BKPT();
            // If the pager is in the error state, return an error immediately.  Otherwise, request the page from the PCache layer.
            var rc = (_errorCode != RC.OK ? _errorCode : _pcache.Fetch(pid, 1, out page));
            if (rc != RC.OK)
            {
                page = null;
                goto pager_get_err;
            }
            Debug.Assert(page.ID == pid);
            Debug.Assert(page.Pager == this || page.Pager == null);
            if (page.Pager != null && !noContent)
            {
                // In this case the pcache already contains an initialized copy of the page. Return without further ado.
                Debug.Assert(pid <= MAX_PID && pid != MJ_PID(this));
                return RC.OK;
            }
            // The pager cache has created a new page. Its content needs to be initialized.
            page.Pager = this;
            page.Extra = _memPageBuilder;
            // The maximum page number is 2^31. Return CORRUPT if a page number greater than this, or the unused locking-page, is requested.
            if (pid > MAX_PID || pid == MJ_PID(this))
            {
                rc = SysEx.CORRUPT_BKPT();
                goto pager_get_err;
            }
            if (_memoryDB || _dbSize < pid || noContent || !_file.Open)
            {
                if (pid > _pids)
                {
                    rc = RC.FULL;
                    goto pager_get_err;
                }
                if (noContent)
                {
                    if (pid <= _dbOrigSize)
                        _inJournal.Set(pid);
                    addToSavepointBitvecs(pid);
                }
                Array.Clear(page._Data, 0, _pageSize);
                SysEx.IOTRACE("ZERO {0:x} {1}\n", GetHashCode(), pid);
            }
            else
            {
                Debug.Assert(page.Pager == this);
                rc = readDbPage(page);
                if (rc != RC.OK)
                    goto pager_get_err;
            }
            pager_set_pagehash(page);
            return RC.OK;
        pager_get_err:
            Debug.Assert(rc != RC.OK);
            if (page != null)
                PCache.DropPage(page);
            UnlockIfUnused();
            page = null;
            return rc;
        }

        public PgHdr Lookup(Pid pid)
        {
            Debug.Assert(pid != 0);
            Debug.Assert(_pcache != null);
            Debug.Assert(_state >= PAGER.READER && _state != PAGER.ERROR);
            PgHdr page;
            _pcache.Fetch(pid, 0, out page);
            return page;
        }

        public static void Unref(PgHdr page)
        {
            if (page != null)
            {
                var pager = page.Pager;
                PCache.ReleasePage(page);
                pager.UnlockIfUnused();
            }
        }
    }
}
