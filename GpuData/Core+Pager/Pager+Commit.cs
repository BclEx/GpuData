using System;
using System.Diagnostics;
using DbPage = Core.PgHdr;
using Pgno = System.UInt32;
namespace Core
{
    public partial class Pager
    {
        public RC CommitPhaseOne(string master, bool noSync)
        {
            var rc = RC.OK;
            Debug.Assert(_state == PAGER.WRITER_LOCKED || _state == PAGER.WRITER_CACHEMOD || _state == PAGER.WRITER_DBMOD || _state == PAGER.ERROR);
            Debug.Assert(assert_pager_state());
            // If a prior error occurred, report that error again.
            if (WIN.NEVER(_errorCode != 0))
                return _errorCode;
            PAGERTRACE("DATABASE SYNC: File={0} zMaster={1} nSize={2}", _filename, master, _dbSize);
            // If no database changes have been made, return early.
            if (_state < PAGER.WRITER_CACHEMOD)
                return RC.OK;
            if (_inMemory)
            {
                // If this is an in-memory db, or no pages have been written to, or this function has already been called, it is mostly a no-op.  However, any
                // backup in progress needs to be restarted.
                if (_backup != null)
                    _backup.sqlite3BackupRestart();
            }
            else
            {
                if (pagerUseWal())
                {
                    var pList = _pcache.sqlite3PcacheDirtyList();
                    PgHdr pPageOne = null;
                    if (pList == null)
                    {
                        // Must have at least one page for the WAL commit flag.
                        rc = Get(1, ref pPageOne);
                        pList = pPageOne;
                        pList.Dirtys = null;
                    }
                    Debug.Assert(rc == RC.OK);
                    if (WIN.ALWAYS(pList))
                        rc = pagerWalFrames(pList, _dbSize, 1, (_fullSync ? _syncFlags : 0));
                    Unref(pPageOne);
                    if (rc == RC.OK)
                        _pcache.CleanAllPages();
                }
                else
                {
                    // The following block updates the change-counter. Exactly how it does this depends on whether or not the atomic-update optimization
                    // was enabled at compile time, and if this transaction meets the runtime criteria to use the operation:
                    //    * The file-system supports the atomic-write property for blocks of size page-size, and
                    //    * This commit is not part of a multi-file transaction, and
                    //    * Exactly one page has been modified and store in the journal file.
                    // If the optimization was not enabled at compile time, then the pager_incr_changecounter() function is called to update the change
                    // counter in 'indirect-mode'. If the optimization is compiled in but is not applicable to this transaction, call sqlite3JournalCreate()
                    // to make sure the journal file has actually been created, then call pager_incr_changecounter() to update the change-counter in indirect
                    // mode.
                    // Otherwise, if the optimization is both enabled and applicable, then call pager_incr_changecounter() to update the change-counter
                    // in 'direct' mode. In this case the journal file will never be created for this transaction.
#if ATOMIC_WRITES
                    PgHdr pPg;
                    Debug.Assert(this._journalFile.isOpen || this._journalMode == JOURNALMODE.OFF || this._journalMode == JOURNALMODE.WAL);
                    if (!master && this._journalFile.isOpen
                        && this._journalOff == jrnlBufferSize(this)
                        && this._dbSize >= this.dbOrigSize
                        && (0 == (pPg = sqlite3PcacheDirtyList(this._pcache)) || 0 == pPg.pDirty))
                        // Update the db file change counter via the direct-write method. The following call will modify the in-memory representation of page 1
                        // to include the updated change counter and then write page 1 directly to the database file. Because of the atomic-write
                        // property of the host file-system, this is safe.
                        rc = pager_incr_changecounter(this, 1);
                    else
                    {
                        rc = sqlite3JournalCreate(this._journalFile);
                        if (rc == SQLITE.OK)
                            rc = pager_incr_changecounter(this, 0);
                    }
#else
                    rc = pager_incr_changecounter(false);
#endif
                    if (rc != RC.OK)
                        goto commit_phase_one_exit;
                    // If this transaction has made the database smaller, then all pages being discarded by the truncation must be written to the journal
                    // file. This can only happen in auto-vacuum mode.
                    // Before reading the pages with page numbers larger than the current value of Pager.dbSize, set dbSize back to the value
                    // that it took at the start of the transaction. Otherwise, the calls to sqlite3PagerGet() return zeroed pages instead of
                    // reading data from the database file.
#if !OMIT_AUTOVACUUM
                    if (_dbSize < _dbOrigSize && _journalMode != JOURNALMODE.OFF)
                    {
                        var iSkip = PAGER_MJ_PGNO(this); // Pending lock page
                        var lastDbSize = this._dbSize;       // Database image size
                        this._dbSize = _dbOrigSize;
                        for (Pgno i = lastDbSize + 1; i <= _dbOrigSize; i++)
                            if (!_inJournal.Get(i) && i != iSkip)
                            {
                                PgHdr pPage = null;             // Page to journal
                                rc = Get(i, ref pPage);
                                if (rc != RC.OK)
                                    goto commit_phase_one_exit;
                                rc = Write(pPage);
                                Unref(pPage);
                                if (rc != RC.OK)
                                    goto commit_phase_one_exit;
                            }
                        this._dbSize = lastDbSize;
                    }
#endif

                    // Write the master journal name into the journal file. If a master journal file name has already been written to the journal file,
                    // or if zMaster is NULL (no master journal), then this call is a no-op.
                    rc = writeMasterJournal(master);
                    if (rc != RC.OK)
                        goto commit_phase_one_exit;
                    // Sync the journal file and write all dirty pages to the database. If the atomic-update optimization is being used, this sync will not 
                    // create the journal file or perform any real IO.
                    // Because the change-counter page was just modified, unless the atomic-update optimization is used it is almost certain that the
                    // journal requires a sync here. However, in locking_mode=exclusive on a system under memory pressure it is just possible that this is 
                    // not the case. In this case it is likely enough that the redundant xSync() call will be changed to a no-op by the OS anyhow. 
                    rc = syncJournal(0);
                    if (rc != RC.OK)
                        goto commit_phase_one_exit;
                    rc = pager_write_pagelist(_pcache.sqlite3PcacheDirtyList());
                    if (rc != RC.OK)
                    {
                        Debug.Assert(rc != RC.IOERR_BLOCKED);
                        goto commit_phase_one_exit;
                    }
                    _pcache.CleanAllPages();
                    // If the file on disk is not the same size as the database image, then use pager_truncate to grow or shrink the file here.
                    if (this._dbSize != _dbFileSize)
                    {
                        var nNew = (Pgno)(this._dbSize - (this._dbSize == PAGER_MJ_PGNO(this) ? 1 : 0));
                        Debug.Assert(this._state >= PAGER.WRITER_DBMOD);
                        rc = pager_truncate(nNew);
                        if (rc != RC.OK)
                            goto commit_phase_one_exit;
                    }
                    // Finally, sync the database file.
                    if (!noSync)
                        rc = Sync();
                    SysEx.IOTRACE("DBSYNC {0:x}", this.GetHashCode());
                }
            }
        commit_phase_one_exit:
            if (rc == RC.OK && !pagerUseWal())
                this._state = PAGER.WRITER_FINISHED;
            return rc;
        }

        // was:sqlite3PagerCommitPhaseTwo
        public RC CommitPhaseTwo()
        {
            var rc = RC.OK;
            // This routine should not be called if a prior error has occurred. But if (due to a coding error elsewhere in the system) it does get
            // called, just return the same error code without doing anything. */
            if (Check.NEVER(this._errorCode) != RC.OK)
                return this._errorCode;
            Debug.Assert(this._state == PAGER.WRITER_LOCKED || this._state == PAGER.WRITER_FINISHED || (pagerUseWal() && this._state == PAGER.WRITER_CACHEMOD));
            Debug.Assert(assert_pager_state());
            // An optimization. If the database was not actually modified during this transaction, the pager is running in exclusive-mode and is
            // using persistent journals, then this function is a no-op.
            // The start of the journal file currently contains a single journal header with the nRec field set to 0. If such a journal is used as
            // a hot-journal during hot-journal rollback, 0 changes will be made to the database file. So there is no need to zero the journal
            // header. Since the pager is in exclusive mode, there is no need to drop any locks either.
            if (this._state == PAGER.WRITER_LOCKED && this._exclusiveMode && this._journalMode == JOURNALMODE.PERSIST)
            {
                Debug.Assert(this._journalOff == JOURNAL_HDR_SZ(this) || 0 == this._journalOff);
                this._state = PAGER.READER;
                return RC.OK;
            }
            PAGERTRACE("COMMIT {0}", PAGERID(this));
            rc = pager_end_transaction(this._setMaster);
            return pager_error(rc);
        }

        public RC Rollback()
        {
            var rc = RC.OK;
            PAGERTRACE("ROLLBACK {0}", PAGERID(this));
            // PagerRollback() is a no-op if called in READER or OPEN state. If the pager is already in the ERROR state, the rollback is not 
            // attempted here. Instead, the error code is returned to the caller.
            Debug.Assert(assert_pager_state());
            if (_state == PAGER.ERROR)
                return _errorCode;
            if (_state <= PAGER.READER)
                return RC.OK;
            if (pagerUseWal())
            {
                rc = Savepoint(SAVEPOINT.ROLLBACK, -1);
                var rc2 = pager_end_transaction(this._setMaster);
                if (rc == RC.OK)
                    rc = rc2;
                rc = pager_error(rc);
            }
            else if (!this._journalFile.Open || _state == PAGER.WRITER_LOCKED)
            {
                var eState = _state;
                rc = pager_end_transaction(0);
                if (_inMemory && eState > PAGER.WRITER_LOCKED)
                {
                    // This can happen using journal_mode=off. Move the pager to the error  state to indicate that the contents of the cache may not be trusted.
                    // Any active readers will get SQLITE_ABORT.
                    _errorCode = RC.ABORT;
                    _state = PAGER.ERROR;
                    return rc;
                }
            }
            else
                rc = pager_playback(0);
            Debug.Assert(_state == PAGER.READER || rc != RC.OK);
            Debug.Assert(rc == RC.OK || rc == RC.FULL || ((int)rc & 0xFF) == (int)RC.IOERR);
            // If an error occurs during a ROLLBACK, we can no longer trust the pager cache. So call pager_error() on the way out to make any error persistent.
            return pager_error(rc);
        }

#if !OMIT_AUTOVACUUM
        public RC sqlite3PagerMovepage(DbPage pPg, Pgno pgno, int isCommit)
        {
            Debug.Assert(pPg.Refs > 0);
            Debug.Assert(_state == PAGER.WRITER_CACHEMOD || _state == PAGER.WRITER_DBMOD);
            Debug.Assert(assert_pager_state());
            // In order to be able to rollback, an in-memory database must journal the page we are moving from.
            var rc = RC.OK;
            if (!_inMemory)
            {
                rc = Write(pPg);
                if (rc != RC.OK)
                    return rc;
            }
            PgHdr pPgOld;                // The page being overwritten.
            uint needSyncPgno = 0;       // Old value of pPg.pgno, if sync is required
            Pgno origPgno;               // The original page number
            // If the page being moved is dirty and has not been saved by the latest savepoint, then save the current contents of the page into the
            // sub-journal now. This is required to handle the following scenario:
            //   BEGIN;
            //     <journal page X, then modify it in memory>
            //     SAVEPOINT one;
            //       <Move page X to location Y>
            //     ROLLBACK TO one;
            // If page X were not written to the sub-journal here, it would not be possible to restore its contents when the "ROLLBACK TO one"
            // statement were is processed.
            // subjournalPage() may need to allocate space to store pPg.pgno into one or more savepoint bitvecs. This is the reason this function
            // may return SQLITE_NOMEM.
            if ((pPg.Flags & PgHdr.PGHDR.DIRTY) != 0 && subjRequiresPage(pPg) && RC.OK != (rc = subjournalPage(pPg)))
                return rc;
            PAGERTRACE("MOVE {0} page {1} (needSync={2}) moves to {3}",
            PAGERID(this), pPg.ID, (pPg.Flags & PgHdr.PGHDR.NEED_SYNC) != 0 ? 1 : 0, pgno);
            Console.WriteLine("MOVE {0} {1} {2}", GetHashCode(), pPg.ID, pgno);
            // If the journal needs to be sync()ed before page pPg.pgno can be written to, store pPg.pgno in local variable needSyncPgno.
            // If the isCommit flag is set, there is no need to remember that the journal needs to be sync()ed before database page pPg.pgno
            // can be written to. The caller has already promised not to write to it.
            if (((pPg.Flags & PgHdr.PGHDR.NEED_SYNC) != 0) && 0 == isCommit)
            {
                needSyncPgno = pPg.ID;
                Debug.Assert(pageInJournal(pPg) || pPg.ID > _dbOrigSize);
                Debug.Assert((pPg.Flags & PgHdr.PGHDR.DIRTY) != 0);
            }
            // If the cache contains a page with page-number pgno, remove it from its hash chain. Also, if the PGHDR_NEED_SYNC was set for
            // page pgno before the 'move' operation, it needs to be retained for the page moved there.
            pPg.Flags &= ~PgHdr.PGHDR.NEED_SYNC;
            pPgOld = pager_lookup(pgno);
            Debug.Assert(null == pPgOld || pPgOld.Refs == 1);
            if (pPgOld != null)
            {
                pPg.Flags |= (pPgOld.Flags & PgHdr.PGHDR.NEED_SYNC);
                if (!_inMemory)
                    // Do not discard pages from an in-memory database since we might need to rollback later.  Just move the page out of the way.
                    PCache.MovePage(pPgOld, this._dbSize + 1);
                else
                    PCache.DropPage(pPgOld);
            }
            origPgno = pPg.ID;
            PCache.MovePage(pPg, pgno);
            PCache.MakePageDirty(pPg);
            // For an in-memory database, make sure the original page continues to exist, in case the transaction needs to roll back.  Use pPgOld
            // as the original page since it has already been allocated.
            if (!_inMemory)
            {
                Debug.Assert(pPgOld);
                PCache.MovePage(pPgOld, origPgno);
                Unref(pPgOld);
            }
            if (needSyncPgno != 0)
            {
                // If needSyncPgno is non-zero, then the journal file needs to be sync()ed before any data is written to database file page needSyncPgno.
                // Currently, no such page exists in the page-cache and the "is journaled" bitvec flag has been set. This needs to be remedied by
                // loading the page into the pager-cache and setting the PGHDR_NEED_SYNC flag.
                // If the attempt to load the page into the page-cache fails, (due to a malloc() or IO failure), clear the bit in the pInJournal[]
                // array. Otherwise, if the page is loaded and written again in this transaction, it may be written to the database file before
                // it is synced into the journal file. This way, it may end up in the journal file twice, but that is not a problem.
                PgHdr pPgHdr = null;
                rc = Get(needSyncPgno, ref pPgHdr);
                if (rc != RC.OK)
                {
                    if (needSyncPgno <= _dbOrigSize)
                    {
                        Debug.Assert(this._tempSpace != null);
                        var pTemp = new uint[this._tempSpace.Length];
                        this._inJournal.Clear(needSyncPgno, pTemp);
                    }
                    return rc;
                }
                pPgHdr.Flags |= PgHdr.PGHDR.NEED_SYNC;
                PCache.MakePageDirty(pPgHdr);
                Unref(pPgHdr);
            }
            return RC.OK;
        }
#endif
    }
}
