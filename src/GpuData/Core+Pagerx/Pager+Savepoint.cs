using System;
using System.Diagnostics;
using Pgno = System.UInt32;
using Core.Collections;
using Core.IO;
namespace Core
{
    public partial class Pager
    {
        public enum SAVEPOINT
        {
            BEGIN = 0,
            RELEASE = 1,
            ROLLBACK = 2,
        }

        private RC pagerPlaybackSavepoint(PagerSavepoint pSavepoint)
        {
            Debug.Assert(this._state != PAGER.ERROR);
            Debug.Assert(this._state >= PAGER.WRITER_LOCKED);
            // Allocate a bitvec to use to store the set of pages rolled back
            Bitvec pDone = null;     // Bitvec to ensure pages played back only once
            if (pSavepoint != null)
                pDone = new Bitvec(pSavepoint.nOrig);
            // Set the database size back to the value it was before the savepoint being reverted was opened.
            this._dbSize = pSavepoint != null ? pSavepoint.nOrig : this._dbOrigSize;
            this._changeCountDone = this._tempFile;
            if (!pSavepoint && this.pagerUseWal())
                return this.pagerRollbackWal();
            // Use pPager.journalOff as the effective size of the main rollback journal.  The actual file might be larger than this in
            // PAGER_JOURNALMODE_TRUNCATE or PAGER_JOURNALMODE_PERSIST.  But anything past pPager.journalOff is off-limits to us.
            var szJ = this._journalOff; // Effective size of the main journal
            Debug.Assert(!this.pagerUseWal() || szJ == 0);
            // Begin by rolling back records from the main journal starting at PagerSavepoint.iOffset and continuing to the next journal header.
            // There might be records in the main journal that have a page number greater than the current database size (pPager.dbSize) but those
            // will be skipped automatically.  Pages are added to pDone as they are played back.
            long iHdrOff;             // End of first segment of main-journal records
            var rc = RC.OK;      // Return code
            if (pSavepoint != null && !this.pagerUseWal())
            {
                iHdrOff = (pSavepoint.iHdrOffset != 0 ? pSavepoint.iHdrOffset : szJ);
                this._journalOff = pSavepoint.iOffset;
                while (rc == RC.OK && this._journalOff < iHdrOff)
                    rc = pager_playback_one_page(ref this._journalOff, pDone, 1, 1);
                Debug.Assert(rc != RC.DONE);
            }
            else
                this._journalOff = 0;
            // Continue rolling back records out of the main journal starting at the first journal header seen and continuing until the effective end
            // of the main journal file.  Continue to skip out-of-range pages and continue adding pages rolled back to pDone.
            while (rc == RC.OK && this._journalOff < szJ)
            {
                uint nJRec;         // Number of Journal Records
                uint dummy;
                rc = readJournalHdr(0, szJ, out nJRec, out dummy);
                Debug.Assert(rc != RC.DONE);
                // The "pPager.journalHdr+JOURNAL_HDR_SZ(pPager)==pPager.journalOff" test is related to ticket #2565.  See the discussion in the
                // pager_playback() function for additional information.
                if (nJRec == 0 && this._journalHdr + JOURNAL_HDR_SZ(this) >= this._journalOff)
                    nJRec = (uint)((szJ - this._journalOff) / JOURNAL_PG_SZ(this));
                for (uint ii = 0; rc == RC.OK && ii < nJRec && this._journalOff < szJ; ii++)
                    rc = pager_playback_one_page(ref this._journalOff, pDone, 1, 1);
                Debug.Assert(rc != RC.DONE);
            }
            Debug.Assert(rc != RC.OK || this._journalOff >= szJ);
            // Finally,  rollback pages from the sub-journal.  Page that were previously rolled back out of the main journal (and are hence in pDone)
            // will be skipped.  Out-of-range pages are also skipped.
            if (pSavepoint != null)
            {
                long offset = pSavepoint.iSubRec * (4 + this._pageSize);
                if (this.pagerUseWal())
                    rc = this._wal.SavepointUndo(pSavepoint.aWalData);
                for (var ii = pSavepoint.iSubRec; rc == RC.OK && ii < this._nSubRec; ii++)
                {
                    Debug.Assert(offset == ii * (4 + this._pageSize));
                    rc = pager_playback_one_page(ref offset, pDone, 0, 1);
                }
                Debug.Assert(rc != RC.DONE);
            }
            Bitvec.Destroy(ref pDone);
            if (rc == RC.OK)
                this._journalOff = (int)szJ;
            return rc;
        }

        //private void releaseAllSavepoints()
        //{
        //    for (var ii = 0; ii < nSavepoint; ii++)
        //        Bitvec.Destroy(ref _savepoint[ii].pInSavepoint);
        //    if (!_exclusiveMode || _journal2File is MemJournalFile)
        //        FileEx.OSClose(_journal2File);
        //    _savepoint = null;
        //    nSavepoint = 0;
        //    _nSubRec = 0;
        //}

        //private RC addToSavepointBitvecs(Pgno pgno)
        //{
        //    var rc = RC.OK;
        //    for (var ii = 0; ii < nSavepoint; ii++)
        //    {
        //        var p = _savepoint[ii];
        //        if (pgno <= p.nOrig)
        //        {
        //            rc |= p.pInSavepoint.Set(pgno);
        //            Debug.Assert(rc == RC.OK || rc == RC.NOMEM);
        //        }
        //    }
        //    return rc;
        //}

        // was:sqlite3PagerOpenSavepoint
        public RC OpenSavepoint(int nSavepoint)
        {
            var rc = RC.OK;
            var nCurrent = this.nSavepoint;        // Current number of savepoints 
            Debug.Assert(this._state >= PAGER.WRITER_LOCKED);
            Debug.Assert(assert_pager_state());
            if (nSavepoint > nCurrent && this._useJournal != 0)
            {
                // Grow the Pager.aSavepoint array using realloc(). Return SQLITE_NOMEM if the allocation fails. Otherwise, zero the new portion in case a 
                // malloc failure occurs while populating it in the for(...) loop below.
                Array.Resize(ref this._savepoint, nSavepoint);
                var aNew = this._savepoint; // New Pager.aSavepoint array
                // Populate the PagerSavepoint structures just allocated.
                for (var ii = nCurrent; ii < nSavepoint; ii++)
                {
                    aNew[ii] = new PagerSavepoint();
                    aNew[ii].nOrig = this._dbSize;
                    aNew[ii].iOffset = (this._journalFile.IsOpen && this._journalOff > 0 ? this._journalOff : (int)JOURNAL_HDR_SZ(this));
                    aNew[ii].iSubRec = this._nSubRec;
                    aNew[ii].pInSavepoint = new Bitvec(this._dbSize);
                    if (pagerUseWal())
                        this._wal.Savepoint(aNew[ii].aWalData);
                    this.nSavepoint = ii + 1;
                }
                Debug.Assert(this.nSavepoint == nSavepoint);
                assertTruncateConstraint();
            }
            return rc;
        }

        // was:sqlite3PagerSavepoint
        public RC Savepoint(SAVEPOINT op, int iSavepoint)
        {
            var rc = this._errorCode;
            Debug.Assert(op == SAVEPOINT.RELEASE || op == SAVEPOINT.ROLLBACK);
            Debug.Assert(iSavepoint >= 0 || op == SAVEPOINT.ROLLBACK);
            if (rc == RC.OK && iSavepoint < this.nSavepoint)
            {
                // Figure out how many savepoints will still be active after this operation. Store this value in nNew. Then free resources associated
                // with any savepoints that are destroyed by this operation.
                var nNew = iSavepoint + ((op == SAVEPOINT.RELEASE) ? 0 : 1); // Number of remaining savepoints after this op.
                for (var ii = nNew; ii < this.nSavepoint; ii++)
                    Bitvec.Destroy(ref this._savepoint[ii].pInSavepoint);
                this.nSavepoint = nNew;
                // If this is a release of the outermost savepoint, truncate the sub-journal to zero bytes in size.
                if (op == SAVEPOINT.RELEASE)
                    if (nNew == 0 && this._journal2File.IsOpen)
                    {
                        // Only truncate if it is an in-memory sub-journal.
                        if (this._journal2File is MemJournalFile)
                        {
                            rc = this._journal2File.Truncate(0);
                            Debug.Assert(rc == RC.OK);
                        }
                        this._nSubRec = 0;
                    }
                    // Else this is a rollback operation, playback the specified savepoint. If this is a temp-file, it is possible that the journal file has
                    // not yet been opened. In this case there have been no changes to the database file, so the playback operation can be skipped.
                    else if (pagerUseWal() || this._journalFile.IsOpen)
                    {
                        var pSavepoint = (nNew == 0 ? (PagerSavepoint)null : this._savepoint[nNew - 1]);
                        rc = pagerPlaybackSavepoint(pSavepoint);
                        Debug.Assert(rc != RC.DONE);
                    }
            }
            return rc;
        }
    }
}
