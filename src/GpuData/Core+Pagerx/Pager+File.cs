using System;
using System.Diagnostics;
using System.Text;
using Pid = System.UInt32;
using VFSACCESS = Core.IO.VFileSystem.ACCESS;
using VFSLOCK = Core.IO.VFile.LOCK;
using VFSOPEN = Core.IO.VFileSystem.OPEN;
using System.IO;
using Core.IO;
namespace Core
{
    public partial class Pager
    {
        public RC Close()
        {
            //MallocEx.sqlite3BeginBenignMalloc();
            _exclusiveMode = false;
#if !OMIT_WAL
            _eal.sqlite3WalClose(pPager.ckptSyncFlags, pPager.pageSize, _tempSpace);
            _wal = 0;
#endif
            pager_reset();
            if (_memoryDB)
                pager_unlock();
            else
            {
                // If it is open, sync the journal file before calling UnlockAndRollback.
                // If this is not done, then an unsynced portion of the open journal file may be played back into the database. If a power failure occurs 
                // while this is happening, the database could become corrupt.
                // If an error occurs while trying to sync the journal, shift the pager into the ERROR state. This causes UnlockAndRollback to unlock the
                // database and close the journal file without attempting to roll it back or finalize it. The next database user will have to do hot-journal
                // rollback before accessing the database file.
                if (_journalFile.Open)
                    pager_error(pagerSyncHotJournal());
                pagerUnlockAndRollback();
            }
            //MallocEx.sqlite3EndBenignMalloc();
            PAGERTRACE("CLOSE {0}", PAGERID(this));
            SysEx.IOTRACE("CLOSE {0:x}", GetHashCode());
            FileEx.OSClose(_journalFile);
            FileEx.OSClose(_file);
            _pcache.Close();
            Debug.Assert(_savepoint == null && !_inJournal);
            Debug.Assert(!_journalFile.Open && !_journal2File.Open);
            return RC.OK;
        }

        private static RC readDbPage(PgHdr page)
        {
            var pPager = page.Pager;    // Pager object associated with page pPg
            var pgno = page.ID;        // Page number to read
            var rc = RC.OK;         // Return code
            var isInWal = 0;            // True if page is in log file
            var pgsz = pPager._pageSize; // Number of bytes to read
            Debug.Assert(pPager._state >= PAGER.READER && !pPager._memoryDB);
            Debug.Assert(pPager._file.Open);
            if (SysEx.NEVER(!pPager._file.Open))
            {
                Debug.Assert(pPager._tempFile);
                Array.Clear(page._Data, 0, pPager._pageSize);
                return RC.OK;
            }
            if (pPager.pagerUseWal())
                // Try to pull the page from the write-ahead log.
                rc = pPager._wal.Read(pgno, ref isInWal, pgsz, page._Data);
            if (rc == RC.OK && 0 == isInWal)
            {
                var iOffset = (pgno - 1) * (long)pPager._pageSize;
                rc = pPager._file.Read(page._Data, pgsz, iOffset);
                if (rc == RC.IOERR_SHORT_READ)
                    rc = RC.OK;
            }
            if (pgno == 1)
            {
                if (rc != 0)
                    // If the read is unsuccessful, set the dbFileVers[] to something that will never be a valid file version.  dbFileVers[] is a copy
                    // of bytes 24..39 of the database.  Bytes 28..31 should always be zero or the size of the database in page. Bytes 32..35 and 35..39
                    // should be page numbers which are never 0xffffffff.  So filling pPager.dbFileVers[] with all 0xff bytes should suffice.
                    //
                    // For an encrypted database, the situation is more complex:  bytes 24..39 of the database are white noise.  But the probability of
                    // white noising equaling 16 bytes of 0xff is vanishingly small so we should still be ok.
                    for (int i = 0; i < pPager._dbFileVers.Length; pPager._dbFileVers[i++] = 0xff) ; // memset(pPager.dbFileVers, 0xff, sizeof(pPager.dbFileVers));
                else
                    Buffer.BlockCopy(page._Data, 24, pPager._dbFileVers, 0, pPager._dbFileVers.Length);
            }
            if (CODEC1(pPager, page._Data, pgno, codec_ctx.DECRYPT))
                rc = RC.NOMEM;
            SysEx.IOTRACE("PGIN {0:x} {1}", pPager.GetHashCode(), pgno);
            PAGERTRACE("FETCH {0} page {1}% hash({2,08:x})", PAGERID(pPager), pgno, pager_pagehash(page));
            return rc;
        }

        private RC pagerPagecount(ref Pid pnPage)
        {
            // Query the WAL sub-system for the database size. The WalDbsize() function returns zero if the WAL is not open (i.e. Pager.pWal==0), or
            // if the database size is not available. The database size is not available from the WAL sub-system if the log file is empty or
            // contains no valid committed transactions.
            Debug.Assert(_state == PAGER.OPEN);
            Debug.Assert(this._lock >= VFSLOCK.SHARED || this._noReadlock != 0);
            var nPage = this._wal.DBSize();
            // If the database size was not available from the WAL sub-system, determine it based on the size of the database file. If the size
            // of the database file is not an integer multiple of the page-size, round down to the nearest page. Except, any file larger than 0
            // bytes in size is considered to contain at least one page.
            if (nPage == 0)
            {
                var n = 0L; // Size of db file in bytes
                Debug.Assert(_file.Open || _tempFile);
                if (_file.Open)
                {
                    var rc = _file.get_FileSize(ref n);
                    if (rc != RC.OK)
                        return rc;
                }
                nPage = (Pid)(n / _pageSize);
                if (nPage == 0 && n > 0)
                    nPage = 1;
            }
            // If the current number of pages in the file is greater than the configured maximum pager number, increase the allowed limit so
            // that the file can be read.
            if (nPage > this._pids)
                this._pids = (Pid)nPage;
            pnPage = nPage;
            return RC.OK;
        }

        private RC pagerOpentemp(ref VFile pFile, VFSOPEN vfsFlags)
        {
            vfsFlags |= VFSOPEN.READWRITE | VFSOPEN.CREATE | VFSOPEN.EXCLUSIVE | VFSOPEN.DELETEONCLOSE;
            VFSOPEN dummy = 0;
            var rc = FileEx.OsOpen(this._vfs, null, pFile, vfsFlags, ref dummy);
            Debug.Assert(rc != RC.OK || pFile.IsOpen);
            return rc;
        }

        public RC ReadFileHeader(int N, byte[] pDest)
        {
            var rc = RC.OK;
            Array.Clear(pDest, 0, N);
            Debug.Assert(_file.Open || _tempFile);
            // This routine is only called by btree immediately after creating the Pager object.  There has not been an opportunity to transition
            // to WAL mode yet.
            Debug.Assert(!pagerUseWal());
            if (_file.Open)
            {
                SysEx.IOTRACE("DBHDR {0} 0 {1}", this.GetHashCode(), N);
                rc = _file.Read(pDest, N, 0);
                if (rc == RC.IOERR_SHORT_READ)
                    rc = RC.OK;
            }
            return rc;
        }

        public RC SharedLock()
        {
            var rc = RC.OK;
            // This routine is only called from b-tree and only when there are no outstanding pages. This implies that the pager state should either
            // be OPEN or READER. READER is only possible if the pager is or was in exclusive access mode.
            Debug.Assert(this._pcache.RefCount() == 0);
            Debug.Assert(assert_pager_state());
            Debug.Assert(this._state == PAGER.OPEN || this._state == PAGER.READER);
            if (SysEx.NEVER(_memoryDB &&_errorCode != 0))
                return _errorCode;
            if (!pagerUseWal() && _state == PAGER.OPEN)
            {
                int bHotJournal = 1; // True if there exists a hot journal-file
                Debug.Assert(_memoryDB);
                Debug.Assert(_noReadlock == 0 || _readOnly);
                if (_noReadlock == 0)
                {
                    rc = pager_wait_on_lock(VFSLOCK.SHARED);
                    if (rc != RC.OK)
                    {
                        Debug.Assert(_lock == VFSLOCK.NO || _lock == VFSLOCK.UNKNOWN);
                        goto failed;
                    }
                }
                // If a journal file exists, and there is no RESERVED lock on the database file, then it either needs to be played back or deleted.
                if (this._lock <= VFSLOCK.SHARED)
                    rc = hasHotJournal(ref bHotJournal);
                if (rc != RC.OK)
                    goto failed;
                if (bHotJournal != 0)
                {
                    // Get an EXCLUSIVE lock on the database file. At this point it is important that a RESERVED lock is not obtained on the way to the
                    // EXCLUSIVE lock. If it were, another process might open the database file, detect the RESERVED lock, and conclude that the
                    // database is safe to read while this process is still rolling the 
                    // hot-journal back.
                    // Because the intermediate RESERVED lock is not requested, any other process attempting to access the database file will get to 
                    // this point in the code and fail to obtain its own EXCLUSIVE lock on the database file.
                    // Unless the pager is in locking_mode=exclusive mode, the lock is downgraded to SHARED_LOCK before this function returns.
                    rc = pagerLockDb(VFSLOCK.EXCLUSIVE);
                    if (rc != RC.OK)
                        goto failed;
                    // If it is not already open and the file exists on disk, open the journal for read/write access. Write access is required because 
                    // in exclusive-access mode the file descriptor will be kept open and possibly used for a transaction later on. Also, write-access 
                    // is usually required to finalize the journal in journal_mode=persist mode (and also for journal_mode=truncate on some systems).
                    // If the journal does not exist, it usually means that some other connection managed to get in and roll it back before 
                    // this connection obtained the exclusive lock above. Or, it may mean that the pager was in the error-state when this
                    // function was called and the journal file does not exist.
                    if (!this._journalFile.IsOpen)
                    {
                        var pVfs = this._vfs;
                        int bExists;              // True if journal file exists
                        rc = pVfs.xAccess(this._journal, VFSACCESS.EXISTS, out bExists);
                        if (rc == RC.OK && bExists != 0)
                        {
                            Debug.Assert(!this._tempFile);
                            VFSOPEN fout = 0;
                            rc = FileEx.OsOpen(pVfs, this._journal, this._journalFile, VFSOPEN.READWRITE | VFSOPEN.MAIN_JOURNAL, ref fout);
                            Debug.Assert(rc != RC.OK || this._journalFile.IsOpen);
                            if (rc == RC.OK && (fout & VFSOPEN.READONLY) != 0)
                            {
                                rc = SysEx.CANTOPEN_BKPT();
                                FileEx.OSClose(this._journalFile);
                            }
                        }
                    }
                    // Playback and delete the journal.  Drop the database write lock and reacquire the read lock. Purge the cache before
                    // playing back the hot-journal so that we don't end up with an inconsistent cache.  Sync the hot journal before playing
                    // it back since the process that crashed and left the hot journal probably did not sync it and we are required to always sync
                    // the journal before playing it back.
                    if (this._journalFile.IsOpen)
                    {
                        Debug.Assert(rc == RC.OK);
                        rc = pagerSyncHotJournal();
                        if (rc == RC.OK)
                        {
                            rc = pager_playback(1);
                            this._state = PAGER.OPEN;
                        }
                    }
                    else if (!this._exclusiveMode)
                        pagerUnlockDb(VFSLOCK.SHARED);
                    if (rc != RC.OK)
                    {
                        // This branch is taken if an error occurs while trying to open or roll back a hot-journal while holding an EXCLUSIVE lock. The
                        // pager_unlock() routine will be called before returning to unlock the file. If the unlock attempt fails, then Pager.eLock must be
                        // set to UNKNOWN_LOCK (see the comment above the #define for UNKNOWN_LOCK above for an explanation). 
                        // In order to get pager_unlock() to do this, set Pager.eState to PAGER_ERROR now. This is not actually counted as a transition
                        // to ERROR state in the state diagram at the top of this file, since we know that the same call to pager_unlock() will very
                        // shortly transition the pager object to the OPEN state. Calling assert_pager_state() would fail now, as it should not be possible
                        // to be in ERROR state when there are zero outstanding page references.
                        pager_error(rc);
                        goto failed;
                    }
                    Debug.Assert(this._state == PAGER.OPEN);
                    Debug.Assert((this._lock == VFSLOCK.SHARED) || (this._exclusiveMode && this._lock > VFSLOCK.SHARED));
                }
                if (!this._tempFile && (this._backup != null || this._pcache.sqlite3PcachePagecount() > 0))
                {
                    // The shared-lock has just been acquired on the database file and there are already pages in the cache (from a previous
                    // read or write transaction).  Check to see if the database has been modified.  If the database has changed, flush the
                    // cache.
                    // Database changes is detected by looking at 15 bytes beginning at offset 24 into the file.  The first 4 of these 16 bytes are
                    // a 32-bit counter that is incremented with each change.  The other bytes change randomly with each file change when
                    // a codec is in use.
                    // There is a vanishingly small chance that a change will not be detected.  The chance of an undetected change is so small that
                    // it can be neglected.
                    Pid nPage = 0;
                    var dbFileVers = new byte[this._dbFileVers.Length];
                    rc = pagerPagecount(ref nPage);
                    if (rc != 0)
                        goto failed;
                    if (nPage > 0)
                    {
                        SysEx.IOTRACE("CKVERS {0} {1}\n", this, dbFileVers.Length);
                        rc = this._file.Read(dbFileVers, dbFileVers.Length, 24);
                        if (rc != RC.OK)
                            goto failed;
                    }
                    else
                        Array.Clear(dbFileVers, 0, dbFileVers.Length);
                    if (ArrayEx.Compare(this._dbFileVers, dbFileVers, dbFileVers.Length) != 0)
                        pager_reset();
                }
                // If there is a WAL file in the file-system, open this database in WAL mode. Otherwise, the following function call is a no-op.
                rc = pagerOpenWalIfPresent();
#if !SQLITE_OMIT_WAL
                Debug.Assert(pPager.pWal == null || rc == SQLITE.OK);
#endif
            }
            if (pagerUseWal())
            {
                Debug.Assert(rc == RC.OK);
                rc = pagerBeginReadTransaction();
            }
            if (this._state == PAGER.OPEN && rc == RC.OK)
                rc = pagerPagecount(ref this._dbSize);

        failed:
            if (rc != RC.OK)
            {
                Debug.Assert(
#if SQLITE_OMIT_MEMORYDB
0==MEMDB
#else
0 == this._memoryDB
#endif
);
                pager_unlock();
                Debug.Assert(this._state == PAGER.OPEN);
            }
            else
                this._state = PAGER.READER;
            return rc;
        }

        // was:sqlite3PagerOpen
        public static RC Open(VirtualFileSystem pVfs, out Pager ppPager, string zFilename, int nExtra, PAGEROPEN flags, VFSOPEN vfsFlags, Action<PgHdr> xReinit, Func<object> memPageBuilder)
        {
            Pager pPager = null;     // Pager object to allocate and return
            byte memDb = 0;            // True if this is an in-memory file
            bool readOnly = false;   // True if this is a read-only file
            string zPathname = null; // Full path to database file
            //int nPathname = 0;       // Number of bytes in zPathname
            bool useJournal = (flags & PAGEROPEN.OMIT_JOURNAL) == 0; // False to omit journal
            bool noReadlock = (flags & PAGEROPEN.NO_READLOCK) != 0;  // True to omit read-lock
            int pcacheSize = PCache.sqlite3PcacheSize();       // Bytes to allocate for PCache
            uint szPageDflt = SQLITE_DEFAULT_PAGE_SIZE;  // Default page size
            string zUri = null;     // URI args to copy
            int nUri = 0;           // Number of bytes of URI args at *zUri
            // Figure out how much space is required for each journal file-handle (there are two of them, the main journal and the sub-journal). This
            // is the maximum space required for an in-memory journal file handle and a regular journal file-handle. Note that a "regular journal-handle"
            // may be a wrapper capable of caching the first portion of the journal file in memory to implement the atomic-write optimization (see
            // source file journal.c).
            int journalFileSize = SysEx.ROUND8(sqlite3JournalSize(pVfs) > MemJournalFile.sqlite3MemJournalSize() ? sqlite3JournalSize(pVfs) : MemJournalFile.sqlite3MemJournalSize()); // Bytes to allocate for each journal fd
            // Set the output variable to NULL in case an error occurs.
            ppPager = null;
#if !SQLITE_OMIT_MEMORYDB
            if ((flags & PAGEROPEN.MEMORY) != 0)
            {
                memDb = 1;
                zFilename = null;
            }
#endif
            // Compute and store the full pathname in an allocated buffer pointed to by zPathname, length nPathname. Or, if this is a temporary file,
            // leave both nPathname and zPathname set to 0.
            var rc = RC.OK;
            if (!string.IsNullOrEmpty(zFilename))
            {
                rc = pVfs.xFullPathname(zFilename, out zPathname);
                var z = zUri = zFilename;
                nUri = zUri.Length;
                if (rc == RC.OK && zPathname.Length + 8 > pVfs.mxPathname)
                    // This branch is taken when the journal path required by the database being opened will be more than pVfs.mxPathname
                    // bytes in length. This means the database cannot be opened, as it will not be possible to open the journal file or even
                    // check for a hot-journal before reading.
                    rc = SysEx.CANTOPEN_BKPT();
                if (rc != RC.OK)
                    return rc;
            }
            // Allocate memory for the Pager structure, PCache object, the three file descriptors, the database file name and the journal
            // file name. The layout in memory is as follows:
            //     Pager object                    (sizeof(Pager) bytes)
            //     PCache object                   (sqlite3PcacheSize() bytes)
            //     Database file handle            (pVfs.szOsFile bytes)
            //     Sub-journal file handle         (journalFileSize bytes)
            //     Main journal file handle        (journalFileSize bytes)
            //     Database file name              (nPathname+1 bytes)
            //     Journal file name               (nPathname+8+1 bytes)
            pPager = new Pager(memPageBuilder);
            pPager._pcache = new PCache();
            pPager._file = new VirtualFile();
            pPager._journal2File = new VirtualFile();
            pPager._journalFile = new VirtualFile();
            // Fill in the Pager.zFilename and Pager.zJournal buffers, if required.
            if (zPathname != null)
            {
                Debug.Assert(zPathname.Length > 0);
                pPager._filename = zPathname.ToString();
                zUri = pPager._filename;
                pPager._journal = pPager._filename + "-journal";
#if !SQLITE_OMIT_WAL
                pPager._walName = &pPager._journal[nPathname + 8 + 1];
                memcpy(pPager._walName, zPathname, nPathname);
                memcpy(&pPager._walName[nPathname], "-wal", 4);
#endif
            }
            else
                pPager._filename = string.Empty;
            pPager._vfs = pVfs;
            pPager._vfsFlags = vfsFlags;
            // Open the pager file.
            var tempFile = LOCKINGMODE.NORMAL;         // True for temp files (incl. in-memory files) 
            if (!string.IsNullOrEmpty(zFilename))
            {
                VFSOPEN fout = 0; // VFS flags returned by xOpen()
                rc = FileEx.OsOpen(pVfs, zFilename, pPager._file, vfsFlags, ref fout);
                Debug.Assert(0 == memDb);
                readOnly = (fout & VFSOPEN.READONLY) != 0;
                // If the file was successfully opened for read/write access, choose a default page size in case we have to create the
                // database file. The default page size is the maximum of:
                //    + SQLITE_DEFAULT_PAGE_SIZE,
                //    + The value returned by sqlite3OsSectorSize()
                //    + The largest page size that can be written atomically.
                if (rc == RC.OK && !readOnly)
                {
                    pPager.setSectorSize();
                    Debug.Assert(SQLITE_DEFAULT_PAGE_SIZE <= SQLITE_MAX_DEFAULT_PAGE_SIZE);
                    if (szPageDflt < pPager._sectorSize)
                        szPageDflt = (pPager._sectorSize > SQLITE_MAX_DEFAULT_PAGE_SIZE ? SQLITE_MAX_DEFAULT_PAGE_SIZE : (uint)pPager._sectorSize);
#if SQLITE_ENABLE_ATOMIC_WRITE
                    int iDc = sqlite3OsDeviceCharacteristics(pPager.fd);
                    Debug.Assert(SQLITE_IOCAP_ATOMIC512 == (512 >> 8));
                    Debug.Assert(SQLITE_IOCAP_ATOMIC64K == (65536 >> 8));
                    Debug.Assert(SQLITE_MAX_DEFAULT_PAGE_SIZE <= 65536);
                    for (var ii = szPageDflt; ii <= SQLITE_MAX_DEFAULT_PAGE_SIZE; ii = ii * 2)
                        if (iDc & (SQLITE_IOCAP_ATOMIC | (ii >> 8)))
                            szPageDflt = ii;
#endif
                }
            }
            else
            {
                // If a temporary file is requested, it is not opened immediately. In this case we accept the default page size and delay actually
                // opening the file until the first call to OsWrite().
                // This branch is also run for an in-memory database. An in-memory database is the same as a temp-file that is never written out to
                // disk and uses an in-memory rollback journal.
                tempFile = LOCKINGMODE.EXCLUSIVE;
                pPager._state = PAGER.READER;
                pPager._lock = VFSLOCK.EXCLUSIVE;
                readOnly = (vfsFlags & VFSOPEN.READONLY) != 0;
            }

            // The following call to PagerSetPagesize() serves to set the value of Pager.pageSize and to allocate the Pager.pTmpSpace buffer.
            if (rc == RC.OK)
            {
                Debug.Assert(pPager._memoryDB == 0);
                rc = pPager.SetPageSize(ref szPageDflt, -1);
            }
            // If an error occurred in either of the blocks above, free the Pager structure and close the file.
            if (rc != RC.OK)
            {
                Debug.Assert(null == pPager._tempSpace);
                FileEx.OSClose(pPager._file);
                return rc;
            }
            // Initialize the PCache object.
            Debug.Assert(nExtra < 1000);
            nExtra = SysEx.ROUND8(nExtra);
            PCache.Open((int)szPageDflt, nExtra, (memDb == 0), (memDb == 0 ? (Func<object, PgHdr, RC>)pagerStress : null), pPager, pPager._pcache);
            PAGERTRACE("OPEN {0} {1}", FILEHANDLEID(pPager._file), pPager._filename);
            SysEx.IOTRACE("OPEN {0:x} {1}", pPager.GetHashCode(), pPager._filename);
            pPager._useJournal = (byte)(useJournal ? 1 : 0);
            pPager._noReadlock = (byte)(noReadlock && readOnly ? 1 : 0);
            pPager._pids = SQLITE_MAX_PAGE_COUNT;
#if false
            Debug.Assert(pPager.state == (tempFile != 0 ? PAGER.EXCLUSIVE : PAGER.UNLOCK));
#endif
            pPager._tempFile = tempFile != 0;
            Debug.Assert(tempFile == LOCKINGMODE.NORMAL || tempFile == LOCKINGMODE.EXCLUSIVE);
            pPager._exclusiveMode = tempFile != 0;
            pPager._changeCountDone = pPager._tempFile;
            pPager._memoryDB = memDb;
            pPager._readOnly = readOnly;
            Debug.Assert(useJournal || pPager._tempFile);
            pPager._noSync = pPager._tempFile;
            pPager._fullSync = pPager._noSync;
            pPager._syncFlags = (pPager._noSync ? 0 : VirtualFile.SYNC.NORMAL);
            pPager._ckptSyncFlags = pPager._syncFlags;
            pPager._extra = (ushort)nExtra;
            pPager._journalSizeLimit = SQLITE_DEFAULT_JOURNAL_SIZE_LIMIT;
            Debug.Assert(pPager._file.IsOpen || tempFile != 0);
            pPager.setSectorSize();
            if (!useJournal)
                pPager._journalMode = JOURNALMODE.OFF;
            else if (memDb != 0)
                pPager._journalMode = JOURNALMODE.MEMORY;
            pPager._reiniter = xReinit;
            ppPager = pPager;
            return RC.OK;
        }

        public RC Sync()
        {
            var rc = RC.OK;
            if (!this._noSync)
            {
                Debug.Assert(
#if SQLITE_OMIT_MEMORYDB
0 == MEMDB
#else
0 == this._memoryDB
#endif
);
                rc = this._file.Sync(this._syncFlags);
            }
            else if (this._file.IsOpen)
            {
                Debug.Assert(
#if SQLITE_OMIT_MEMORYDB
0 == MEMDB
#else
0 == this._memoryDB
#endif
);
                var refArg = 0L;
                this._file.SetFileControl(VirtualFile.FCNTL.SYNC_OMITTED, ref refArg);
                rc = (RC)refArg;
            }
            return rc;
        }
    }
}
