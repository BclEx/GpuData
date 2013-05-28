using System.Diagnostics;
using VFSLOCK = Core.IO.VFile.LOCK;
using System;
using Core.IO;
namespace Core
{
    public partial class Pager
    {
        //private static void PAGERTRACE(string x, params object[] args) { Console.WriteLine("p:" + string.Format(x, args)); }
        //private static int PAGERID(Pager p) { return p.GetHashCode(); }
        //private static int FILEHANDLEID(VFile fd) { return fd.GetHashCode(); }

//#if DEBUG
//        internal bool assert_pager_state()
//        {
//            // Regardless of the current state, a temp-file connection always behaves as if it has an exclusive lock on the database file. It never updates
//            // the change-counter field, so the changeCountDone flag is always set.
//            Debug.Assert(!_tempFile || _lock == VFSLOCK.EXCLUSIVE);
//            Debug.Assert(!_tempFile || _changeCountDone);
//            // If the useJournal flag is clear, the journal-mode must be "OFF". And if the journal-mode is "OFF", the journal file must not be open.
//            Debug.Assert(_journalMode == JOURNALMODE.OFF || _useJournal != 0);
//            Debug.Assert(_journalMode != JOURNALMODE.OFF || !_journalFile.Open);
//            // Check that MEMDB implies noSync. And an in-memory journal. Since  this means an in-memory pager performs no IO at all, it cannot encounter 
//            // either SQLITE_IOERR or SQLITE_FULL during rollback or while finalizing a journal file. (although the in-memory journal implementation may 
//            // return SQLITE_IOERR_NOMEM while the journal file is being written). It is therefore not possible for an in-memory pager to enter the ERROR state.
//            if (_memoryDB)
//            {
//                Debug.Assert(_noSync);
//                Debug.Assert(_journalMode == JOURNALMODE.OFF || _journalMode == JOURNALMODE.MEMORY);
//                Debug.Assert(_state != PAGER.ERROR && _state != PAGER.OPEN);
//                Debug.Assert(!pagerUseWal());
//            }
//            // If changeCountDone is set, a RESERVED lock or greater must be held on the file.
//            Debug.Assert(!_changeCountDone || _lock >= VFSLOCK.RESERVED);
//            Debug.Assert(_lock != VFSLOCK.PENDING);
//            switch (_state)
//            {
//                case PAGER.OPEN:
//                    Debug.Assert(!_memoryDB);
//                    Debug.Assert(_errorCode == RC.OK);
//                    Debug.Assert(_pcache.RefCount() == 0 || _tempFile);
//                    break;
//                case PAGER.READER:
//                    Debug.Assert(_errorCode == RC.OK);
//                    Debug.Assert(_lock != VFSLOCK.UNKNOWN);
//                    Debug.Assert(_lock >= VFSLOCK.SHARED || _noReadlock != 0);
//                    break;
//                case PAGER.WRITER_LOCKED:
//                    Debug.Assert(_lock != VFSLOCK.UNKNOWN);
//                    Debug.Assert(_errorCode == RC.OK);
//                    if (!pagerUseWal())
//                        Debug.Assert(_lock >= VFSLOCK.RESERVED);
//                    Debug.Assert(_dbSize == _dbOrigSize);
//                    Debug.Assert(_dbOrigSize == _dbFileSize);
//                    Debug.Assert(_dbOrigSize == _dbHintSize);
//                    Debug.Assert(_setMaster == 0);
//                    break;
//                case PAGER.WRITER_CACHEMOD:
//                    Debug.Assert(_lock != VFSLOCK.UNKNOWN);
//                    Debug.Assert(_errorCode == RC.OK);
//                    if (!pagerUseWal())
//                    {
//                        // It is possible that if journal_mode=wal here that neither the journal file nor the WAL file are open. This happens during
//                        // a rollback transaction that switches from journal_mode=off to journal_mode=wal.
//                        Debug.Assert(_lock >= VFSLOCK.RESERVED);
//                        Debug.Assert(_journalFile.Open || _journalMode == JOURNALMODE.OFF || _journalMode == JOURNALMODE.WAL);
//                    }
//                    Debug.Assert(_dbOrigSize == _dbFileSize);
//                    Debug.Assert(_dbOrigSize == _dbHintSize);
//                    break;
//                case PAGER.WRITER_DBMOD:
//                    Debug.Assert(_lock == VFSLOCK.EXCLUSIVE);
//                    Debug.Assert(_errorCode == RC.OK);
//                    Debug.Assert(!pagerUseWal());
//                    Debug.Assert(_lock >= VFSLOCK.EXCLUSIVE);
//                    Debug.Assert(_journalFile.Open || _journalMode == JOURNALMODE.OFF || _journalMode == JOURNALMODE.WAL);
//                    Debug.Assert(_dbOrigSize <= _dbHintSize);
//                    break;
//                case PAGER.WRITER_FINISHED:
//                    Debug.Assert(_lock == VFSLOCK.EXCLUSIVE);
//                    Debug.Assert(_errorCode == RC.OK);
//                    Debug.Assert(!pagerUseWal());
//                    Debug.Assert(_journalFile.Open || _journalMode == JOURNALMODE.OFF || _journalMode == JOURNALMODE.WAL);
//                    break;
//                case PAGER.ERROR:
//                    // There must be at least one outstanding reference to the pager if in ERROR state. Otherwise the pager should have already dropped
//                    // back to OPEN state.
//                    Debug.Assert(_errorCode != RC.OK);
//                    Debug.Assert(_pcache.RefCount() > 0);
//                    break;
//            }

//            return true;
//        }
//#else
//        internal bool assert_pager_state() { return true; }
//#endif

//#if DEBUG
//        // Return a pointer to a human readable string in a static buffer containing the state of the Pager object passed as an argument. This
//        // is intended to be used within debuggers. For example, as an alternative to "print *pPager" in gdb:
//        // (gdb) printf "%s", print_pager_state(pPager)
//        internal string print_pager_state()
//        {
//            return string.Format(@"
//Filename:      {0}
//State:         {1} errCode={2}
//Lock:          {3}
//Locking mode:  locking_mode={4}
//Journal mode:  journal_mode={5}
//Backing store: tempFile={6} memDb={7} useJournal={8}
//Journal:       journalOff={9.11} journalHdr={10.11}
//Size:          dbsize={11} dbOrigSize={12} dbFileSize={13}"
//          , _filename
//          , _state == PAGER.OPEN ? "OPEN" :
//              _state == PAGER.READER ? "READER" :
//              _state == PAGER.WRITER_LOCKED ? "WRITER_LOCKED" :
//              _state == PAGER.WRITER_CACHEMOD ? "WRITER_CACHEMOD" :
//              _state == PAGER.WRITER_DBMOD ? "WRITER_DBMOD" :
//              _state == PAGER.WRITER_FINISHED ? "WRITER_FINISHED" :
//              _state == PAGER.ERROR ? "ERROR" : "?error?"
//          , (int)_errorCode
//          , _lock == VFSLOCK.NO ? "NO_LOCK" :
//              _lock == VFSLOCK.RESERVED ? "RESERVED" :
//              _lock == VFSLOCK.EXCLUSIVE ? "EXCLUSIVE" :
//              _lock == VFSLOCK.SHARED ? "SHARED" :
//              _lock == VFSLOCK.UNKNOWN ? "UNKNOWN" : "?error?"
//          , _exclusiveMode ? "exclusive" : "normal"
//          , _journalMode == JOURNALMODE.MEMORY ? "memory" :
//              _journalMode == JOURNALMODE.OFF ? "off" :
//              _journalMode == JOURNALMODE.DELETE ? "delete" :
//              _journalMode == JOURNALMODE.PERSIST ? "persist" :
//              _journalMode == JOURNALMODE.TRUNCATE ? "truncate" :
//              _journalMode == JOURNALMODE.WAL ? "wal" : "?error?"
//          , (_tempFile ? 1 : 0), (_memoryDB ? 1 : 0), (int)_useJournal
//          , _journalOff, _journalHdr
//          , (int)_dbSize, (int)_dbOrigSize, (int)_dbFileSize);
//        }
//#endif

//#if DEBUG
//        internal static void assertTruncateConstraintCb(PgHdr page) { Debug.Assert((page.Flags & PgHdr.PGHDR.DIRTY) != 0); Debug.Assert(!subjRequiresPage(page) || page.ID <= page.Pager._dbSize); }
//        internal void assertTruncateConstraint() { _pcache.IterateDirty(assertTruncateConstraintCb); }
//#else
//        internal static void assertTruncateConstraintCb(Page page) { }
//        internal void assertTruncateConstraint() { }
//#endif
    }
}
