using Pid = System.UInt32;
using IPage = Core.PgHdr;
using System;
using Core.IO;
using System.Diagnostics;

namespace Core
{
    public partial class Pager
    {
        public enum PAGER : byte
        {
            OPEN = 0,
            READER = 1,
            WRITER_LOCKED = 2,
            WRITER_CACHEMOD = 3,
            WRITER_DBMOD = 4,
            WRITER_FINISHED = 5,
            ERROR = 6,
        }

        const int MAX_SECTOR_SIZE = 0x10000;

        public class PagerSavepoint
        {
            public long Offset;         // Starting offset in main journal
            public long HdrOffset;      // See above
            public Bitvec InSavepoint;  // Set of pages in this savepoint
            public Pid Orig;            // Original number of pages in file
            public Pid SubRec;          // Index of first record in sub-journal
#if !OMIT_WAL
            public uint WalData[WAL_SAVEPOINT_NDATA];        // WAL savepoint context
#else
            public object WalData = null;      // Used for C# convenience
#endif
            // For C#
            //public static implicit operator bool(PagerSavepoint b) { return (b != null); }
        }

        public VFileSystem Vfs;             // OS functions to use for IO
        public bool ExclusiveMode;          // Boolean. True if locking_mode==EXCLUSIVE
        public IPager.JOURNALMODE JournalMode;     // One of the PAGER_JOURNALMODE_* values
        public byte UseJournal;             // Use a rollback journal on this file
        public bool NoSync;                 // Do not sync the journal if true
        public bool FullSync;               // Do extra syncs of the journal for robustness
        public VFile.SYNC CheckpointSyncFlags;    // SYNC_NORMAL or SYNC_FULL for checkpoint
        public VFile.SYNC WalSyncFlags;     // SYNC_NORMAL or SYNC_FULL otherwise
        public VFile.SYNC SyncFlags;        // SYNC_NORMAL or SYNC_FULL otherwise
        public bool TempFile;               // zFilename is a temporary file
        public bool ReadOnly;               // True for a read-only database
        public bool MemoryDB;               // True to inhibit all file I/O
        // The following block contains those class members that change during routine opertion.  Class members not in this block are either fixed
        // when the pager is first created or else only change when there is a significant mode change (such as changing the page_size, locking_mode,
        // or the journal_mode).  From another view, these class members describe the "state" of the pager, while other class members describe the "configuration" of the pager.
        public PAGER State;                 // Pager state (OPEN, READER, WRITER_LOCKED..) 
        public VFile.LOCK Lock;             // Current lock held on database file 
        public bool ChangeCountDone;        // Set after incrementing the change-counter 
        public bool SetMaster;              // True if a m-j name has been written to jrnl 
        public byte DoNotSpill;             // Do not spill the cache when non-zero 
        public byte DoNotSyncSpill;         // Do not do a spill that requires jrnl sync 
        public byte SubjInMemory;           // True to use in-memory sub-journals 
        public Pid DBSize;                  // Number of pages in the database 
        public Pid DBOrigSize;              // dbSize before the current transaction 
        public Pid DBFileSize;              // Number of pages in the database file 
        public Pid DBHintSize;              // Value passed to FCNTL_SIZE_HINT call 
        public RC ErrorCode;                // One of several kinds of errors 
        public int _nRec;                   // Pages journalled since last j-header written 
        public uint ChecksumInit;           // Quasi-random value added to every checksum 
        public uint SubRecords;             // Number of records written to sub-journal 
        public Bitvec InJournal;            // One bit for each page in the database file 
        public VFile File;                  // File descriptor for database 
        public VFile JournalFile;           // File descriptor for main journal 
        public VFile SubJournalFile;        // File descriptor for sub-journal 
        public long JournalOffset;          // Current write offset in the journal file 
        public long JournalHeader;          // Byte offset to previous journal header 
        public IBackup Backup;              // Pointer to list of ongoing backup processes 
        public PagerSavepoint[] Savepoint;  // Array of active savepoints 
        public byte[] DBFileVersions = new byte[16];    // Changes whenever database file changes
        // End of the routinely-changing class members
        public ushort ExtraBytes;           // Add this many bytes to each in-memory page
        public short ReserveBytes;          // Number of unused bytes at end of each page
        public VFileSystem.OPEN VfsFlags;   // Flags for VirtualFileSystem.xOpen() 
        public uint SectorSize;             // Assumed sector size during rollback 
        public int PageSize;                // Number of bytes in a page 
        public Pid MaxPid;                  // Maximum allowed size of the database 
        public long JournalSizeLimit;       // Size limit for persistent journal files 
        public string Filename;             // Name of the database file 
        public string Journal;              // Name of the journal file 
        public Func<object, int> BusyHandler;  // Function to call when busy 
        public object BusyHandlerArg;       // Context argument for xBusyHandler 
        public int[] Stats = new int[3];    // Total cache hits, misses and writes
#if TEST
        public int Reads;                   // Database pages read
#endif
        public Action<IPage> Reiniter;	    // Call this routine when reloading pages
#if HAS_CODEC
        public Func<object, object, Pid, int, object> Codec;    // Routine for en/decoding data
        public Action<object, int, int> CodecSizeChange;        // Notify of page size changes
        public Action<object> CodecFree;                        // Destructor for the codec
        public object CodecArg;                                 // First argument to xCodec... methods
#endif
        public byte[] TmpSpace;				// Pager.pageSize bytes of space for tmp use
        public PCache PCache;				// Pointer to page cache object
#if !OMIT_WAL
        public Wal Wal;					    // Write-ahead log used by "journal_mode=wal"
        public char WalName;                // File name for write-ahead log
#else
        public Wal Wal;             // Having this dummy here makes C# easier
#endif

        public enum STAT : byte
        {
            HIT = 0,
            MISS = 1,
            WRITE = 2,
        }

        const int MAX_PID = 2147483647;

#if !OMIT_WAL
        internal static bool UseWal(Pager pager) { return (pPager.pWal != 0); }
#else
        internal bool UseWal() { return false; }
        internal RC RollbackWal() { return RC.OK; }
        internal RC WalFrames(PgHdr w, Pid x, int y, VFile.SYNC z) { return RC.OK; }
        internal RC OpenWalIfPresent() { return RC.OK; }
        internal RC BeginReadTransaction() { return RC.OK; }
#endif
    }

    public partial class Pager
    {
        #region Debug

#if DEBUG
        internal bool assert_pager_state()
        {
            // Regardless of the current state, a temp-file connection always behaves as if it has an exclusive lock on the database file. It never updates
            // the change-counter field, so the changeCountDone flag is always set.
            Debug.Assert(!TempFile || Lock == VFile.LOCK.EXCLUSIVE);
            Debug.Assert(!TempFile || ChangeCountDone);
            // If the useJournal flag is clear, the journal-mode must be "OFF". And if the journal-mode is "OFF", the journal file must not be open.
            Debug.Assert(JournalMode == IPager.JOURNALMODE.OFF || UseJournal != 0);
            Debug.Assert(JournalMode != IPager.JOURNALMODE.OFF || !JournalFile.Open);
            // Check that MEMDB implies noSync. And an in-memory journal. Since  this means an in-memory pager performs no IO at all, it cannot encounter 
            // either SQLITE_IOERR or SQLITE_FULL during rollback or while finalizing a journal file. (although the in-memory journal implementation may 
            // return SQLITE_IOERR_NOMEM while the journal file is being written). It is therefore not possible for an in-memory pager to enter the ERROR state.
            if (MemoryDB)
            {
                Debug.Assert(NoSync);
                Debug.Assert(JournalMode == IPager.JOURNALMODE.OFF || JournalMode == IPager.JOURNALMODE.JMEMORY);
                Debug.Assert(State != PAGER.ERROR && State != PAGER.OPEN);
                Debug.Assert(!UseWal());
            }
            // If changeCountDone is set, a RESERVED lock or greater must be held on the file.
            Debug.Assert(!ChangeCountDone || Lock >= VFile.LOCK.RESERVED);
            Debug.Assert(Lock != VFile.LOCK.PENDING);
            switch (State)
            {
                case PAGER.OPEN:
                    Debug.Assert(!MemoryDB);
                    Debug.Assert(ErrorCode == RC.OK);
                    Debug.Assert(PCache.RefCount(this.PCache) == 0 || TempFile);
                    break;
                case PAGER.READER:
                    Debug.Assert(ErrorCode == RC.OK);
                    Debug.Assert(Lock != VFile.LOCK.UNKNOWN);
                    Debug.Assert(Lock >= VFile.LOCK.SHARED || NoReadlock != 0);
                    break;
                case PAGER.WRITER_LOCKED:
                    Debug.Assert(_lock != VFSLOCK.UNKNOWN);
                    Debug.Assert(_errorCode == RC.OK);
                    if (!pagerUseWal())
                        Debug.Assert(_lock >= VFSLOCK.RESERVED);
                    Debug.Assert(_dbSize == _dbOrigSize);
                    Debug.Assert(_dbOrigSize == _dbFileSize);
                    Debug.Assert(_dbOrigSize == _dbHintSize);
                    Debug.Assert(_setMaster == 0);
                    break;
                case PAGER.WRITER_CACHEMOD:
                    Debug.Assert(_lock != VFSLOCK.UNKNOWN);
                    Debug.Assert(_errorCode == RC.OK);
                    if (!pagerUseWal())
                    {
                        // It is possible that if journal_mode=wal here that neither the journal file nor the WAL file are open. This happens during
                        // a rollback transaction that switches from journal_mode=off to journal_mode=wal.
                        Debug.Assert(_lock >= VFSLOCK.RESERVED);
                        Debug.Assert(_journalFile.Open || _journalMode == JOURNALMODE.OFF || _journalMode == JOURNALMODE.WAL);
                    }
                    Debug.Assert(_dbOrigSize == _dbFileSize);
                    Debug.Assert(_dbOrigSize == _dbHintSize);
                    break;
                case PAGER.WRITER_DBMOD:
                    Debug.Assert(_lock == VFSLOCK.EXCLUSIVE);
                    Debug.Assert(_errorCode == RC.OK);
                    Debug.Assert(!pagerUseWal());
                    Debug.Assert(_lock >= VFSLOCK.EXCLUSIVE);
                    Debug.Assert(_journalFile.Open || _journalMode == JOURNALMODE.OFF || _journalMode == JOURNALMODE.WAL);
                    Debug.Assert(_dbOrigSize <= _dbHintSize);
                    break;
                case PAGER.WRITER_FINISHED:
                    Debug.Assert(_lock == VFSLOCK.EXCLUSIVE);
                    Debug.Assert(_errorCode == RC.OK);
                    Debug.Assert(!pagerUseWal());
                    Debug.Assert(_journalFile.Open || _journalMode == JOURNALMODE.OFF || _journalMode == JOURNALMODE.WAL);
                    break;
                case PAGER.ERROR:
                    // There must be at least one outstanding reference to the pager if in ERROR state. Otherwise the pager should have already dropped
                    // back to OPEN state.
                    Debug.Assert(_errorCode != RC.OK);
                    Debug.Assert(_pcache.RefCount() > 0);
                    break;
            }

            return true;
        }
#endif

        #endregion

        #region X



        #endregion

        #region X



        #endregion

        #region X



        #endregion

        #region X



        #endregion
    }
}
//const int DEFAULT_JOURNAL_SIZE_LIMIT = -1;
//const int DEFAULT_PAGE_SIZE = 1024;
//const int MAX_DEFAULT_PAGE_SIZE = 8192;
//public const int MAX_PAGE_SIZE = 65535;
//const int MAX_PAGE_COUNT = 1073741823;