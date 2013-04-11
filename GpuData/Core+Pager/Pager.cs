using System;
using System.Diagnostics;
using Pid = System.UInt32;
using Core.IO;
namespace Core
{
    public partial class Pager
    {
        const int DEFAULT_JOURNAL_SIZE_LIMIT = -1;
        const int DEFAULT_PAGE_SIZE = 1024;
        const int MAX_DEFAULT_PAGE_SIZE = 8192;
        public const int MAX_PAGE_SIZE = 65535;
        const int MAX_PAGE_COUNT = 1073741823;
        const int MAX_SECTOR_SIZE = 0x10000;
        const int MAX_PID = 2147483647;

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

        public enum LOCKINGMODE : sbyte
        {
            QUERY = -1,
            NORMAL = 0,
            EXCLUSIVE = 1,
        }

        [Flags]
        public enum JOURNALMODE : sbyte
        {
            QUERY = -1,
            DELETE = 0,
            PERSIST = 1,
            OFF = 2,
            TRUNCATE = 3,
            MEMORY = 4,
            WAL = 5,
        }

        [Flags]
        public enum PAGEROPEN : byte
        {
            OMIT_JOURNAL = 0x0001,
            NO_READLOCK = 0x0002,
            MEMORY = 0x0004,
        }

        public Pager(Func<object> memPageBuilder)
        {
            _memPageBuilder = memPageBuilder;
        }

        private Func<object> _memPageBuilder; // connects mempage to pager
        public VFileSystem _vfs;      // OS functions to use for IO
        public bool _exclusiveMode;          // Boolean. True if locking_mode==EXCLUSIVE
        public JOURNALMODE _journalMode;     // One of the PAGER_JOURNALMODE_* values
        public byte _useJournal;             // Use a rollback journal on this file
        public byte _noReadlock;             // Do not bother to obtain readlocks
        public bool _noSync;                 // Do not sync the journal if true
        public bool _fullSync;               // Do extra syncs of the journal for robustness
        public VFile.SYNC _ckptSyncFlags;          // SYNC_NORMAL or SYNC_FULL for checkpoint
        public VFile.SYNC _syncFlags;              // SYNC_NORMAL or SYNC_FULL otherwise
        public bool _tempFile;               // zFilename is a temporary file
        public bool _readOnly;               // True for a read-only database
        public bool _alwaysRollback;         // Disable DontRollback() for all pages
        public bool _memoryDB;                  // True to inhibit all file I/O
        // The following block contains those class members that change during routine opertion.  Class members not in this block are either fixed
        // when the pager is first created or else only change when there is a significant mode change (such as changing the page_size, locking_mode,
        // or the journal_mode).  From another view, these class members describe the "state" of the pager, while other class members describe the
        // "configuration" of the pager.
        public PAGER _state;                // Pager state (OPEN, READER, WRITER_LOCKED..) 
        public VFile.LOCK _lock;      // Current lock held on database file 
        public bool _changeCountDone;        // Set after incrementing the change-counter 
        public int _setMaster;               // True if a m-j name has been written to jrnl 
        public byte _doNotSpill;             // Do not spill the cache when non-zero 
        public byte _doNotSyncSpill;         // Do not do a spill that requires jrnl sync 
        public byte _subjInMemory;           // True to use in-memory sub-journals 
        public Pid _dbSize;                 // Number of pages in the database 
        public Pid _dbOrigSize;             // dbSize before the current transaction 
        public Pid _dbFileSize;             // Number of pages in the database file 
        public Pid _dbHintSize;             // Value passed to FCNTL_SIZE_HINT call 
        public RC _errorCode;              // One of several kinds of errors 
        public int _nRec;                    // Pages journalled since last j-header written 
        public uint _cksumInit;              // Quasi-random value added to every checksum 
        public uint _nSubRec;                // Number of records written to sub-journal 
        public Bitvec _inJournal;           // One bit for each page in the database file 
        public VFile _file;             // File descriptor for database 
        public VFile _journalFile;            // File descriptor for main journal 
        public VFile _journal2File;           // File descriptor for sub-journal 
        public long _journalOff;             // Current write offset in the journal file 
        public long _journalHdr;             // Byte offset to previous journal header 
        public IBackup _backup;             // Pointer to list of ongoing backup processes 
        public PagerSavepoint[] _savepoint; // Array of active savepoints 
        public byte[] _dbFileVers = new byte[16];    // Changes whenever database file changes
        // End of the routinely-changing class members
        public ushort _extra;               // Add this many bytes to each in-memory page
        public short _reserve;              // Number of unused bytes at end of each page
        public VFileSystem.OPEN _vfsFlags;               // Flags for VirtualFileSystem.xOpen() 
        public uint _sectorSize;             // Assumed sector size during rollback 
        public int _pageSize;                // Number of bytes in a page 
        public Pid _pids;                 // Maximum allowed size of the database 
        public long _journalSizeLimit;       // Size limit for persistent journal files 
        public string _filename;            // Name of the database file 
        public string _journal;             // Name of the journal file 
        public Func<object, int> _busyHandler;  // Function to call when busy 
        public object _busyHandlerArg;      // Context argument for xBusyHandler 
#if DEBUG
        public int _statHits, _statMisses;             // Cache hits and missing 
        public int _statReads, _statWrites;           // Database pages read/written 
#else
        public int _statHits;
#endif
        public Action<PgHdr> _reiniter;     // Call this routine when reloading pages
        public byte[] _tempSpace;               // Pager.pageSize bytes of space for tmp use
        public PCache _pcache;                 // Pointer to page cache object
#if !OMIT_WAL
        public Wal _wal;                       // Write-ahead log used by "journal_mode=wal"
        public string _walName;                    // File name for write-ahead log
#else
        public Wal _wal = null;             // Having this dummy here makes C# easier
#endif

        public static Pid MJ_PID(Pager pager) { return ((Pid)((VFile.PENDING_BYTE / ((pager)._pageSize)) + 1)); }

#if false && !OMIT_WAL
        internal static int pagerUseWal(Pager pager) { return (pPager.pWal != 0); }
#else
        internal bool pagerUseWal() { return false; }
        internal RC pagerRollbackWal() { return RC.OK; }
        internal RC pagerWalFrames(PgHdr w, Pid x, int y, VFile.SYNC z) { return RC.OK; }
        internal RC pagerOpenWalIfPresent() { return RC.OK; }
        internal RC pagerBeginReadTransaction() { return RC.OK; }
#endif

#if ATOMIC_WRITES
        internal static int jrnlBufferSize(Pager pager)
        {
            if (!pager._tempFile)
            {
                Debug.Assert(pager._file.Open);
                var dc = sqlite3OsDeviceCharacteristics(pager._file);
                var nSector = pager._sectorSize;
                var szPage = pager._pageSize;
                Debug.Assert(IOCAP_ATOMIC512 == (512 >> 8));
                Debug.Assert(IOCAP_ATOMIC64K == (65536 >> 8));
                if (0 == (dc & (IOCAP_ATOMIC | (szPage >> 8)) || nSector > szPage))
                    return 0;
            }
            return JOURNAL_HDR_SZ(pager) + JOURNAL_PG_SZ(pager);
        }
#endif

#if CHECK_PAGES
        internal static uint pager_pagehash(PgHdr page) { return pager_datahash(page.Pager._pageSize, page._Data); }
        internal static uint pager_datahash(int nByte, byte[] pData)
        {
            uint hash = 0;
            for (var i = 0; i < nByte; i++)
                hash = (hash * 1039) + pData[i];
            return hash;
        }
        internal static void pager_set_pagehash(PgHdr page) { page.PageHash = pager_pagehash(page); }
        internal static void checkPage(PgHdr page)
        {
            var pPager = page.Pager;
            Debug.Assert(pPager._state != PAGER.ERROR);
            Debug.Assert((page.Flags & PgHdr.PGHDR.DIRTY) != 0 || page.PageHash == pager_pagehash(page));
        }
#else
        internal static uint pager_pagehash(PgHdr x) { return 0; }
        internal static uint pager_datahash(int x, byte[] y) { return 0; }
        internal static void pager_set_pagehash(PgHdr x) { }
#endif
    }
}
