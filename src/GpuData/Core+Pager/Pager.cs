using Pid = System.UInt32;
using IPage = Core.PgHdr;
using System;
using Core.IO;

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


        //const int DEFAULT_JOURNAL_SIZE_LIMIT = -1;
        //const int DEFAULT_PAGE_SIZE = 1024;
        //const int MAX_DEFAULT_PAGE_SIZE = 8192;
        //public const int MAX_PAGE_SIZE = 65535;
        //const int MAX_PAGE_COUNT = 1073741823;

        //const int MAX_PID = 2147483647;

        public VFileSystem Vfs;             // OS functions to use for IO
        public bool ExclusiveMode;          // Boolean. True if locking_mode==EXCLUSIVE
        public JOURNALMODE JournalMode;     // One of the PAGER_JOURNALMODE_* values
        public byte UseJournal;             // Use a rollback journal on this file
        public bool NoSync;                 // Do not sync the journal if true
        public bool FullSync;               // Do extra syncs of the journal for robustness
        public VFile.SYNC CkptSyncFlags;    // SYNC_NORMAL or SYNC_FULL for checkpoint
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
        public uint CksumInit;              // Quasi-random value added to every checksum 
        public uint SubRecords;             // Number of records written to sub-journal 
        public Bitvec InJournal;            // One bit for each page in the database file 
        public VFile File;                  // File descriptor for database 
        public VFile JournalFile;           // File descriptor for main journal 
        public VFile SubJournalFile;        // File descriptor for sub-journal 
        public long JournalOff;             // Current write offset in the journal file 
        public long JournalHdr;             // Byte offset to previous journal header 
        public IBackup Backup;              // Pointer to list of ongoing backup processes 
        public PagerSavepoint[] Savepoint;  // Array of active savepoints 
        public byte[] DBFileVersions = new byte[16];    // Changes whenever database file changes
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



    }
}