using Pid = System.UInt32;
using IPage = Core.PgHdr;
using System;
using Core.IO;
using System.Diagnostics;
using System.Linq;
using System.Text;

namespace Core
{
    public partial class Pager
    {
        // sqliteLimit.h
        const int MAX_PAGE_SIZE = 65535;
        const int DEFAULT_PAGE_SIZE = 1024;
        const int MAX_DEFAULT_PAGE_SIZE = 8192;
        const int MAX_PAGE_COUNT = 1073741823;

        //const int DEFAULT_JOURNAL_SIZE_LIMIT = -1;

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
        public PagerSavepoint[] Savepoints; // Array of active savepoints 
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

        private static readonly byte[] _journalMagic = new byte[] { 0xd9, 0xd5, 0x05, 0xf9, 0x20, 0xa1, 0x63, 0xd7 };
        private static uint JOURNAL_PG_SZ(Pager pager) { return (uint)pager.PageSize + 8; }
        private static uint JOURNAL_HDR_SZ(Pager pager) { return pager.SectorSize; }

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
            // State must be valid.
            Debug.Assert(State == PAGER.OPEN ||
                State == PAGER.READER ||
                State == PAGER.WRITER_LOCKED ||
                State == PAGER.WRITER_CACHEMOD ||
                State == PAGER.WRITER_DBMOD ||
                State == PAGER.WRITER_FINISHED ||
                State == PAGER.ERROR);

            // Regardless of the current state, a temp-file connection always behaves as if it has an exclusive lock on the database file. It never updates
            // the change-counter field, so the changeCountDone flag is always set.
            Debug.Assert(!TempFile || Lock == VFile.LOCK.EXCLUSIVE);
            Debug.Assert(!TempFile || ChangeCountDone);

            // If the useJournal flag is clear, the journal-mode must be "OFF". And if the journal-mode is "OFF", the journal file must not be open.
            Debug.Assert(JournalMode == IPager.JOURNALMODE.OFF || UseJournal != 0);
            Debug.Assert(JournalMode != IPager.JOURNALMODE.OFF || !JournalFile.Opened);

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
                    Debug.Assert(Lock >= VFile.LOCK.SHARED);
                    break;

                case PAGER.WRITER_LOCKED:
                    Debug.Assert(Lock != VFile.LOCK.UNKNOWN);
                    Debug.Assert(ErrorCode == RC.OK);
                    if (!UseWal())
                        Debug.Assert(Lock >= VFile.LOCK.RESERVED);
                    Debug.Assert(DBSize == DBOrigSize);
                    Debug.Assert(DBOrigSize == DBFileSize);
                    Debug.Assert(DBOrigSize == DBHintSize);
                    Debug.Assert(!SetMaster);
                    break;

                case PAGER.WRITER_CACHEMOD:
                    Debug.Assert(Lock != VFile.LOCK.UNKNOWN);
                    Debug.Assert(ErrorCode == RC.OK);
                    if (!UseWal())
                    {
                        // It is possible that if journal_mode=wal here that neither the journal file nor the WAL file are open. This happens during
                        // a rollback transaction that switches from journal_mode=off to journal_mode=wal.
                        Debug.Assert(Lock >= VFile.LOCK.RESERVED);
                        Debug.Assert(JournalFile.Opened || JournalMode == IPager.JOURNALMODE.OFF || JournalMode == IPager.JOURNALMODE.WAL);
                    }
                    Debug.Assert(DBOrigSize == DBFileSize);
                    Debug.Assert(DBOrigSize == DBHintSize);
                    break;

                case PAGER.WRITER_DBMOD:
                    Debug.Assert(Lock == VFile.LOCK.EXCLUSIVE);
                    Debug.Assert(ErrorCode == RC.OK);
                    Debug.Assert(!UseWal());
                    Debug.Assert(Lock >= VFile.LOCK.EXCLUSIVE);
                    Debug.Assert(JournalFile.Opened || JournalMode == IPager.JOURNALMODE.OFF || JournalMode == IPager.JOURNALMODE.WAL);
                    Debug.Assert(DBOrigSize <= DBHintSize);
                    break;

                case PAGER.WRITER_FINISHED:
                    Debug.Assert(Lock == VFile.LOCK.EXCLUSIVE);
                    Debug.Assert(ErrorCode == RC.OK);
                    Debug.Assert(!UseWal());
                    Debug.Assert(JournalFile.Opened || JournalMode == IPager.JOURNALMODE.OFF || JournalMode == IPager.JOURNALMODE.WAL);
                    break;

                case PAGER.ERROR:
                    // There must be at least one outstanding reference to the pager if in ERROR state. Otherwise the pager should have already dropped back to OPEN state.
                    Debug.Assert(ErrorCode != RC.OK);
                    Debug.Assert(PCache.RefCount(this.PCache) > 0);
                    break;
            }

            return true;
        }


        internal string print_pager_state()
        {
            return string.Format(@"
Filename:      {0}
State:         {1} errCode={2}
Lock:          {3}
Locking mode:  locking_mode={4}
Journal mode:  journal_mode={5}
Backing store: tempFile={6} memDb={7} useJournal={8}
Journal:       journalOff={9.11} journalHdr={10.11}
Size:          dbsize={11} dbOrigSize={12} dbFileSize={13}"
          , Filename
          , State == PAGER.OPEN ? "OPEN" :
              State == PAGER.READER ? "READER" :
              State == PAGER.WRITER_LOCKED ? "WRITER_LOCKED" :
              State == PAGER.WRITER_CACHEMOD ? "WRITER_CACHEMOD" :
              State == PAGER.WRITER_DBMOD ? "WRITER_DBMOD" :
              State == PAGER.WRITER_FINISHED ? "WRITER_FINISHED" :
              State == PAGER.ERROR ? "ERROR" : "?error?"
          , (int)ErrorCode
          , Lock == VFile.LOCK.NO ? "NO_LOCK" :
              Lock == VFile.LOCK.RESERVED ? "RESERVED" :
              Lock == VFile.LOCK.EXCLUSIVE ? "EXCLUSIVE" :
              Lock == VFile.LOCK.SHARED ? "SHARED" :
              Lock == VFile.LOCK.UNKNOWN ? "UNKNOWN" : "?error?"
          , ExclusiveMode ? "exclusive" : "normal"
          , JournalMode == IPager.JOURNALMODE.JMEMORY ? "memory" :
              JournalMode == IPager.JOURNALMODE.OFF ? "off" :
              JournalMode == IPager.JOURNALMODE.DELETE ? "delete" :
              JournalMode == IPager.JOURNALMODE.PERSIST ? "persist" :
              JournalMode == IPager.JOURNALMODE.TRUNCATE ? "truncate" :
              JournalMode == IPager.JOURNALMODE.WAL ? "wal" : "?error?"
          , (TempFile ? 1 : 0), (MemoryDB ? 1 : 0), (int)UseJournal
          , JournalOffset, JournalHeader
          , (int)DBSize, (int)DBOrigSize, (int)DBFileSize);
        }



#endif

        #endregion

        #region Name1

        private static bool subjRequiresPage(PgHdr pg)
        {
            var id = pg.ID;
            var pager = pg.Pager;
            for (var i = 0; i < pager.Savepoints.Length; i++)
            {
                var p = pager.Savepoints[i];
                if (p.Orig >= id && !p.InSavepoint.Get(id))
                    return true;
            }
            return false;
        }

        private static bool pageInJournal(PgHdr pg)
        {
            return pg.Pager.InJournal.Get(pg.ID);
        }

        private RC pagerUnlockDb(VFile.LOCK @lock)
        {
            Debug.Assert(!ExclusiveMode || Lock == @lock);
            Debug.Assert(@lock == VFile.LOCK.NO || @lock == VFile.LOCK.SHARED);
            Debug.Assert(@lock != VFile.LOCK.NO || !UseWal());
            var rc = RC.OK;
            if (File.Opened)
            {
                Debug.Assert(Lock >= @lock);
                rc = File.Unlock(@lock);
                if (Lock != VFile.LOCK.UNKNOWN)
                    Lock = @lock;
                SysEx.IOTRACE("UNLOCK {0:x} {1}", this, @lock);
            }
            return rc;
        }

        private RC pagerLockDb(VFile.LOCK @lock)
        {
            Debug.Assert(@lock == VFile.LOCK.SHARED || @lock == VFile.LOCK.RESERVED || @lock == VFile.LOCK.EXCLUSIVE);
            var rc = RC.OK;
            if (Lock < @lock || Lock == VFile.LOCK.UNKNOWN)
            {
                rc = File.Lock(@lock);
                if (rc == RC.OK && (Lock != VFile.LOCK.UNKNOWN || @lock == VFile.LOCK.EXCLUSIVE))
                {
                    Lock = @lock;
                    SysEx.IOTRACE("LOCK {0:x} {1}", this, @lock);
                }
            }
            return rc;
        }

#if ENABLE_ATOMIC_WRITE
        internal static int jrnlBufferSize(Pager pager)
        {
            Debug.Assert(!pager.MemoryDB);
            if (!pager.TempFile)
            {
                Debug.Assert(pager.File.Opened);
                var dc = pager.File.get_DeviceCharacteristics();
                var sectorSize = pager.SectorSize;
                var pageSize = pager.PageSize;
                Debug.Assert(IOCAP_ATOMIC512 == (512 >> 8));
                Debug.Assert(IOCAP_ATOMIC64K == (65536 >> 8));
                if ((dc & (IOCAP_ATOMIC | (pageSize >> 8)) || sectorSize > pageSize) == 0)
                    return 0;
            }
            return JOURNAL_HDR_SZ(pager) + JOURNAL_PG_SZ(pager);
        }
#endif

#if CHECK_PAGES
        internal static uint pager_datahash(int bytes, byte[] data)
        {
            uint hash = 0;
            for (var i = 0; i < bytes; i++)
                hash = (hash * 1039) + data[i];
            return hash;
        }
        internal static uint pager_pagehash(PgHdr page) { return pager_datahash(page.Pager.PageSize, page.Data); }
        internal static void pager_set_pagehash(PgHdr page) { page.PageHash = pager_pagehash(page); }
        internal static void checkPage(PgHdr page)
        {
            var pager = page.Pager;
            Debug.Assert(pager.State != PAGER.ERROR);
            Debug.Assert((page.Flags & PgHdr.PGHDR.DIRTY) != 0 || page.PageHash == pager_pagehash(page));
        }
#else
        internal static uint pager_pagehash(PgHdr x) { return 0; }
        internal static uint pager_datahash(int x, byte[] y) { return 0; }
        internal static void pager_set_pagehash(PgHdr x) { }
        internal static void checkPage(PgHdr x) { }
#endif

        #endregion

        #region Journal1

        private static RC readMasterJournal(VFile journalFile, byte[] master, uint masterLength)
        {
            int nameLength = 0;         // Length in bytes of master journal name 
            long fileSize = 0;          // Total size in bytes of journal file pJrnl 
            uint checksum = 0;          // MJ checksum value read from journal
            var magic = new byte[8];    // A buffer to hold the magic header
            master[0] = 0;
            RC rc;
            if ((rc = journalFile.get_FileSize(out fileSize)) != RC.OK ||
                fileSize < 16 ||
                (rc = journalFile.Read4((int)(fileSize - 16), out nameLength)) != RC.OK ||
                nameLength >= masterLength ||
                (rc = journalFile.Read4(fileSize - 12, out checksum)) != RC.OK ||
                (rc = journalFile.Read(magic, 8, fileSize - 8)) != RC.OK ||
                Enumerable.SequenceEqual(magic, _journalMagic) ||
                (rc = journalFile.Read(master, nameLength, (long)(fileSize - 16 - nameLength))) != RC.OK)
                return rc;
            // See if the checksum matches the master journal name
            for (var u = 0; u < nameLength; u++)
                checksum -= master[u];
            if (checksum != 0)
            {
                // If the checksum doesn't add up, then one or more of the disk sectors containing the master journal filename is corrupted. This means
                // definitely roll back, so just return SQLITE.OK and report a (nul) master-journal filename.
                nameLength = 0;
            }
            master[nameLength] = 0;
            return RC.OK;
        }

        private long journalHdrOffset()
        {
            long offset = 0;
            var c = JournalOffset;
            if (c != 0)
                offset = (int)(((c - 1) / JOURNAL_HDR_SZ(this) + 1) * JOURNAL_HDR_SZ(this));
            Debug.Assert(offset % JOURNAL_HDR_SZ(this) == 0);
            Debug.Assert(offset >= c);
            Debug.Assert((offset - c) < JOURNAL_HDR_SZ(this));
            return offset;
        }

        private RC zeroJournalHdr(int doTruncate)
        {
            var rc = RC.OK;
            Debug.Assert(JournalFile.Opened);
            if (JournalOffset != 0)
            {
                var zeroHeader = new byte[28];
                var limit = JournalSizeLimit; // Local cache of jsl
                SysEx.IOTRACE("JZEROHDR {0:x}", this);
                if (doTruncate != 0 || limit == 0)
                    rc = JournalFile.Truncate(0);
                else
                    rc = JournalFile.Write(zeroHeader, zeroHeader.Length, 0);
                if (rc == RC.OK && !NoSync)
                    rc = JournalFile.Sync(VFile.SYNC.DATAONLY | SyncFlags);
                // At this point the transaction is committed but the write lock is still held on the file. If there is a size limit configured for
                // the persistent journal and the journal file currently consumes more space than that limit allows for, truncate it now. There is no need
                // to sync the file following this operation.
                if (rc == RC.OK && limit > 0)
                {
                    long fileSize;
                    rc = JournalFile.get_FileSize(out fileSize);
                    if (rc == RC.OK && fileSize > limit)
                        rc = JournalFile.Truncate(limit);
                }
            }
            return rc;
        }

        private RC writeJournalHdr()
        {
            Debug.Assert(JournalFile.Opened);
            var header = TmpSpace;                 // Temporary space used to build header
            var headerSize = (uint)PageSize;        // Size of buffer pointed to by zHeader
            if (headerSize > JOURNAL_HDR_SZ(this))
                headerSize = JOURNAL_HDR_SZ(this);

            // If there are active savepoints and any of them were created since the most recent journal header was written, update the
            // PagerSavepoint.iHdrOffset fields now.
            for (var ii = 0; ii < Savepoints.Length; ii++)
                if (Savepoints[ii].HdrOffset == 0)
                    Savepoints[ii].HdrOffset = JournalOffset;
            JournalHeader = JournalOffset = journalHdrOffset();

            // Write the nRec Field - the number of page records that follow this journal header. Normally, zero is written to this value at this time.
            // After the records are added to the journal (and the journal synced, if in full-sync mode), the zero is overwritten with the true number
            // of records (see syncJournal()).
            //
            // A faster alternative is to write 0xFFFFFFFF to the nRec field. When reading the journal this value tells SQLite to assume that the
            // rest of the journal file contains valid page records. This assumption is dangerous, as if a failure occurred whilst writing to the journal
            // file it may contain some garbage data. There are two scenarios where this risk can be ignored:
            //   * When the pager is in no-sync mode. Corruption can follow a power failure in this case anyway.
            //   * When the SQLITE_IOCAP_SAFE_APPEND flag is set. This guarantees that garbage data is never appended to the journal file.
            Debug.Assert(File.Opened || NoSync);
            if (NoSync || (JournalMode == IPager.JOURNALMODE.JMEMORY) || (File.get_DeviceCharacteristics() & VFile.IOCAP.SAFE_APPEND) != 0)
            {
                _journalMagic.CopyTo(header, 0);
                ConvertEx.Put4(header, _journalMagic.Length, 0xffffffff);
            }
            else
                Array.Clear(header, 0, _journalMagic.Length + 4);
            SysEx.MakeRandomness(sizeof(long), ref ChecksumInit);
            ConvertEx.Put4(header, _journalMagic.Length + 4, ChecksumInit); // The random check-hash initializer
            ConvertEx.Put4(header, _journalMagic.Length + 8, DBOrigSize);   // The initial database size
            ConvertEx.Put4(header, _journalMagic.Length + 12, SectorSize);  // The assumed sector size for this process
            ConvertEx.Put4(header, _journalMagic.Length + 16, PageSize);    // The page size
            // Initializing the tail of the buffer is not necessary.  Everything works find if the following memset() is omitted.  But initializing
            // the memory prevents valgrind from complaining, so we are willing to take the performance hit.
            Array.Clear(header, _journalMagic.Length + 20, (int)headerSize - _journalMagic.Length + 20);

            // In theory, it is only necessary to write the 28 bytes that the journal header consumes to the journal file here. Then increment the 
            // Pager.journalOff variable by JOURNAL_HDR_SZ so that the next record is written to the following sector (leaving a gap in the file
            // that will be implicitly filled in by the OS).
            //
            // However it has been discovered that on some systems this pattern can be significantly slower than contiguously writing data to the file,
            // even if that means explicitly writing data to the block of (JOURNAL_HDR_SZ - 28) bytes that will not be used. So that is what is done. 
            //
            // The loop is required here in case the sector-size is larger than the database page size. Since the zHeader buffer is only Pager.pageSize
            // bytes in size, more than one call to sqlite3OsWrite() may be required to populate the entire journal header sector.
            RC rc = RC.OK;
            for (uint headerWritten = 0; rc == RC.OK && headerWritten < JOURNAL_HDR_SZ(this); headerWritten += headerSize)
            {
                SysEx.IOTRACE("JHDR {0:x} {1,11} {2}", this, JournalHeader, headerSize);
                rc = JournalFile.Write(header, (int)headerSize, JournalOffset);
                Debug.Assert(JournalHeader <= JournalOffset);
                JournalOffset += (int)headerSize;
            }
            return rc;
        }

        private RC readJournalHdr(int isHot, long journalSize, ref uint recordsOut, ref uint dbSizeOut)
        {
            Debug.Assert(JournalFile.Opened);

            // Advance Pager.journalOff to the start of the next sector. If the journal file is too small for there to be a header stored at this
            // point, return SQLITE_DONE.
            JournalOffset = journalHdrOffset();
            if (JournalOffset + JOURNAL_HDR_SZ(this) > journalSize)
                return RC.DONE;
            var headerOffset = JournalOffset;

            // Read in the first 8 bytes of the journal header. If they do not match the  magic string found at the start of each journal header, return
            // SQLITE_DONE. If an IO error occurs, return an error code. Otherwise, proceed.
            RC rc;
            var magic = new byte[8];
            if (isHot != 0 || headerOffset != JournalHeader)
            {
                rc = JournalFile.Read(magic, magic.Length, headerOffset);
                if (rc != RC.OK)
                    return rc;
                if (Enumerable.SequenceEqual(magic, _journalMagic))
                    return RC.DONE;
            }
            // Read the first three 32-bit fields of the journal header: The nRec field, the checksum-initializer and the database size at the start
            // of the transaction. Return an error code if anything goes wrong.
            if ((rc = JournalFile.Read4(headerOffset + 8, out recordsOut)) != RC.OK ||
                (rc = JournalFile.Read4(headerOffset + 12, out ChecksumInit)) != RC.OK ||
                (rc = JournalFile.Read4(headerOffset + 16, out dbSizeOut)) != RC.OK)
                return rc;

            if (JournalOffset == 0)
            {
                uint pageSize = 0;     // Page-size field of journal header
                uint sectorSize = 0;   // Sector-size field of journal header
                // Read the page-size and sector-size journal header fields.
                if ((rc = JournalFile.Read4(headerOffset + 20, out sectorSize)) != RC.OK ||
                    (rc = JournalFile.Read4(headerOffset + 24, out pageSize)) != RC.OK)
                    return rc;

                // Versions of SQLite prior to 3.5.8 set the page-size field of the journal header to zero. In this case, assume that the Pager.pageSize
                // variable is already set to the correct page size.
                if (pageSize == 0)
                    pageSize = (uint)PageSize;

                // Check that the values read from the page-size and sector-size fields are within range. To be 'in range', both values need to be a power
                // of two greater than or equal to 512 or 32, and not greater than their respective compile time maximum limits.
                if (pageSize < 512 || sectorSize < 32 ||
                    pageSize > MAX_PAGE_SIZE || sectorSize > MAX_SECTOR_SIZE ||
                    ((pageSize - 1) & pageSize) != 0 || ((sectorSize - 1) & sectorSize) != 0)
                    // If the either the page-size or sector-size in the journal-header is invalid, then the process that wrote the journal-header must have
                    // crashed before the header was synced. In this case stop reading the journal file here.
                    return RC.DONE;

                // Update the page-size to match the value read from the journal. Use a testcase() macro to make sure that malloc failure within PagerSetPagesize() is tested.
                rc = SetPageSize(ref pageSize, -1);

                // Update the assumed sector-size to match the value used by the process that created this journal. If this journal was
                // created by a process other than this one, then this routine is being called from within pager_playback(). The local value
                // of Pager.sectorSize is restored at the end of that routine.
                SectorSize = sectorSize;
            }

            JournalOffset += (int)JOURNAL_HDR_SZ(this);
            return rc;
        }

        private RC writeMasterJournal(string master)
        {
            Debug.Assert(!SetMaster);
            Debug.Assert(!UseWal());

            if (master == null ||
                JournalMode == IPager.JOURNALMODE.JMEMORY ||
                JournalMode == IPager.JOURNALMODE.OFF)
                return RC.OK;
            SetMaster = true;
            Debug.Assert(JournalFile.Opened);
            Debug.Assert(JournalHeader <= JournalOffset);

            // Calculate the length in bytes and the checksum of zMaster
            uint checksum = 0;  // Checksum of string zMaster
            int masterLength;   // Length of string zMaster
            for (masterLength = 0; masterLength < master.Length && master[masterLength] != 0; masterLength++)
                checksum += master[masterLength];

            // If in full-sync mode, advance to the next disk sector before writing the master journal name. This is in case the previous page written to
            // the journal has already been synced.
            if (FullSync)
                JournalOffset = journalHdrOffset();
            var headerOffset = JournalOffset; // Offset of header in journal file

            // Write the master journal data to the end of the journal file. If an error occurs, return the error code to the caller.
            RC rc;
            if ((rc = JournalFile.Write4(headerOffset, (uint)IPager.MJ_PID(this))) != RC.OK ||
                (rc = JournalFile.Write(Encoding.UTF8.GetBytes(master), masterLength, headerOffset + 4)) != RC.OK ||
                (rc = JournalFile.Write4(headerOffset + 4 + masterLength, (uint)masterLength)) != RC.OK ||
                (rc = JournalFile.Write4(headerOffset + 4 + masterLength + 4, checksum)) != RC.OK ||
                (rc = JournalFile.Write(_journalMagic, 8, headerOffset + 4 + masterLength + 8)) != RC.OK)
                return rc;
            JournalOffset += masterLength + 20;

            // If the pager is in peristent-journal mode, then the physical journal-file may extend past the end of the master-journal name
            // and 8 bytes of magic data just written to the file. This is dangerous because the code to rollback a hot-journal file
            // will not be able to find the master-journal name to determine whether or not the journal is hot. 
            //
            // Easiest thing to do in this scenario is to truncate the journal file to the required size. 
            long journalSize = 0;  // Size of journal file on disk
            if ((rc = JournalFile.get_FileSize(out journalSize)) == RC.OK && journalSize > JournalOffset)
                rc = JournalFile.Truncate(JournalOffset);
            return rc;
        }

        #endregion

        #region X



        #endregion

        #region X



        #endregion
    }
}
