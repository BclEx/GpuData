﻿using Pid = System.UInt32;
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
        private static readonly byte[] _journalMagic = new byte[] { 0xd9, 0xd5, 0x05, 0xf9, 0x20, 0xa1, 0x63, 0xd7 };
        // sqliteLimit.h
        const int MAX_PAGE_SIZE = 65535;
        const int DEFAULT_PAGE_SIZE = 1024;
        const int MAX_DEFAULT_PAGE_SIZE = 8192;
        const int MAX_PAGE_COUNT = 1073741823;
        // pager.h
        const int DEFAULT_JOURNAL_SIZE_LIMIT = -1;

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
            public long Offset;             // Starting offset in main journal
            public long HdrOffset;          // See above
            public Bitvec InSavepoint;      // Set of pages in this savepoint
            public Pid Orig;                // Original number of pages in file
            public Pid SubRecords;              // Index of first record in sub-journal
#if !OMIT_WAL
            public uint WalData[WAL_SAVEPOINT_NDATA];        // WAL savepoint context
#else
            // For C#
            public object WalData = null;
#endif
            // For C#
            //public static implicit operator bool(PagerSavepoint b) { return (b != null); }
        }

        public VFileSystem Vfs;             // OS functions to use for IO
        public bool ExclusiveMode;          // Boolean. True if locking_mode==EXCLUSIVE
        public IPager.JOURNALMODE JournalMode;     // One of the PAGER_JOURNALMODE_* values
        public bool UseJournal;             // Use a rollback journal on this file
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
        public bool DoNotSpill;             // Do not spill the cache when non-zero 
        public bool DoNotSyncSpill;         // Do not do a spill that requires jrnl sync 
        public bool SubjInMemory;           // True to use in-memory sub-journals 
        public Pid DBSize;                  // Number of pages in the database 
        public Pid DBOrigSize;              // dbSize before the current transaction 
        public Pid DBFileSize;              // Number of pages in the database file 
        public Pid DBHintSize;              // Value passed to FCNTL_SIZE_HINT call 
        public RC ErrorCode;                // One of several kinds of errors 
        public int Records;                 // Pages journalled since last j-header written 
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
        public byte[] DBFileVersion = new byte[16];    // Changes whenever database file changes
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
        // For C#
        public Wal Wal;
#endif

        public enum STAT : byte
        {
            HIT = 0,
            MISS = 1,
            WRITE = 2,
        }

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

        static void PAGERTRACE(string x, params object[] args) { Console.WriteLine("p:" + string.Format(x, args)); }
        static int PAGERID(Pager p) { return p.GetHashCode(); }
        static int FILEHANDLEID(VFile fd) { return fd.GetHashCode(); }
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
                    Debug.Assert(PCache.get_Refs() == 0 || TempFile);
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
                    Debug.Assert(PCache.get_Refs() > 0);
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

        private static RC readMasterJournal(VFile journalFile, out string master, uint masterLength)
        {
            int nameLength = 0;         // Length in bytes of master journal name 
            long fileSize = 0;          // Total size in bytes of journal file pJrnl 
            uint checksum = 0;          // MJ checksum value read from journal
            var magic = new byte[8];    // A buffer to hold the magic header
            var master2 = new byte[masterLength];
            master2[0] = 0;
            RC rc;
            if ((rc = journalFile.get_FileSize(out fileSize)) != RC.OK ||
                fileSize < 16 ||
                (rc = journalFile.Read4((int)(fileSize - 16), out nameLength)) != RC.OK ||
                nameLength >= masterLength ||
                (rc = journalFile.Read4(fileSize - 12, out checksum)) != RC.OK ||
                (rc = journalFile.Read(magic, 8, fileSize - 8)) != RC.OK ||
                Enumerable.SequenceEqual(magic, _journalMagic) ||
                (rc = journalFile.Read(master2, nameLength, (long)(fileSize - 16 - nameLength))) != RC.OK)
            {
                master = null;
                return rc;
            }
            // See if the checksum matches the master journal name
            for (var u = 0U; u < nameLength; u++)
                checksum -= master2[u];
            if (checksum != 0)
            {
                // If the checksum doesn't add up, then one or more of the disk sectors containing the master journal filename is corrupted. This means
                // definitely roll back, so just return SQLITE.OK and report a (nul) master-journal filename.
                nameLength = 0;
            }
            master2[nameLength] = 0;
            master = Encoding.UTF8.GetString(master2);
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

        private RC zeroJournalHdr(bool doTruncate)
        {
            Debug.Assert(JournalFile.Opened);
            var rc = RC.OK;
            if (JournalOffset != 0)
            {
                var zeroHeader = new byte[28];
                var limit = JournalSizeLimit; // Local cache of jsl
                SysEx.IOTRACE("JZEROHDR {0:x}", this);
                if (doTruncate || limit == 0)
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
            var header = TmpSpace;                  // Temporary space used to build header
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
            for (var headerWritten = 0U; rc == RC.OK && headerWritten < JOURNAL_HDR_SZ(this); headerWritten += headerSize)
            {
                SysEx.IOTRACE("JHDR {0:x} {1,11} {2}", this, JournalHeader, headerSize);
                rc = JournalFile.Write(header, (int)headerSize, JournalOffset);
                Debug.Assert(JournalHeader <= JournalOffset);
                JournalOffset += (int)headerSize;
            }
            return rc;
        }

        private RC readJournalHdr(bool isHot, long journalSize, ref uint recordsOut, ref uint dbSizeOut)
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
            if (isHot || headerOffset != JournalHeader)
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
            JournalOffset += (masterLength + 20);

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

        #region Name2

        private PgHdr pager_lookup(Pid id)
        {
            // It is not possible for a call to PcacheFetch() with createFlag==0 to fail, since no attempt to allocate dynamic memory will be made.
            PgHdr p;
            PCache.Fetch(id, false, out p);
            return p;
        }

        private void pager_reset()
        {
            if (Backup != null)
                Backup.Restart();
            PCache.Clear();
        }

        private void releaseAllSavepoints()
        {
            for (var ii = 0; ii < Savepoints.Length; ii++)
                Bitvec.Destroy(ref Savepoints[ii].InSavepoint);
            if (!ExclusiveMode || SubJournalFile is MemoryVFile)
                SubJournalFile.Close();
            Savepoints = null;
            SubRecords = 0;
        }

        private RC addToSavepointBitvecs(Pid id)
        {
            var rc = RC.OK;
            for (var ii = 0; ii < Savepoints.Length; ii++)
            {
                var p = Savepoints[ii];
                if (id <= p.Orig)
                {
                    rc |= p.InSavepoint.Set(id);
                    Debug.Assert(rc == RC.OK || rc == RC.NOMEM);
                }
            }
            return rc;
        }

        private void pager_unlock()
        {
            Debug.Assert(State == PAGER.READER ||
                State == PAGER.OPEN ||
                State == PAGER.ERROR);

            Bitvec.Destroy(ref InJournal);
            InJournal = null;
            releaseAllSavepoints();

            if (UseWal())
            {
                Debug.Assert(!JournalFile.Opened);
                Wal.EndReadTransaction();
                State = PAGER.OPEN;
            }
            else if (!ExclusiveMode)
            {
                // If the operating system support deletion of open files, then close the journal file when dropping the database lock.  Otherwise
                // another connection with journal_mode=delete might delete the file out from under us.
                Debug.Assert(((int)IPager.JOURNALMODE.JMEMORY & 5) != 1);
                Debug.Assert(((int)IPager.JOURNALMODE.OFF & 5) != 1);
                Debug.Assert(((int)IPager.JOURNALMODE.WAL & 5) != 1);
                Debug.Assert(((int)IPager.JOURNALMODE.DELETE & 5) != 1);
                Debug.Assert(((int)IPager.JOURNALMODE.TRUNCATE & 5) == 1);
                Debug.Assert(((int)IPager.JOURNALMODE.PERSIST & 5) == 1);
                var dc = (File.Opened ? File.get_DeviceCharacteristics() : 0);
                if ((dc & VFile.IOCAP.UNDELETABLE_WHEN_OPEN) == 0 || ((int)JournalMode & 5) != 1)
                    JournalFile.Close();

                // If the pager is in the ERROR state and the call to unlock the database file fails, set the current lock to UNKNOWN_LOCK. See the comment
                // above the #define for UNKNOWN_LOCK for an explanation of why this is necessary.
                var rc = pagerUnlockDb(VFile.LOCK.NO);
                if (rc != RC.OK && State == PAGER.ERROR)
                    Lock = VFile.LOCK.UNKNOWN;

                // The pager state may be changed from PAGER_ERROR to PAGER_OPEN here without clearing the error code. This is intentional - the error
                // code is cleared and the cache reset in the block below.
                Debug.Assert(ErrorCode != 0 || State != PAGER.ERROR);
                ChangeCountDone = false;
                State = PAGER.OPEN;
            }

            // If Pager.errCode is set, the contents of the pager cache cannot be trusted. Now that there are no outstanding references to the pager,
            // it can safely move back to PAGER_OPEN state. This happens in both normal and exclusive-locking mode.
            if (ErrorCode != 0)
            {
                Debug.Assert(!MemoryDB);
                pager_reset();
                ChangeCountDone = TempFile;
                State = PAGER.OPEN;
                ErrorCode = RC.OK;
            }
            JournalOffset = 0;
            JournalHeader = 0;
            SetMaster = false;
        }

        internal RC pager_error(RC rc)
        {
            var rc2 = (RC)((int)rc & 0xff);
            Debug.Assert(rc == RC.OK || !MemoryDB);
            Debug.Assert(ErrorCode == RC.FULL || ErrorCode == RC.OK || ((int)ErrorCode & 0xff) == (int)RC.IOERR);
            if (rc2 == RC.FULL || rc2 == RC.IOERR)
            {
                ErrorCode = rc;
                State = PAGER.ERROR;
            }
            return rc;
        }

        #endregion

        #region Transaction1

        private RC pager_end_transaction(bool hasMaster, bool commit)
        {
            // Do nothing if the pager does not have an open write transaction or at least a RESERVED lock. This function may be called when there
            // is no write-transaction active but a RESERVED or greater lock is held under two circumstances:
            //
            //   1. After a successful hot-journal rollback, it is called with eState==PAGER_NONE and eLock==EXCLUSIVE_LOCK.
            //
            //   2. If a connection with locking_mode=exclusive holding an EXCLUSIVE lock switches back to locking_mode=normal and then executes a
            //      read-transaction, this function is called with eState==PAGER_READER and eLock==EXCLUSIVE_LOCK when the read-transaction is closed.
            Debug.Assert(assert_pager_state());
            Debug.Assert(State != PAGER.ERROR);
            if (State < PAGER.WRITER_LOCKED && Lock < VFile.LOCK.RESERVED)
                return RC.OK;

            releaseAllSavepoints();
            Debug.Assert(JournalFile.Opened || InJournal == null);
            var rc = RC.OK;
            if (JournalFile.Opened)
            {
                Debug.Assert(!UseWal());

                // Finalize the journal file.
                if (JournalFile is MemoryVFile)
                {
                    Debug.Assert(JournalMode == IPager.JOURNALMODE.JMEMORY);
                    JournalFile.Close();
                }
                else if (JournalMode == IPager.JOURNALMODE.TRUNCATE)
                {
                    rc = (JournalOffset == 0 ? RC.OK : JournalFile.Truncate(0));
                    JournalOffset = 0;
                }
                else if (JournalMode == IPager.JOURNALMODE.PERSIST || (ExclusiveMode && JournalMode != IPager.JOURNALMODE.WAL))
                {
                    rc = zeroJournalHdr(hasMaster);
                    JournalOffset = 0;
                }
                else
                {
                    // This branch may be executed with Pager.journalMode==MEMORY if a hot-journal was just rolled back. In this case the journal
                    // file should be closed and deleted. If this connection writes to the database file, it will do so using an in-memory journal.
                    var delete_ = (!TempFile && JournalFile.JournalExists());
                    Debug.Assert(JournalMode == IPager.JOURNALMODE.DELETE ||
                        JournalMode == IPager.JOURNALMODE.JMEMORY ||
                        JournalMode == IPager.JOURNALMODE.WAL);
                    JournalFile.Close();
                    if (delete_)
                        Vfs.Delete(Journal, false);
                }
            }

#if CHECK_PAGES
            PCache.IterateDirty(pager_set_pagehash);
            if (DBSize == 0 && PCache.get_Refs() > 0)
            {
                var p = pager_lookup(1);
                if (p != null)
                {
                    p.PageHash = 0;
                    sqlite3PagerUnref(p);
                }
            }
#endif

            Bitvec.Destroy(ref InJournal); InJournal = null;
            Records = 0;
            PCache.CleanAll();
            PCache.Truncate(DBSize);

            var rc2 = RC.OK;    // Error code from db file unlock operation
            if (UseWal())
            {
                // Drop the WAL write-lock, if any. Also, if the connection was in locking_mode=exclusive mode but is no longer, drop the EXCLUSIVE 
                // lock held on the database file.
                rc2 = Wal.EndWriteTransaction();
                Debug.Assert(rc2 == RC.OK);
            }
            if (!ExclusiveMode && (!UseWal() || Wal.ExclusiveMode(0)))
            {
                rc2 = pagerUnlockDb(VFile.LOCK.SHARED);
                ChangeCountDone = false;
            }
            State = PAGER.READER;
            SetMaster = false;

            return (rc == RC.OK ? rc2 : rc);
        }

        private void pagerUnlockAndRollback()
        {
            if (State != PAGER.ERROR && State != PAGER.OPEN)
            {
                Debug.Assert(assert_pager_state());
                if (State >= PAGER.WRITER_LOCKED)
                {
                    SysEx.BeginBenignAlloc();
                    Rollback();
                    SysEx.EndBenignAlloc();
                }
                else if (!ExclusiveMode)
                {
                    Debug.Assert(State == PAGER.READER);
                    pager_end_transaction(false, false);
                }
            }
            pager_unlock();
        }

        private uint pager_cksum(byte[] data)
        {
            var checksum = ChecksumInit;
            var i = PageSize - 200;
            while (i > 0)
            {
                checksum += data[i];
                i -= 200;
            }
            return checksum;
        }

#if HAS_CODEC
        private void pagerReportSize()
        {
            if (CodecSizeChange != null)
                CodecSizeChange(Codec, PageSize, ReserveBytes);
        }
#else
        private void pagerReportSize() { }
#endif

        private RC pager_playback_one_page(ref long offset, Bitvec done, bool isMainJournal, bool isSavepoint)
        {
            //Debug.Assert((isMainJournal & ~1) == 0);            // isMainJrnl is 0 or 1
            //Debug.Assert((isSavepoint & ~1) == 0);              // isSavepnt is 0 or 1
            Debug.Assert(isMainJournal || done != null);   // pDone always used on sub-journals
            Debug.Assert(isSavepoint || done == null);     // pDone never used on non-savepoint

            var data = TmpSpace; // Temporary storage for the page
            Debug.Assert(data != null); // Temp storage must have already been allocated
            Debug.Assert(!UseWal() || (!isMainJournal && isSavepoint));

            // Either the state is greater than PAGER_WRITER_CACHEMOD (a transaction or savepoint rollback done at the request of the caller) or this is
            // a hot-journal rollback. If it is a hot-journal rollback, the pager is in state OPEN and holds an EXCLUSIVE lock. Hot-journal rollback
            // only reads from the main journal, not the sub-journal.
            Debug.Assert(State >= PAGER.WRITER_CACHEMOD || (State == PAGER.OPEN && Lock == VFile.LOCK.EXCLUSIVE));
            Debug.Assert(State >= PAGER.WRITER_CACHEMOD || isMainJournal);

            // Read the page number and page data from the journal or sub-journal file. Return an error code to the caller if an IO error occurs.
            var journalFile = (isMainJournal ? JournalFile : SubJournalFile); // The file descriptor for the journal file
            Pid id; // The page number of a page in journal
            var rc = journalFile.Read4(offset, out id);
            if (rc != RC.OK)
                return rc;
            rc = journalFile.Read(data, PageSize, offset + 4);
            if (rc != RC.OK)
                return rc;
            offset += PageSize + 4 + (isMainJournal ? 4 : 0); //TODO: CHECK THIS

            // Sanity checking on the page.  This is more important that I originally thought.  If a power failure occurs while the journal is being written,
            // it could cause invalid data to be written into the journal.  We need to detect this invalid data (with high probability) and ignore it.
            if (id == 0 || id == PAGER_MJ_PGNO(this))
            {
                Debug.Assert(!isSavepoint);
                return RC.DONE;
            }
            if (id > DBSize || done.Get(id))
                return RC.OK;
            if (isMainJournal)
            {
                uint checksum; // Checksum used for sanity checking
                rc = journalFile.Read4(offset - 4, out checksum);
                if (rc != RC.OK) return rc;
                if (!isSavepoint && pager_cksum(data) != checksum)
                    return RC.DONE;
            }

            // If this page has already been played by before during the current rollback, then don't bother to play it back again.
            if (done != null && (rc = done.Set(id)) != RC.OK)
                return rc;

            // When playing back page 1, restore the nReserve setting
            if (id == 1 && ReserveBytes != data[20])
            {
                ReserveBytes = (data)[20];
                pagerReportSize();
            }

            // If the pager is in CACHEMOD state, then there must be a copy of this page in the pager cache. In this case just update the pager cache,
            // not the database file. The page is left marked dirty in this case.
            //
            // An exception to the above rule: If the database is in no-sync mode and a page is moved during an incremental vacuum then the page may
            // not be in the pager cache. Later: if a malloc() or IO error occurs during a Movepage() call, then the page may not be in the cache
            // either. So the condition described in the above paragraph is not assert()able.
            //
            // If in WRITER_DBMOD, WRITER_FINISHED or OPEN state, then we update the pager cache if it exists and the main file. The page is then marked 
            // not dirty. Since this code is only executed in PAGER_OPEN state for a hot-journal rollback, it is guaranteed that the page-cache is empty
            // if the pager is in OPEN state.
            //
            // Ticket #1171:  The statement journal might contain page content that is different from the page content at the start of the transaction.
            // This occurs when a page is changed prior to the start of a statement then changed again within the statement.  When rolling back such a
            // statement we must not write to the original database unless we know for certain that original page contents are synced into the main rollback
            // journal.  Otherwise, a power loss might leave modified data in the database file without an entry in the rollback journal that can
            // restore the database to its original form.  Two conditions must be met before writing to the database files. (1) the database must be
            // locked.  (2) we know that the original page content is fully synced in the main journal either because the page is not in cache or else
            // the page is marked as needSync==0.
            //
            // 2008-04-14:  When attempting to vacuum a corrupt database file, it is possible to fail a statement on a database that does not yet exist.
            // Do not attempt to write if database file has never been opened.
            var pg = (UseWal() ? null : pager_lookup(id)); // An existing page in the cache
            Debug.Assert(pg != null || !MemoryDB);
            Debug.Assert(State != PAGER.OPEN || pg == null);
            PAGERTRACE("PLAYBACK {0} page {1} hash({2,08:x}) {3}", PAGERID(this), id, pager_datahash(PageSize, data), (isMainJournal ? "main-journal" : "sub-journal"));
            bool isSynced; // True if journal page is synced
            if (isMainJournal)
                isSynced = NoSync || (offset <= JournalHeader);
            else
                isSynced = (pg == null || 0 == (pg.Flags & PgHdr.PGHDR.NEED_SYNC));
            if (File.Opened && (State >= PAGER.WRITER_DBMOD || State == PAGER.OPEN) && isSynced)
            {
                long ofst = (id - 1) * PageSize;
                Debug.Assert(!UseWal());
                rc = File.Write(data, PageSize, ofst);
                if (id > DBFileSize)
                    DBFileSize = id;
                if (Backup != null)
                {
                    if (CODEC1(this, data, id, codec_ctx.DECRYPT))
                        rc = RC.NOMEM;
                    Backup.Update(id, data);
                    if (CODEC2(this, data, id, codec_ctx.ENCRYPT_READ_CTX, ref data))
                        rc = RC.NOMEM;
                }
            }
            else if (isMainJournal && pg == null)
            {
                // If this is a rollback of a savepoint and data was not written to the database and the page is not in-memory, there is a potential
                // problem. When the page is next fetched by the b-tree layer, it will be read from the database file, which may or may not be 
                // current. 
                //
                // There are a couple of different ways this can happen. All are quite obscure. When running in synchronous mode, this can only happen 
                // if the page is on the free-list at the start of the transaction, then populated, then moved using sqlite3PagerMovepage().
                //
                // The solution is to add an in-memory page to the cache containing the data just read from the sub-journal. Mark the page as dirty 
                // and if the pager requires a journal-sync, then mark the page as requiring a journal-sync before it is written.
                Debug.Assert(isSavepoint);
                Debug.Assert(DoNotSpill == 0);
                DoNotSpill++;
                rc = Acquire(id, ref pg, 1);
                Debug.Assert(DoNotSpill == 1);
                DoNotSpill--;
                if (rc != RC.OK)
                    return rc;
                pg.Flags &= ~PgHdr.PGHDR.NEED_READ;
                PCache.MakeDirty(pg);
            }
            if (pg != null)
            {
                // No page should ever be explicitly rolled back that is in use, except for page 1 which is held in use in order to keep the lock on the
                // database active. However such a page may be rolled back as a result of an internal error resulting in an automatic call to
                // sqlite3PagerRollback().
                var pageData = pg.Data;
                Buffer.BlockCopy(data, 0, pageData, 0, PageSize);
                Reiniter(pg);
                if (isMainJournal && (!isSavepoint || offset <= JournalHeader))
                {
                    // If the contents of this page were just restored from the main journal file, then its content must be as they were when the 
                    // transaction was first opened. In this case we can mark the page as clean, since there will be no need to write it out to the
                    // database.
                    //
                    // There is one exception to this rule. If the page is being rolled back as part of a savepoint (or statement) rollback from an 
                    // unsynced portion of the main journal file, then it is not safe to mark the page as clean. This is because marking the page as
                    // clean will clear the PGHDR_NEED_SYNC flag. Since the page is already in the journal file (recorded in Pager.pInJournal) and
                    // the PGHDR_NEED_SYNC flag is cleared, if the page is written to again within this transaction, it will be marked as dirty but
                    // the PGHDR_NEED_SYNC flag will not be set. It could then potentially be written out into the database file before its journal file
                    // segment is synced. If a crash occurs during or following this, database corruption may ensue.
                    Debug.Assert(!UseWal());
                    PCache.MakeClean(pg);
                }
                pager_set_pagehash(pg);

                // If this was page 1, then restore the value of Pager.dbFileVers. Do this before any decoding.
                if (id == 1)
                    Buffer.BlockCopy(pageData, 24, DBFileVersion, 0, DBFileVersion.Length);

                // Decode the page just read from disk
                if (CODEC1(this, pageData, pg.ID, codec_ctx.DECRYPT)) rc = RC.NOMEM;
                PCache.Release(pg);
            }
            return rc;
        }

        private RC pager_delmaster(string master)
        {
            var vfs = Vfs;

            // Allocate space for both the pJournal and pMaster file descriptors. If successful, open the master journal file for reading.
            var masterFile = new CoreVFile();   // Malloc'd master-journal file descriptor
            var journalFile = new CoreVFile();   // Malloc'd child-journal file descriptor
            VFileSystem.OPEN dummy;
            var rc = vfs.Open(master, masterFile, VFileSystem.OPEN.READONLY | VFileSystem.OPEN.MASTER_JOURNAL, out dummy);
            if (rc != RC.OK) goto delmaster_out;

            // Load the entire master journal file into space obtained from sqlite3_malloc() and pointed to by zMasterJournal.   Also obtain
            // sufficient space (in zMasterPtr) to hold the names of master journal files extracted from regular rollback-journals.
            long masterJournalSize; // Size of master journal file
            rc = masterFile.get_FileSize(out masterJournalSize);
            if (rc != RC.OK) goto delmaster_out;
            var masterPtrSize = vfs.MaxPathname + 1; // Amount of space allocated to zMasterPtr[]
            var masterJournal = new byte[masterJournalSize + 1];
            string masterPtr;
            rc = masterFile.Read(masterJournal, (int)masterJournalSize, 0);
            if (rc != RC.OK) goto delmaster_out;
            masterJournal[masterJournalSize] = 0;

            var journalIdx = 0; // Pointer to one journal within MJ file
            while (journalIdx < masterJournalSize)
            {
                var journal = "SETME";
                int exists;
                rc = vfs.Access(journal, VFileSystem.ACCESS.EXISTS, out exists);
                if (rc != RC.OK)
                    goto delmaster_out;
                if (exists != 0)
                {
                    // One of the journals pointed to by the master journal exists. Open it and check if it points at the master journal. If
                    // so, return without deleting the master journal file.
                    VFileSystem.OPEN dummy2;
                    rc = vfs.Open(journal, journalFile, VFileSystem.OPEN.READONLY | VFileSystem.OPEN.MAIN_JOURNAL, out dummy2);
                    if (rc != RC.OK)
                        goto delmaster_out;

                    rc = readMasterJournal(journalFile, out masterPtr, (uint)masterPtrSize);
                    journalFile.Close();
                    if (rc != RC.OK)
                        goto delmaster_out;

                    var c = string.Equals(master, masterPtr);
                    if (c) // We have a match. Do not delete the master journal file.
                        goto delmaster_out;
                }
                journalIdx += (sqlite3Strlen30(journal) + 1);
            }

            masterFile.Close();
            rc = vfs.Delete(master, false);

        delmaster_out:
            masterJournal = null;
            if (masterFile != null)
            {
                masterFile.Close();
                Debug.Assert(!masterFile.Opened);
                masterFile = null;
            }
            return rc;
        }

        private RC pager_truncate(Pid pages)
        {
            Debug.Assert(State != PAGER.ERROR);
            Debug.Assert(State != PAGER.READER);

            var rc = RC.OK;
            if (File.Opened && (State >= PAGER.WRITER_DBMOD || State == PAGER.OPEN))
            {
                var sizePage = PageSize;
                Debug.Assert(Lock == VFile.LOCK.EXCLUSIVE);
                // TODO: Is it safe to use Pager.dbFileSize here?
                long currentSize;
                rc = File.get_FileSize(out currentSize);
                var newSize = sizePage * pages;
                if (rc == RC.OK && currentSize != newSize)
                {
                    if (currentSize > newSize)
                        rc = File.Truncate(newSize);
                    else
                    {
                        var tmp = TmpSpace;
                        Array.Clear(tmp, 0, sizePage);
                        rc = File.Write(tmp, sizePage, newSize - sizePage);
                    }
                    if (rc == RC.OK)
                        DBSize = pages;
                }
            }
            return rc;
        }

        #endregion

        #region Transaction2

        uint sqlite3SectorSize(VFile file)
        {
            var ret = file.SectorSize;
            if (ret < 32)
                ret = 512;
            else if (ret > MAX_SECTOR_SIZE)
            {
                Debug.Assert(MAX_SECTOR_SIZE >= 512);
                ret = MAX_SECTOR_SIZE;
            }
            return ret;
        }

        void setSectorSize()
        {
            Debug.Assert(File.Opened || TempFile);
            if (TempFile || (File.get_DeviceCharacteristics() & VFile.IOCAP.POWERSAFE_OVERWRITE) != 0)
                SectorSize = 512; // Sector size doesn't matter for temporary files. Also, the file may not have been opened yet, in which case the OsSectorSize() call will segfault.
            else
                SectorSize = File.SectorSize;
        }

        private RC pager_playback(bool isHot)
        {
            // Figure out how many records are in the journal.  Abort early if the journal is empty.
            Debug.Assert(JournalFile.Opened);
            long sizeJournal; // Size of the journal file in bytes
            RC rc = JournalFile.get_FileSize(out sizeJournal);
            if (rc != RC.OK)
                goto end_playback;

            // Read the master journal name from the journal, if it is present. If a master journal file name is specified, but the file is not
            // present on disk, then the journal is not hot and does not need to be played back.
            //
            // TODO: Technically the following is an error because it assumes that buffer Pager.pTmpSpace is (mxPathname+1) bytes or larger. i.e. that
            // (pPager->pageSize >= pPager->pVfs->mxPathname+1). Using os_unix.c, mxPathname is 512, which is the same as the minimum allowable value
            // for pageSize.
            var vfs = Vfs;
            string master; // Name of master journal file if any
            rc = readMasterJournal(JournalFile, out master, (uint)vfs.MaxPathname + 1);
            var res = 1;
            if (rc == RC.OK && master[0] != 0)
                rc = vfs.Access(master, VFileSystem.ACCESS.EXISTS, out res);
            master = null;
            if (rc != RC.OK || res == 0)
                goto end_playback;
            JournalOffset = 0;
            bool needPagerReset = isHot; // True to reset page prior to first page rollback

            // This loop terminates either when a readJournalHdr() or pager_playback_one_page() call returns SQLITE_DONE or an IO error occurs.
            while (true)
            {
                // Read the next journal header from the journal file.  If there are not enough bytes left in the journal file for a complete header, or
                // it is corrupted, then a process must have failed while writing it. This indicates nothing more needs to be rolled back.
                uint records; // Number of Records in the journal
                Pid maxPage = 0; // Size of the original file in pages
                rc = readJournalHdr(isHot, sizeJournal, ref records, ref maxPage);
                if (rc != RC.OK)
                {
                    if (rc == RC.DONE)
                        rc = RC.OK;
                    goto end_playback;
                }

                // If nRec is 0xffffffff, then this journal was created by a process working in no-sync mode. This means that the rest of the journal
                // file consists of pages, there are no more journal headers. Compute the value of nRec based on this assumption.
                if (records == 0xffffffff)
                {
                    Debug.Assert(JournalOffset == JOURNAL_HDR_SZ(this));
                    records = (uint)((sizeJournal - JOURNAL_HDR_SZ(this)) / JOURNAL_PG_SZ(this));
                }

                // If nRec is 0 and this rollback is of a transaction created by this process and if this is the final header in the journal, then it means
                // that this part of the journal was being filled but has not yet been synced to disk.  Compute the number of pages based on the remaining
                // size of the file.
                //
                // The third term of the test was added to fix ticket #2565. When rolling back a hot journal, nRec==0 always means that the next
                // chunk of the journal contains zero pages to be rolled back.  But when doing a ROLLBACK and the nRec==0 chunk is the last chunk in
                // the journal, it means that the journal might contain additional pages that need to be rolled back and that the number of pages 
                // should be computed based on the journal file size.
                if (records == 0 && !isHot && JournalHeader + JOURNAL_HDR_SZ(this) == JournalOffset)
                    records = (uint)((sizeJournal - JournalOffset) / JOURNAL_PG_SZ(this));

                // If this is the first header read from the journal, truncate the database file back to its original size.
                if (JournalOffset == JOURNAL_HDR_SZ(this))
                {
                    rc = pager_truncate(maxPage);
                    if (rc != RC.OK)
                        goto end_playback;
                    DBSize = maxPage;
                }

                // Copy original pages out of the journal and back into the database file and/or page cache.
                for (var u = 0U; u < records; u++)
                {
                    if (needPagerReset)
                    {
                        pager_reset();
                        needPagerReset = false;
                    }
                    rc = pager_playback_one_page(ref JournalOffset, null, true, false);
                    if (rc != RC.OK)
                        if (rc == RC.DONE)
                        {
                            JournalOffset = sizeJournal;
                            break;
                        }
                        else if (rc == RC.IOERR_SHORT_READ)
                        {
                            // If the journal has been truncated, simply stop reading and processing the journal. This might happen if the journal was
                            // not completely written and synced prior to a crash.  In that case, the database should have never been written in the
                            // first place so it is OK to simply abandon the rollback.
                            rc = RC.OK;
                            goto end_playback;
                        }
                        else
                        {
                            // If we are unable to rollback, quit and return the error code.  This will cause the pager to enter the error state
                            // so that no further harm will be done.  Perhaps the next process to come along will be able to rollback the database.
                            goto end_playback;
                        }
                }
            }
            Debug.Assert(false);

        end_playback:
            // Following a rollback, the database file should be back in its original state prior to the start of the transaction, so invoke the
            // SQLITE_FCNTL_DB_UNCHANGED file-control method to disable the assertion that the transaction counter was modified.
#if DEBUG
            long dummy = 0;
            File.FileControl(VFile.FCNTL.DB_UNCHANGED, ref dummy);
            // TODO: verfiy this, because its different then the c# version
#endif

            // If this playback is happening automatically as a result of an IO or malloc error that occurred after the change-counter was updated but 
            // before the transaction was committed, then the change-counter modification may just have been reverted. If this happens in exclusive 
            // mode, then subsequent transactions performed by the connection will not update the change-counter at all. This may lead to cache inconsistency
            // problems for other processes at some point in the future. So, just in case this has happened, clear the changeCountDone flag now.
            ChangeCountDone = TempFile;

            if (rc == RC.OK)
                rc = readMasterJournal(JournalFile, out master, (uint)Vfs.MaxPathname + 1);
            if (rc == RC.OK && (State >= PAGER.WRITER_DBMOD || State == PAGER.OPEN))
                rc = Sync();
            if (rc == RC.OK)
                rc = pager_end_transaction(master[0] != '\0', false);
            if (rc == RC.OK && master[0] != '\0' && res != 0)
                // If there was a master journal and this routine will return success, see if it is possible to delete the master journal.
                rc = pager_delmaster(master);

            // The Pager.sectorSize variable may have been updated while rolling back a journal created by a process with a different sector size
            // value. Reset it to the correct value for this process.
            setSectorSize();
            return rc;
        }

        private static RC readDbPage(PgHdr page)
        {
            var pager = page.Pager; // Pager object associated with page pPg

            Debug.Assert(pager.State >= PAGER.READER && !pager.MemoryDB);
            Debug.Assert(pager.File.Opened);

            if (SysEx.NEVER(!pager.File.Opened))
            {
                Debug.Assert(pager.TempFile);
                Array.Clear(page.Data, 0, pager.PageSize);
                return RC.OK;
            }

            var rc = RC.OK;
            var id = page.ID; // Page number to read
            var isInWal = 0; // True if page is in log file
            var pageSize = pager.PageSize; // Number of bytes to read
            if (pager.UseWal()) // Try to pull the page from the write-ahead log.
                rc = pager.Wal.Read(id, ref isInWal, pageSize, page.Data);
            if (rc == RC.OK && isInWal == 0)
            {
                var offset = (id - 1) * (long)pager.PageSize;
                rc = pager.File.Read(page.Data, pageSize, offset);
                if (rc == RC.IOERR_SHORT_READ)
                    rc = RC.OK;
            }

            if (id == 1)
            {
                // If the read is unsuccessful, set the dbFileVers[] to something that will never be a valid file version.  dbFileVers[] is a copy
                // of bytes 24..39 of the database.  Bytes 28..31 should always be zero or the size of the database in page. Bytes 32..35 and 35..39
                // should be page numbers which are never 0xffffffff.  So filling pPager->dbFileVers[] with all 0xff bytes should suffice.
                //
                // For an encrypted database, the situation is more complex:  bytes 24..39 of the database are white noise.  But the probability of
                // white noising equaling 16 bytes of 0xff is vanishingly small so we should still be ok.
                if (rc != 0)
                    for (int i = 0; i < pager.DBFileVersion.Length; pager.DBFileVersion[i++] = 0xff) ; //_memset(pager->DBFileVersion, 0xff, sizeof(pager->DBFileVersion));
                else
                    Buffer.BlockCopy(page.Data, 24, pager.DBFileVersion, 0, pager.DBFileVersion.Length);
            }
            if (CODEC1(pager, page.Data, id, codec_ctx.DECRYPT))
                rc = RC.NOMEM;

            PAGER_INCR(sqlite3_pager_readdb_count);
            PAGER_INCR(pager.Reads);
            SysEx.IOTRACE("PGIN {0:x} {1}", pager.GetHashCode(), id);
            PAGERTRACE("FETCH {0} page {1}% hash({2,08:x})", PAGERID(pager), id, pager_pagehash(page));

            return rc;
        }

        private static void pager_write_changecounter(PgHdr pg)
        {
            // Increment the value just read and write it back to byte 24.
            uint change_counter = ConvertEx.Get4(pg.Pager.DBFileVersion, 0) + 1;
            ConvertEx.Put4(pg.Data, 24, change_counter);

            // Also store the SQLite version number in bytes 96..99 and in bytes 92..95 store the change counter for which the version number is valid.
            ConvertEx.Put4(pg.Data, 92, change_counter);
            ConvertEx.Put4(pg.Data, 96, SysEx.VERSION_NUMBER);
        }

#if !OMIT_WAL
        static int pagerUndoCallback(Pager pCtx, Pgno iPg)
        {
            var rc = RC.OK;
            Pager pPager = (Pager)pCtx;
            PgHdr pPg;

            pPg = sqlite3PagerLookup(pPager, iPg);
            if (pPg)
            {
                if (sqlite3PcachePageRefcount(pPg) == 1)
                {
                    sqlite3PcacheDrop(pPg);
                }
                else
                {
                    rc = readDbPage(pPg);
                    if (rc == SQLITE.OK)
                    {
                        pPager._reiniter(pPg);
                    }
                    sqlite3PagerUnref(pPg);
                }
            }
            // Normally, if a transaction is rolled back, any backup processes are updated as data is copied out of the rollback journal and into the
            // database. This is not generally possible with a WAL database, as rollback involves simply truncating the log file. Therefore, if one
            // or more frames have already been written to the log (and therefore also copied into the backup databases) as part of this transaction,
            // the backups must be restarted.
            sqlite3BackupRestart(pPager.pBackup);
            return rc;
        }

        static int pagerRollbackWal(Pager pPager)
        {
            int rc;                         /* Return Code */
            PgHdr pList;                   /* List of dirty pages to revert */

            /* For all pages in the cache that are currently dirty or have already been written (but not committed) to the log file, do one of the 
            ** following:
            **   + Discard the cached page (if refcount==0), or
            **   + Reload page content from the database (if refcount>0).
            */
            pPager._dbSize = pPager._dbOrigSize;
            rc = sqlite3WalUndo(pPager._wal, pagerUndoCallback, pPager);
            pList = sqlite3PcacheDirtyList(pPager._pcache);
            while (pList && rc == SQLITE.OK)
            {
                PgHdr pNext = pList.pDirty;
                rc = pagerUndoCallback(pPager, pList.pgno);
                pList = pNext;
            }

            return rc;
        }


        static int pagerWalFrames(Pager pPager, PgHdr pList, Pgno nTruncate, int isCommit, int syncFlags)
        {
            int rc;                         /* Return code */
#if DEBUG || CHECK_PAGES
            PgHdr p;                       /* For looping over pages */
#endif

            Debug.Assert(pPager._wal);
#if DEBUG
/* Verify that the page list is in accending order */
for(p=pList; p && p->pDirty; p=p->pDirty){
assert( p->pgno < p->pDirty->pgno );
}
#endif

            if (isCommit)
            {
                /* If a WAL transaction is being committed, there is no point in writing
                ** any pages with page numbers greater than nTruncate into the WAL file.
                ** They will never be read by any client. So remove them from the pDirty
                ** list here. */
                PgHdr* p;
                PgHdr** ppNext = &pList;
                for (p = pList; (*ppNext = p); p = p->pDirty)
                {
                    if (p->pgno <= nTruncate) ppNext = &p->pDirty;
                }
                assert(pList);
            }


            if (pList->pgno == 1) pager_write_changecounter(pList);
            rc = sqlite3WalFrames(pPager._wal,
            pPager._pageSize, pList, nTruncate, isCommit, syncFlags
            );
            if (rc == SQLITE.OK && pPager.pBackup)
            {
                PgHdr* p;
                for (p = pList; p; p = p->pDirty)
                {
                    sqlite3BackupUpdate(pPager.pBackup, p->pgno, (u8*)p->pData);
                }
            }

#if CHECK_PAGES
pList = sqlite3PcacheDirtyList(pPager.pPCache);
for(p=pList; p; p=p->pDirty){
pager_set_pagehash(p);
}
#endif

            return rc;
        }

        static int pagerBeginReadTransaction(Pager* pPager)
        {
            int rc;                         /* Return code */
            int changed = 0;                /* True if cache must be reset */

            assert(_pagerUseWal(pPager));
            assert(pPager.eState == PAGER_OPEN || pPager.eState == PAGER_READER);

            /* sqlite3WalEndReadTransaction() was not called for the previous
            ** transaction in locking_mode=EXCLUSIVE.  So call it now.  If we
            ** are in locking_mode=NORMAL and EndRead() was previously called,
            ** the duplicate call is harmless.
            */
            sqlite3WalEndReadTransaction(pPager.pWal);

            rc = sqlite3WalBeginReadTransaction(pPager.pWal, &changed);
            if (rc != SQLITE.OK || changed)
            {
                pager_reset(pPager);
            }

            return rc;
        }
#endif

        private RC pagerPagecount(out Pid pagesOut)
        {
            // Query the WAL sub-system for the database size. The WalDbsize() function returns zero if the WAL is not open (i.e. Pager.pWal==0), or
            // if the database size is not available. The database size is not available from the WAL sub-system if the log file is empty or
            // contains no valid committed transactions.
            Debug.Assert(State == PAGER.OPEN);
            Debug.Assert(Lock >= VFile.LOCK.SHARED);
            var pages = Wal.DBSize();

            // If the database size was not available from the WAL sub-system, determine it based on the size of the database file. If the size
            // of the database file is not an integer multiple of the page-size, round down to the nearest page. Except, any file larger than 0
            // bytes in size is considered to contain at least one page.
            if (pages == 0)
            {
                Debug.Assert(File.Opened || TempFile);
                var n = 0L; // Size of db file in bytes
                if (File.Opened)
                {
                    var rc = File.get_FileSize(out n);
                    if (rc != RC.OK)
                        return rc;
                }
                pages = (Pid)((n + PageSize - 1) / PageSize);
            }

            // If the current number of pages in the file is greater than the configured maximum pager number, increase the allowed limit so
            // that the file can be read.
            if (pages > MaxPid)
                MaxPid = (Pid)pages;

            pagesOut = pages;
            return RC.OK;
        }

#if !OMIT_WAL
        static int pagerOpenWalIfPresent(Pager* pPager)
        {
            int rc = SQLITE.OK;
            Debug.Assert(pPager.eState == PAGER_OPEN);
            Debug.Assert(pPager.eLock >= SHARED_LOCK || pPager.noReadlock);

            if (!pPager.tempFile)
            {
                int isWal;                    /* True if WAL file exists */
                Pgno nPage;                   /* Size of the database file */

                rc = pagerPagecount(pPager, &nPage);
                if (rc) return rc;
                if (nPage == 0)
                {
                    rc = sqlite3OsDelete(pPager.pVfs, pPager.zWal, 0);
                    isWal = 0;
                }
                else
                {
                    rc = sqlite3OsAccess(
                    pPager.pVfs, pPager.zWal, SQLITE_ACCESS_EXISTS, &isWal
                    );
                }
                if (rc == SQLITE.OK)
                {
                    if (isWal)
                    {
                        testcase(sqlite3PcachePagecount(pPager.pPCache) == 0);
                        rc = sqlite3PagerOpenWal(pPager, 0);
                    }
                    else if (pPager.journalMode == PAGER_JOURNALMODE_WAL)
                    {
                        pPager.journalMode = PAGER_JOURNALMODE_DELETE;
                    }
                }
            }
            return rc;
        }
#endif

        private RC pagerPlaybackSavepoint(PagerSavepoint savepoint)
        {
            Debug.Assert(State != PAGER.ERROR);
            Debug.Assert(State >= PAGER.WRITER_LOCKED);

            // Allocate a bitvec to use to store the set of pages rolled back
            Bitvec done = null; // Bitvec to ensure pages played back only once
            if (savepoint != null)
                done = new Bitvec(savepoint.Orig);

            // Set the database size back to the value it was before the savepoint being reverted was opened.
            DBSize = (savepoint != null ? savepoint.Orig : DBOrigSize);
            ChangeCountDone = TempFile;

            if (savepoint != null && UseWal())
                return pagerRollbackWal();

            // Use pPager->journalOff as the effective size of the main rollback journal.  The actual file might be larger than this in
            // PAGER_JOURNALMODE_TRUNCATE or PAGER_JOURNALMODE_PERSIST.  But anything past pPager->journalOff is off-limits to us.
            var sizeJournal = JournalOffset; // Effective size of the main journal
            Debug.Assert(!UseWal() || sizeJournal == 0);

            // Begin by rolling back records from the main journal starting at PagerSavepoint.iOffset and continuing to the next journal header.
            // There might be records in the main journal that have a page number greater than the current database size (pPager->dbSize) but those
            // will be skipped automatically.  Pages are added to pDone as they are played back.

            var rc = RC.OK;
            if (savepoint != null && !UseWal())
            {
                long hdrOffset = (savepoint.HdrOffset != 0 ? savepoint.HdrOffset : sizeJournal); // End of first segment of main-journal records
                JournalOffset = savepoint.Offset;
                while (rc == RC.OK && JournalOffset < hdrOffset)
                    rc = pager_playback_one_page(ref JournalOffset, done, true, true);
                Debug.Assert(rc != RC.DONE);
            }
            else
                JournalOffset = 0;

            // Continue rolling back records out of the main journal starting at the first journal header seen and continuing until the effective end
            // of the main journal file.  Continue to skip out-of-range pages and continue adding pages rolled back to pDone.
            while (rc == RC.OK && JournalOffset < sizeJournal)
            {
                uint records; // Number of Journal Records
                uint dummy;
                rc = readJournalHdr(false, sizeJournal, ref records, ref dummy);
                Debug.Assert(rc != RC.DONE);

                // The "pPager.journalHdr+JOURNAL_HDR_SZ(pPager)==pPager.journalOff" test is related to ticket #2565.  See the discussion in the
                // pager_playback() function for additional information.
                if (records == 0 && JournalHeader + JOURNAL_HDR_SZ(this) >= JournalOffset)
                    records = (uint)((sizeJournal - JournalOffset) / JOURNAL_PG_SZ(this));
                for (var ii = 0U; rc == RC.OK && ii < records && JournalOffset < sizeJournal; ii++)
                    rc = pager_playback_one_page(ref JournalOffset, done, true, true);
                Debug.Assert(rc != RC.DONE);
            }
            Debug.Assert(rc != RC.OK || JournalOffset >= sizeJournal);

            // Finally,  rollback pages from the sub-journal.  Page that were previously rolled back out of the main journal (and are hence in pDone)
            // will be skipped.  Out-of-range pages are also skipped.
            if (savepoint != null)
            {
                long offset = savepoint.SubRecords * (4 + PageSize);
                if (UseWal())
                    rc = Wal.SavepointUndo(savepoint.WalData);
                for (var ii = savepoint.SubRecords; rc == RC.OK && ii < SubRecords; ii++)
                {
                    Debug.Assert(offset == ii * (4 + PageSize));
                    rc = pager_playback_one_page(ref offset, done, false, true);
                }
                Debug.Assert(rc != RC.DONE);
            }

            Bitvec.Destroy(ref done);
            if (rc == RC.OK)
                JournalOffset = (int)sizeJournal;

            return rc;
        }

        #endregion

        #region Name3

        // was:sqlite3PagerSetCachesize
        public void SetCacheSize(int maxPage)
        {
            PCache.SetCachesize(maxPage);
        }

        void sqlite3PagerShrink(Pager pager)
        {
            PCache.Shrink();
        }

#if !OMIT_PAGER_PRAGMAS
        // was:sqlite3PagerSetSafetyLevel
        public void SetSafetyLevel(int level, bool fullFsync, bool checkpointFullFsync)
        {
            Debug.Assert(level >= 1 && level <= 3);
            NoSync = (level == 1 || TempFile);
            FullSync = (level == 3 && !TempFile);
            if (NoSync)
            {
                SyncFlags = 0;
                CheckpointSyncFlags = 0;
            }
            else if (fullFsync)
            {
                SyncFlags = VFile.SYNC.FULL;
                CheckpointSyncFlags = VFile.SYNC.FULL;
            }
            else if (checkpointFullFsync)
            {
                SyncFlags = VFile.SYNC.NORMAL;
                CheckpointSyncFlags = VFile.SYNC.FULL;
            }
            else
            {
                SyncFlags = VFile.SYNC.NORMAL;
                CheckpointSyncFlags = VFile.SYNC.NORMAL;
            }
            WalSyncFlags = SyncFlags;
            if (FullSync)
                WalSyncFlags |= VFile.SYNC.WAL_TRANSACTIONS;
        }
#endif

#if TEST
        // The following global variable is incremented whenever the library attempts to open a temporary file.  This information is used for testing and analysis only.  
        int sqlite3_opentemp_count = 0;
#endif

        private RC pagerOpentemp(ref VFile file, VFileSystem.OPEN vfsFlags)
        {
#if TEST
            sqlite3_opentemp_count++; // Used for testing and analysis only
#endif
            vfsFlags |= VFileSystem.OPEN.READWRITE | VFileSystem.OPEN.CREATE | VFileSystem.OPEN.EXCLUSIVE | VFileSystem.OPEN.DELETEONCLOSE;
            VFileSystem.OPEN dummy = 0;
            var rc = Vfs.Open(null, file, vfsFlags, out dummy);
            Debug.Assert(rc != RC.OK || file.Opened);
            return rc;
        }

        // was:sqlite3PagerSetBusyhandler
        public void SetBusyHandler(Func<object, int> busyHandler, object busyHandlerArg)
        {
            BusyHandler = busyHandler;
            BusyHandlerArg = busyHandlerArg;

            if (File.Opened)
            {
                //object ap = BusyHandler;
                //Debug.Assert(ap[0] == busyHandler);
                //Debug.Assert(ap[1] == busyHandlerArg);
                // TODO: This
                //File.FileControl(VFile.FCNTL.BUSYHANDLER, (void *)ap);
            }
        }

        // was:sqlite3PagerSetPagesize
        public RC SetPageSize(ref uint pageSizeRef, int reserveBytes)
        {
            // It is not possible to do a full assert_pager_state() here, as this function may be called from within PagerOpen(), before the state
            // of the Pager object is internally consistent.
            //
            // At one point this function returned an error if the pager was in PAGER_ERROR state. But since PAGER_ERROR state guarantees that
            // there is at least one outstanding page reference, this function is a no-op for that case anyhow.
            var pageSize = pageSizeRef;
            Debug.Assert(pageSize == 0 || (pageSize >= 512 && pageSize <= MAX_PAGE_SIZE));
            var rc = RC.OK;
            if ((!MemoryDB || DBSize == 0) &&
                PCache.get_Refs() == 0 &&
                pageSize != 0 && pageSize != (uint)PageSize)
            {
                long bytes = 0;
                if (State > PAGER.OPEN && File.Opened)
                    rc = File.get_FileSize(out bytes);
                byte[] tempSpace = null; // New temp space
                if (rc == RC.OK)
                {
                    tempSpace = sqlite3PageMalloc(pageSize); //MallocEx.sqlite3Malloc((int)pageSize)
                }
                if (rc == RC.OK)
                {
                    pager_reset();
                    DBSize = (Pid)((bytes + pageSize - 1) / pageSize);
                    PageSize = (int)pageSize;
                    sqlite3PageFree(ref TmpSpace);
                    TmpSpace = tempSpace;
                    PCache.SetPageSize((int)pageSize);
                }
            }
            pageSizeRef = (uint)PageSize;
            if (rc == RC.OK)
            {
                if (reserveBytes < 0) reserveBytes = ReserveBytes;
                Debug.Assert(reserveBytes >= 0 && reserveBytes < 1000);
                ReserveBytes = (short)reserveBytes;
                pagerReportSize();
            }
            return rc;
        }

        public byte[] sqlite3PagerTempSpace()
        {
            return TmpSpace;
        }

        // was:sqlite3PagerMaxPageCount
        public Pid SetMaxPageCount(int maxPage)
        {
            if (maxPage > 0)
                MaxPid = (Pid)maxPage;
            Debug.Assert(State != PAGER.OPEN);  // Called only by OP_MaxPgcnt
            Debug.Assert(MaxPid >= DBSize);     // OP_MaxPgcnt enforces this
            return MaxPid;
        }

#if TEST
        //extern int sqlite3_io_error_pending;
        //extern int sqlite3_io_error_hit;
        static int saved_cnt;

        void disable_simulated_io_errors()
        {
            saved_cnt = sqlite3_io_error_pending;
            sqlite3_io_error_pending = -1;
        }

        void enable_simulated_io_errors()
        {
            sqlite3_io_error_pending = saved_cnt;
        }
#else
        void disable_simulated_io_errors() {}
        void enable_simulated_io_errors() {}
#endif

        public RC ReadFileHeader(int n, byte[] dest)
        {
            Array.Clear(dest, 0, n);
            Debug.Assert(File.Opened || TempFile);

            // This routine is only called by btree immediately after creating the Pager object.  There has not been an opportunity to transition to WAL mode yet.
            Debug.Assert(!UseWal());

            var rc = RC.OK;
            if (File.Opened)
            {
                SysEx.IOTRACE("DBHDR {0} 0 {1}", GetHashCode(), n);
                rc = File.Read(dest, n, 0);
                if (rc == RC.IOERR_SHORT_READ)
                    rc = RC.OK;
            }
            return rc;
        }

        // was:sqlite3PagerPagecount
        public void GetPageCount(out Pid pagesOut)
        {
            Debug.Assert(State >= PAGER.READER);
            Debug.Assert(State != PAGER.WRITER_FINISHED);
            pagesOut = DBSize;
        }

        private RC pager_wait_on_lock(VFile.LOCK locktype)
        {
            // Check that this is either a no-op (because the requested lock is already held, or one of the transistions that the busy-handler
            // may be invoked during, according to the comment above sqlite3PagerSetBusyhandler().
            Debug.Assert((Lock >= locktype) ||
                (Lock == VFile.LOCK.NO && locktype == VFile.LOCK.SHARED) ||
                (Lock == VFile.LOCK.RESERVED && locktype == VFile.LOCK.EXCLUSIVE));

            RC rc;
            do
                rc = pagerLockDb(locktype);
            while (rc == RC.BUSY && BusyHandler(BusyHandlerArg) != 0);
            return rc;
        }

#if DEBUG
        static void assertTruncateConstraintCb(PgHdr page)
        {
            Debug.Assert((page.Flags & PgHdr.PGHDR.DIRTY) != 0);
            Debug.Assert(!subjRequiresPage(page) || page.ID <= page.Pager.DBSize);
        }
        void assertTruncateConstraint()
        {
            PCache.IterateDirty(assertTruncateConstraintCb);
        }
#else
        void assertTruncateConstraint() { }
#endif

        // was:sqlite3PagerTruncateImage
        public void TruncateImage(Pid pages)
        {
            Debug.Assert(DBSize >= pages);
            Debug.Assert(State >= PAGER.WRITER_CACHEMOD);
            DBSize = pages;

            // At one point the code here called assertTruncateConstraint() to ensure that all pages being truncated away by this operation are,
            // if one or more savepoints are open, present in the savepoint journal so that they can be restored if the savepoint is rolled
            // back. This is no longer necessary as this function is now only called right before committing a transaction. So although the 
            // Pager object may still have open savepoints (Pager.nSavepoint!=0), they cannot be rolled back. So the assertTruncateConstraint() call
            // is no longer correct.
        }

        private RC pagerSyncHotJournal()
        {
            var rc = RC.OK;
            if (!NoSync)
                rc = JournalFile.Sync(VFile.SYNC.NORMAL);
            if (rc == RC.OK)
                rc = JournalFile.get_FileSize(out JournalHeader);
            return rc;
        }

        public RC Close()
        {
            Debug.Assert(assert_pager_state());
            disable_simulated_io_errors();
            SysEx.BeginBenignAlloc();
            ExclusiveMode = false;
            var tmp = TmpSpace;
#if !OMIT_WAL
            Wal.sqlite3WalClose(CheckpointSyncFlags, PageSize, tmp);
            Wal = null;
#endif
            pager_reset();
            if (MemoryDB)
                pager_unlock();
            else
            {
                // If it is open, sync the journal file before calling UnlockAndRollback. If this is not done, then an unsynced portion of the open journal 
                // file may be played back into the database. If a power failure occurs while this is happening, the database could become corrupt.
                //
                // If an error occurs while trying to sync the journal, shift the pager into the ERROR state. This causes UnlockAndRollback to unlock the
                // database and close the journal file without attempting to roll it back or finalize it. The next database user will have to do hot-journal
                // rollback before accessing the database file.
                if (JournalFile.Opened)
                    pager_error(pagerSyncHotJournal());
                pagerUnlockAndRollback();
            }
            SysEx.EndBenignAlloc();
            PAGERTRACE("CLOSE {0}", PAGERID(this));
            SysEx.IOTRACE("CLOSE {0:x}", GetHashCode());
            JournalFile.Close();
            File.Close();
            sqlite3PageFree(tmp);
            PCache.Close();

#if HAS_CODEC
			if (CodecFree) CodecFree(Codec);
#endif

            Debug.Assert(Savepoints == null && !InJournal);
            Debug.Assert(!JournalFile.Opened && !SubJournalFile.Opened);

            //this = null;
            return RC.OK;
        }

#if !DEBUG || TEST
        // was:sqlite3PagerPagenumber
        public static Pid GetPageID(IPage pg)
        {
            return pg.ID;
        }
#endif

        // was:sqlite3PagerRef
        public static void AddPageRef(IPage pg)
        {
            PCache.Ref(pg);
        }

        #endregion

        #region Main

        private RC syncJournal(bool newHeader)
        {

            Debug.Assert(State == PAGER.WRITER_CACHEMOD || State == PAGER.WRITER_DBMOD);
            Debug.Assert(assert_pager_state());
            Debug.Assert(!UseWal());

            var rc = sqlite3PagerExclusiveLock();
            if (rc != RC.OK) return rc;

            if (!NoSync)
            {
                Debug.Assert(!TempFile);
                if (JournalFile.Opened && JournalMode != IPager.JOURNALMODE.JMEMORY)
                {
                    var dc = File.get_DeviceCharacteristics();
                    Debug.Assert(JournalFile.Opened);

                    if ((dc & VFile.IOCAP.SAFE_APPEND) == 0)
                    {
                        // This block deals with an obscure problem. If the last connection that wrote to this database was operating in persistent-journal
                        // mode, then the journal file may at this point actually be larger than Pager.journalOff bytes. If the next thing in the journal
                        // file happens to be a journal-header (written as part of the previous connection's transaction), and a crash or power-failure 
                        // occurs after nRec is updated but before this connection writes anything else to the journal file (or commits/rolls back its 
                        // transaction), then SQLite may become confused when doing the hot-journal rollback following recovery. It may roll back all
                        // of this connections data, then proceed to rolling back the old, out-of-date data that follows it. Database corruption.
                        //
                        // To work around this, if the journal file does appear to contain a valid header following Pager.journalOff, then write a 0x00
                        // byte to the start of it to prevent it from being recognized.
                        //
                        // Variable iNextHdrOffset is set to the offset at which this problematic header will occur, if it exists. aMagic is used 
                        // as a temporary buffer to inspect the first couple of bytes of the potential journal header.
                        var header = new byte[_journalMagic.Length + 4];
                        _journalMagic.CopyTo(header, 0);
                        ConvertEx.Put4(header, _journalMagic.Length, Records);

                        var magic = new byte[8];
                        var nextHdrOffset = journalHdrOffset();
                        rc = JournalFile.Read(magic, 8, nextHdrOffset);

                        if (rc == RC.OK && Enumerable.SequenceEqual(magic, _journalMagic))
                        {
                            var zerobyte = new byte[1];
                            rc = JournalFile.Write(zerobyte, 1, nextHdrOffset);
                        }
                        if (rc != RC.OK && rc != RC.IOERR_SHORT_READ)
                            return rc;

                        // Write the nRec value into the journal file header. If in full-synchronous mode, sync the journal first. This ensures that
                        // all data has really hit the disk before nRec is updated to mark it as a candidate for rollback.
                        //
                        // This is not required if the persistent media supports the SAFE_APPEND property. Because in this case it is not possible 
                        // for garbage data to be appended to the file, the nRec field is populated with 0xFFFFFFFF when the journal header is written
                        // and never needs to be updated.
                        if (FullSync && (dc & VFile.IOCAP.SEQUENTIAL) == 0)
                        {
                            PAGERTRACE("SYNC journal of {0}", PAGERID(this));
                            SysEx.IOTRACE("JSYNC {0:x}", GetHashCode());
                            rc = JournalFile.Sync(SyncFlags);
                            if (rc != RC.OK) return rc;
                        }
                        SysEx.IOTRACE("JHDR {0:x} {1,11}", GetHashCode(), JournalHeader);
                        rc = JournalFile.Write(header, header.Length, JournalHeader);
                        if (rc != RC.OK) return rc;
                    }
                    if ((dc & VFile.IOCAP.SEQUENTIAL) == 0)
                    {
                        PAGERTRACE("SYNC journal of {0}", PAGERID(this));
                        SysEx.IOTRACE("JSYNC {0:x}", GetHashCode());
                        rc = JournalFile.Sync(SyncFlags | (SyncFlags == VFile.SYNC.FULL ? VFile.SYNC.DATAONLY : 0));
                        if (rc != RC.OK) return rc;
                    }

                    JournalHeader = JournalOffset;
                    if (newHeader && (dc & VFile.IOCAP.SAFE_APPEND) == 0)
                    {
                        Records = 0;
                        rc = writeJournalHdr();
                        if (rc != RC.OK) return rc;
                    }
                }
                else
                    JournalHeader = JournalOffset;
            }

            // Unless the pager is in noSync mode, the journal file was just successfully synced. Either way, clear the PGHDR_NEED_SYNC flag on all pages.
            PCache.ClearSyncFlags();
            State = PAGER.WRITER_DBMOD;
            Debug.Assert(assert_pager_state());
            return RC.OK;
        }

        private RC pager_write_pagelist(PgHdr list)
        {
            // This function is only called for rollback pagers in WRITER_DBMOD state.
            Debug.Assert(!UseWal());
            Debug.Assert(State == PAGER.WRITER_DBMOD);
            Debug.Assert(Lock == VFile.LOCK.EXCLUSIVE);

            // If the file is a temp-file has not yet been opened, open it now. It is not possible for rc to be other than SQLITE_OK if this branch
            // is taken, as pager_wait_on_lock() is a no-op for temp-files.
            var rc = RC.OK;
            if (!File.Opened)
            {
                Debug.Assert(TempFile && rc == RC.OK);
                rc = pagerOpentemp(ref File, VfsFlags);
            }

            // Before the first write, give the VFS a hint of what the final file size will be.
            Debug.Assert(rc != RC.OK || File.Opened);
            if (rc == RC.OK && DBSize > DBHintSize)
            {
                long sizeFile = PageSize * (long)DBSize;
                File.FileControl(VFile.FCNTL.SIZE_HINT, ref sizeFile);
                DBHintSize = DBSize;
            }

            while (rc == RC.OK && list != null)
            {
                var id = list.ID;

                // If there are dirty pages in the page cache with page numbers greater than Pager.dbSize, this means sqlite3PagerTruncateImage() was called to
                // make the file smaller (presumably by auto-vacuum code). Do not write any such pages to the file.
                //
                // Also, do not write out any page that has the PGHDR_DONT_WRITE flag set (set by sqlite3PagerDontWrite()).
                if (id <= DBSize && (list.Flags & PgHdr.PGHDR.DONT_WRITE) == 0)
                {
                    Debug.Assert((list.Flags & PgHdr.PGHDR.NEED_SYNC) == 0);
                    if (id == 1) pager_write_changecounter(list);

                    // Encode the database
                    byte[] data = null; // Data to write
                    if (CODEC2(this, list.Data, id, codec_ctx.ENCRYPT_WRITE_CTX, ref data)) return RC.NOMEM;

                    // Write out the page data.
                    long offset = (id - 1) * (long)PageSize; // Offset to write
                    rc = File.Write(data, PageSize, offset);

                    // If page 1 was just written, update Pager.dbFileVers to match the value now stored in the database file. If writing this 
                    // page caused the database file to grow, update dbFileSize. 
                    if (id == 1)
                        Buffer.BlockCopy(data, 24, DBFileVersion, 0, DBFileVersion.Length);
                    if (id > DBFileSize)
                        DBFileSize = id;
                    Stats[(int)STAT.WRITE]++;

                    // Update any backup objects copying the contents of this pager.
                    if (Backup != null) Backup.Update(id, list.Data);

                    PAGERTRACE("STORE {0} page {1} hash({2,08:x})", PAGERID(this), id, pager_pagehash(list));
                    SysEx.IOTRACE("PGOUT {0:x} {1}", GetHashCode(), id);
                    PAGER_INCR(sqlite3_pager_writedb_count);
                }
                else
                    PAGERTRACE("NOSTORE {0} page {1}", PAGERID(this), id);
                pager_set_pagehash(list);
                list = list.Dirty;
            }

            return rc;
        }

        private RC openSubJournal()
        {
            var rc = RC.OK;
            if (!SubJournalFile.Opened)
            {
                if (JournalMode == IPager.JOURNALMODE.JMEMORY || SubjInMemory)
                    sqlite3MemJournalOpen(SubJournalFile); //SubJournalFile = new MemoryVFile();
                else
                    rc = pagerOpentemp(ref SubJournalFile, VFileSystem.OPEN.SUBJOURNAL);
            }
            return rc;
        }

        private static RC subjournalPage(PgHdr pg)
        {
            var rc = RC.OK;
            var pager = pg.Pager;
            if (pager.JournalMode != IPager.JOURNALMODE.OFF)
            {
                // Open the sub-journal, if it has not already been opened
                Debug.Assert(pager.UseJournal != 0);
                Debug.Assert(pager.JournalFile.Opened || pager.UseWal());
                Debug.Assert(pager.SubJournalFile.Opened || pager.SubRecords == 0);
                Debug.Assert(pager.UseWal() || pageInJournal(pg) || pg.ID > pager.DBOrigSize);
                rc = pager.openSubJournal();
                // If the sub-journal was opened successfully (or was already open), write the journal record into the file. 
                if (rc == RC.OK)
                {
                    var data = pg.Data;
                    long offset = pager.SubRecords * (4 + pager.PageSize);
                    byte[] pData2 = null;
                    if (CODEC2(pager, data, pg.ID, codec_ctx.ENCRYPT_READ_CTX, ref pData2)) return RC.NOMEM;
                    PAGERTRACE("STMT-JOURNAL {0} page {1}", PAGERID(pager), pg.ID);
                    rc = pager.SubJournalFile.Write4(offset, pg.ID);
                    if (rc == RC.OK)
                        rc = pager.SubJournalFile.Write(pData2, pager.PageSize, offset + 4);
                }
            }
            if (rc == RC.OK)
            {
                pager.SubRecords++;
                Debug.Assert(pager.Savepoints.Length > 0);
                rc = pager.addToSavepointBitvecs(pg.ID);
            }
            return rc;
        }

        private static RC pagerStress(object p, IPage pg)
        {
            var pager = (Pager)p;
            Debug.Assert(pg.Pager == pager);
            Debug.Assert((pg.Flags & PgHdr.PGHDR.DIRTY) != 0);

            // The doNotSyncSpill flag is set during times when doing a sync of journal (and adding a new header) is not allowed. This occurs
            // during calls to sqlite3PagerWrite() while trying to journal multiple pages belonging to the same sector.
            //
            // The doNotSpill flag inhibits all cache spilling regardless of whether or not a sync is required.  This is set during a rollback.
            //
            // Spilling is also prohibited when in an error state since that could lead to database corruption.   In the current implementaton it 
            // is impossible for sqlite3PcacheFetch() to be called with createFlag==1 while in the error state, hence it is impossible for this routine to
            // be called in the error state.  Nevertheless, we include a NEVER() test for the error state as a safeguard against future changes.

            if (SysEx.NEVER(pager.ErrorCode != 0)) return RC.OK;
            if (pager.DoNotSpill) return RC.OK;
            if (pager.DoNotSyncSpill && (pg.Flags & PgHdr.PGHDR.NEED_SYNC) != 0) return RC.OK;

            pg.Dirty = null;
            var rc = RC.OK;
            if (pager.UseWal())
            {
                // Write a single frame for this page to the log.
                if (subjRequiresPage(pg))
                    rc = subjournalPage(pg);
                if (rc == RC.OK)
                    rc = pager.pagerWalFrames(pg, 0, 0);
            }
            else
            {
                // Sync the journal file if required. 
                if ((pg.Flags & PgHdr.PGHDR.NEED_SYNC) != 0 || pager.State == PAGER.WRITER_CACHEMOD)
                    rc = pager.syncJournal(true);

                // If the page number of this page is larger than the current size of the database image, it may need to be written to the sub-journal.
                // This is because the call to pager_write_pagelist() below will not actually write data to the file in this case.
                //
                // Consider the following sequence of events:
                //
                //   BEGIN;
                //     <journal page X>
                //     <modify page X>
                //     SAVEPOINT sp;
                //       <shrink database file to Y pages>
                //       pagerStress(page X)
                //     ROLLBACK TO sp;
                //
                // If (X>Y), then when pagerStress is called page X will not be written out to the database file, but will be dropped from the cache. Then,
                // following the "ROLLBACK TO sp" statement, reading page X will read data from the database file. This will be the copy of page X as it
                // was when the transaction started, not as it was when "SAVEPOINT sp" was executed.
                //
                // The solution is to write the current data for page X into the sub-journal file now (if it is not already there), so that it will
                // be restored to its current value when the "ROLLBACK TO sp" is executed.
                if (SysEx.NEVER(rc == RC.OK && pg.ID > pager.DBSize && subjRequiresPage(pg)))
                    rc = subjournalPage(pg);

                // Write the contents of the page out to the database file.
                if (rc == RC.OK)
                {
                    Debug.Assert((pg.Flags & PgHdr.PGHDR.NEED_SYNC) == 0);
                    rc = pager.pager_write_pagelist(pg);
                }
            }

            // Mark the page as clean.
            if (rc == RC.OK)
            {
                PAGERTRACE("STRESS {0} page {1}", PAGERID(pager), pg.ID);
                PCache.MakeClean(pg);
            }

            return pager.pager_error(rc);
        }

        // was:sqlite3PagerOpen
        public static RC Open(VFileSystem vfs, out Pager pagerOut, string filename, int extraBytes, IPager.PAGEROPEN flags, VFileSystem.OPEN vfsFlags, Action<PgHdr> reinit, Func<object> memPageBuilder)
        {
            // Figure out how much space is required for each journal file-handle (there are two of them, the main journal and the sub-journal). This
            // is the maximum space required for an in-memory journal file handle and a regular journal file-handle. Note that a "regular journal-handle"
            // may be a wrapper capable of caching the first portion of the journal file in memory to implement the atomic-write optimization (see 
            // source file journal.c).
            int journalFileSize = SysEx.ROUND8(sqlite3JournalSize(vfs) > MemoryVFile.sqlite3MemJournalSize() ? sqlite3JournalSize(vfs) : MemoryVFile.sqlite3MemJournalSize()); // Bytes to allocate for each journal fd

            // Set the output variable to NULL in case an error occurs.
            pagerOut = null;

            bool memoryDB = false;      // True if this is an in-memory file
            string pathname = null;     // Full path to database file
            string uri = null;          // URI args to copy
#if !OMIT_MEMORYDB
            if ((flags & IPager.PAGEROPEN.MEMORY) != 0)
            {
                memoryDB = true;
                if (!string.IsNullOrEmpty(filename))
                {
                    pathname = filename;
                    filename = null;
                }
            }
#endif

            // Compute and store the full pathname in an allocated buffer pointed to by zPathname, length nPathname. Or, if this is a temporary file,
            // leave both nPathname and zPathname set to 0.
            var rc = RC.OK;
            if (!string.IsNullOrEmpty(filename))
            {
                rc = vfs.FullPathname(filename, out pathname);
                var z = uri = filename;
                Debug.Assert(uri.Length >= 0);
                if (rc == RC.OK && pathname.Length + 8 > vfs.MaxPathname)
                {
                    // This branch is taken when the journal path required by the database being opened will be more than pVfs->mxPathname
                    // bytes in length. This means the database cannot be opened, as it will not be possible to open the journal file or even
                    // check for a hot-journal before reading.
                    rc = SysEx.CANTOPEN_BKPT();
                }
                if (rc != RC.OK)
                    return rc;
            }

            // Allocate memory for the Pager structure, PCache object, the three file descriptors, the database file name and the journal file name.
            var pager = new Pager(memPageBuilder);
            pager.PCache = new PCache();
            pager.File = new MemoryVFile();
            pager.SubJournalFile = new MemoryVFile();
            pager.JournalFile = new MemoryVFile();

            // Fill in the Pager.zFilename and Pager.zJournal buffers, if required.
            if (pathname != null)
            {
                Debug.Assert(pathname.Length > 0);
                pager.Filename = pathname;
                if (string.IsNullOrEmpty(uri)) pager.Filename += uri;
                pager.Journal = pager.Filename + "-journal";
#if !OMIT_WAL
                pager.WalName = pager.Filename + "-wal";
#endif
            }
            else
                pager.Filename = string.Empty;
            pager.Vfs = vfs;
            pager.VfsFlags = vfsFlags;

            // Open the pager file.
            var tempFile = false; // True for temp files (incl. in-memory files)
            var readOnly = false; // True if this is a read-only file
            uint sizePage = DEFAULT_PAGE_SIZE;  // Default page size
            if (!string.IsNullOrEmpty(filename))
            {
                VFileSystem.OPEN fout = 0; // VFS flags returned by xOpen()
                rc = vfs.Open(filename, pager.File, vfsFlags, out fout);
                Debug.Assert(memoryDB);
                readOnly = ((fout & VFileSystem.OPEN.READONLY) != 0);

                // If the file was successfully opened for read/write access, choose a default page size in case we have to create the
                // database file. The default page size is the maximum of:
                //
                //    + SQLITE_DEFAULT_PAGE_SIZE,
                //    + The value returned by sqlite3OsSectorSize()
                //    + The largest page size that can be written atomically.
                if (rc == RC.OK && !readOnly)
                {
                    pager.setSectorSize();
                    Debug.Assert(DEFAULT_PAGE_SIZE <= MAX_DEFAULT_PAGE_SIZE);
                    if (sizePage < pager.SectorSize)
                        sizePage = (pager.SectorSize > MAX_DEFAULT_PAGE_SIZE ? MAX_DEFAULT_PAGE_SIZE : (uint)pager.SectorSize);
#if ENABLE_ATOMIC_WRITE
                    Debug.Assert((int)VFile.IOCAP.ATOMIC512 == (512 >> 8));
                    Debug.Assert((int)VFile.IOCAP.ATOMIC64K == (65536 >> 8));
                    Debug.Assert(MAX_DEFAULT_PAGE_SIZE <= 65536);
                    var dc = (uint)pager.File.get_DeviceCharacteristics();
                    for (var ii = sizePage; ii <= MAX_DEFAULT_PAGE_SIZE; ii = ii * 2)
                        if ((dc & ((uint)VFile.IOCAP.ATOMIC | (ii >> 8))) != 0)
                            sizePage = ii;
#endif
                }
            }
            else
            {
                // If a temporary file is requested, it is not opened immediately. In this case we accept the default page size and delay actually
                // opening the file until the first call to OsWrite().
                //
                // This branch is also run for an in-memory database. An in-memory database is the same as a temp-file that is never written out to
                // disk and uses an in-memory rollback journal.
                tempFile = true;
                pager.State = PAGER.READER;
                pager.Lock = VFile.LOCK.EXCLUSIVE;
                readOnly = (vfsFlags & VFileSystem.OPEN.READONLY) != 0;
            }

            // The following call to PagerSetPagesize() serves to set the value of Pager.pageSize and to allocate the Pager.pTmpSpace buffer.
            if (rc == RC.OK)
            {
                Debug.Assert(!pager.MemoryDB);
                rc = pager.SetPageSize(ref sizePage, -1);
            }

            // If an error occurred in either of the blocks above, free the Pager structure and close the file.
            if (rc != RC.OK)
            {
                Debug.Assert(pager.TmpSpace == null);
                pager.File.Close();
                return rc;
            }
            // Initialize the PCache object.
            Debug.Assert(extraBytes < 1000);
            extraBytes = SysEx.ROUND8(extraBytes);
            PCache.Open((int)sizePage, extraBytes, !memoryDB, (!memoryDB ? (Func<object, IPage, RC>)pagerStress : null), pager, pager.PCache);

            PAGERTRACE("OPEN {0} {1}", FILEHANDLEID(pager.File), pager.Filename);
            SysEx.IOTRACE("OPEN {0:x} {1}", pager.GetHashCode(), pager.Filename);

            bool useJournal = (flags & IPager.PAGEROPEN.OMIT_JOURNAL) == 0; // False to omit journal
            pager.UseJournal = useJournal;
            pager.MaxPid = MAX_PAGE_COUNT;
            pager.TempFile = tempFile;
            //Debug.Assert(tempFile == IPager.LOCKINGMODE.NORMAL || tempFile == IPager.LOCKINGMODE.EXCLUSIVE);
            //Debug.Assert(IPager.LOCKINGMODE.EXCLUSIVE == 1);
            pager.ExclusiveMode = tempFile;
            pager.ChangeCountDone = tempFile;
            pager.MemoryDB = memoryDB;
            pager.ReadOnly = readOnly;
            Debug.Assert(useJournal || tempFile);
            pager.NoSync = tempFile;
            if (pager.NoSync)
            {
                Debug.Assert(!pager.FullSync);
                Debug.Assert(pager.SyncFlags == 0);
                Debug.Assert(pager.WalSyncFlags == 0);
                Debug.Assert(pager.CheckpointSyncFlags == 0);
            }
            else
            {
                pager.FullSync = true;
                pager.SyncFlags = VFile.SYNC.NORMAL;
                pager.WalSyncFlags = VFile.SYNC.NORMAL | VFile.SYNC.WAL_TRANSACTIONS;
                pager.CheckpointSyncFlags = VFile.SYNC.NORMAL;
            }
            pager.ExtraBytes = (ushort)extraBytes;
            pager.JournalSizeLimit = DEFAULT_JOURNAL_SIZE_LIMIT;
            Debug.Assert(pager.File.Opened || tempFile);
            pager.setSectorSize();
            if (!useJournal)
                pager.JournalMode = IPager.JOURNALMODE.OFF;
            else if (memoryDB)
                pager.JournalMode = IPager.JOURNALMODE.JMEMORY;
            pager.Reiniter = reinit;

            pagerOut = pager;
            return RC.OK;
        }

        private RC hasHotJournal(out bool existsOut)
        {
            Debug.Assert(UseJournal);
            Debug.Assert(File.Opened);
            Debug.Assert(State == PAGER.OPEN);
            var journalOpened = JournalFile.Opened;
            Debug.Assert(journalOpened || (JournalFile.get_DeviceCharacteristics() & VFile.IOCAP.UNDELETABLE_WHEN_OPEN) != 0);

            existsOut = false;
            var vfs = Vfs;
            var rc = RC.OK;
            var exists = 1; // True if a journal file is present
            if (!journalOpened)
                rc = vfs.Access(Journal, VFileSystem.ACCESS.EXISTS, out exists);
            if (rc == RC.OK && exists != 0)
            {
                // Race condition here:  Another process might have been holding the the RESERVED lock and have a journal open at the sqlite3OsAccess() 
                // call above, but then delete the journal and drop the lock before we get to the following sqlite3OsCheckReservedLock() call.  If that
                // is the case, this routine might think there is a hot journal when in fact there is none.  This results in a false-positive which will
                // be dealt with by the playback routine.  Ticket #3883.
                var locked = 0; // True if some process holds a RESERVED lock
                rc = File.CheckReservedLock(ref locked);
                if (rc == RC.OK && locked == 0)
                {
                    // Check the size of the database file. If it consists of 0 pages, then delete the journal file. See the header comment above for 
                    // the reasoning here.  Delete the obsolete journal file under a RESERVED lock to avoid race conditions and to avoid violating [H33020].
                    Pid pages = 0; // Number of pages in database file
                    rc = pagerPagecount(out pages);
                    if (rc == RC.OK)
                        if (pages == 0)
                        {
                            SysEx.BeginBenignAlloc();
                            if (pagerLockDb(VFile.LOCK.RESERVED) == RC.OK)
                            {
                                vfs.Delete(Journal, false);
                                if (!ExclusiveMode) pagerUnlockDb(VFile.LOCK.SHARED);
                            }
                            SysEx.EndBenignAlloc();
                        }
                        else
                        {
                            // The journal file exists and no other connection has a reserved or greater lock on the database file. Now check that there is
                            // at least one non-zero bytes at the start of the journal file. If there is, then we consider this journal to be hot. If not, 
                            // it can be ignored.
                            if (journalOpened)
                            {
                                var f = VFileSystem.OPEN.READONLY | VFileSystem.OPEN.MAIN_JOURNAL;
                                rc = vfs.Open(Journal, JournalFile, f, out f);
                            }
                            if (rc == RC.OK)
                            {
                                var first = new byte[1];
                                rc = JournalFile.Read(first, 1, 0);
                                if (rc == RC.IOERR_SHORT_READ)
                                    rc = RC.OK;
                                if (!journalOpened)
                                    JournalFile.Close();
                                existsOut = (first[0] != 0);
                            }
                            else if (rc == RC.CANTOPEN)
                            {
                                // If we cannot open the rollback journal file in order to see if its has a zero header, that might be due to an I/O error, or
                                // it might be due to the race condition described above and in ticket #3883.  Either way, assume that the journal is hot.
                                // This might be a false positive.  But if it is, then the automatic journal playback and recovery mechanism will deal
                                // with it under an EXCLUSIVE lock where we do not need to worry so much with race conditions.
                                existsOut = true;
                                rc = RC.OK;
                            }
                        }
                }
            }

            return rc;
        }

        public RC SharedLock()
        {
            // This routine is only called from b-tree and only when there are no outstanding pages. This implies that the pager state should either
            // be OPEN or READER. READER is only possible if the pager is or was in exclusive access mode.
            Debug.Assert(PCache.get_Refs() == 0);
            Debug.Assert(assert_pager_state());
            Debug.Assert(State == PAGER.OPEN || State == PAGER.READER);
            if (SysEx.NEVER(MemoryDB && ErrorCode != 0)) return ErrorCode;

            var rc = RC.OK;
            if (!UseWal() && State == PAGER.OPEN)
            {
                Debug.Assert(MemoryDB);

                rc = pager_wait_on_lock(VFile.LOCK.SHARED);
                if (rc != RC.OK)
                {
                    Debug.Assert(Lock == VFile.LOCK.NO || Lock == VFile.LOCK.UNKNOWN);
                    goto failed;
                }

                // If a journal file exists, and there is no RESERVED lock on the database file, then it either needs to be played back or deleted.
                var hotJournal = true; // True if there exists a hot journal-file
                if (Lock <= VFile.LOCK.SHARED)
                    rc = hasHotJournal(out hotJournal);
                if (rc != RC.OK)
                    goto failed;
                if (hotJournal)
                {
                    if (ReadOnly)
                    {
                        rc = RC.READONLY;
                        goto failed;
                    }

                    // Get an EXCLUSIVE lock on the database file. At this point it is important that a RESERVED lock is not obtained on the way to the
                    // EXCLUSIVE lock. If it were, another process might open the database file, detect the RESERVED lock, and conclude that the
                    // database is safe to read while this process is still rolling the hot-journal back.
                    // 
                    // Because the intermediate RESERVED lock is not requested, any other process attempting to access the database file will get to 
                    // this point in the code and fail to obtain its own EXCLUSIVE lock on the database file.
                    //
                    // Unless the pager is in locking_mode=exclusive mode, the lock is downgraded to SHARED_LOCK before this function returns.
                    rc = pagerLockDb(VFile.LOCK.EXCLUSIVE);
                    if (rc != RC.OK)
                        goto failed;

                    // If it is not already open and the file exists on disk, open the journal for read/write access. Write access is required because 
                    // in exclusive-access mode the file descriptor will be kept open and possibly used for a transaction later on. Also, write-access 
                    // is usually required to finalize the journal in journal_mode=persist mode (and also for journal_mode=truncate on some systems).
                    //
                    // If the journal does not exist, it usually means that some other connection managed to get in and roll it back before 
                    // this connection obtained the exclusive lock above. Or, it may mean that the pager was in the error-state when this
                    // function was called and the journal file does not exist.
                    if (!JournalFile.Opened)
                    {
                        var vfs = Vfs;
                        int exists; // True if journal file exists
                        rc = vfs.Access(Journal, VFileSystem.ACCESS.EXISTS, out exists);
                        if (rc == RC.OK && exists != 0)
                        {
                            Debug.Assert(!TempFile);
                            VFileSystem.OPEN fout = 0;
                            rc = vfs.Open(Journal, JournalFile, VFileSystem.OPEN.READWRITE | VFileSystem.OPEN.MAIN_JOURNAL, out fout);
                            Debug.Assert(rc != RC.OK || JournalFile.Opened);
                            if (rc == RC.OK && (fout & VFileSystem.OPEN.READONLY) != 0)
                            {
                                rc = SysEx.CANTOPEN_BKPT();
                                JournalFile.Close();
                            }
                        }
                    }

                    // Playback and delete the journal.  Drop the database write lock and reacquire the read lock. Purge the cache before
                    // playing back the hot-journal so that we don't end up with an inconsistent cache.  Sync the hot journal before playing
                    // it back since the process that crashed and left the hot journal probably did not sync it and we are required to always sync
                    // the journal before playing it back.
                    if (JournalFile.Opened)
                    {
                        Debug.Assert(rc == RC.OK);
                        rc = pagerSyncHotJournal();
                        if (rc == RC.OK)
                        {
                            rc = pager_playback(true);
                            State = PAGER.OPEN;
                        }
                    }
                    else if (!ExclusiveMode)
                        pagerUnlockDb(VFileSystem.LOCK.SHARED);

                    if (rc != RC.OK)
                    {
                        // This branch is taken if an error occurs while trying to open or roll back a hot-journal while holding an EXCLUSIVE lock. The
                        // pager_unlock() routine will be called before returning to unlock the file. If the unlock attempt fails, then Pager.eLock must be
                        // set to UNKNOWN_LOCK (see the comment above the #define for UNKNOWN_LOCK above for an explanation). 
                        //
                        // In order to get pager_unlock() to do this, set Pager.eState to PAGER_ERROR now. This is not actually counted as a transition
                        // to ERROR state in the state diagram at the top of this file, since we know that the same call to pager_unlock() will very
                        // shortly transition the pager object to the OPEN state. Calling assert_pager_state() would fail now, as it should not be possible
                        // to be in ERROR state when there are zero outstanding page references.
                        pager_error(rc);
                        goto failed;
                    }

                    Debug.Assert(State == PAGER.OPEN);
                    Debug.Assert((Lock == VFile.LOCK.SHARED) || (ExclusiveMode && Lock > VFile.LOCK.SHARED));
                }

                if (!TempFile && (Backup != null || PCache.get_Pages() > 0))
                {
                    // The shared-lock has just been acquired on the database file and there are already pages in the cache (from a previous
                    // read or write transaction).  Check to see if the database has been modified.  If the database has changed, flush the cache.
                    //
                    // Database changes is detected by looking at 15 bytes beginning at offset 24 into the file.  The first 4 of these 16 bytes are
                    // a 32-bit counter that is incremented with each change.  The other bytes change randomly with each file change when
                    // a codec is in use.
                    // 
                    // There is a vanishingly small chance that a change will not be detected.  The chance of an undetected change is so small that
                    // it can be neglected.
                    Pid pages = 0;
                    var dbFileVersions = new byte[DBFileVersions.Length];

                    rc = pagerPagecount(out pages);
                    if (rc != RC.OK)
                        goto failed;

                    if (pages > 0)
                    {
                        SysEx.IOTRACE("CKVERS {0} {1}\n", this, dbFileVersions.Length);
                        rc = File.Read(dbFileVersions, dbFileVersions.Length, 24);
                        if (rc != RC.OK)
                            goto failed;
                    }
                    else
                        Array.Clear(dbFileVersions, 0, dbFileVersions.Length);

                    if (Enumerable.SequenceEqual(DBFileVersions, dbFileVersions) != 0)
                        pager_reset();
                }

                // If there is a WAL file in the file-system, open this database in WAL mode. Otherwise, the following function call is a no-op.
                rc = pagerOpenWalIfPresent();
#if !OMIT_WAL
                Debug.Assert(pager.Wal == null || rc == RC.OK);
#endif
            }

            if (UseWal())
            {
                Debug.Assert(rc == RC.OK);
                rc = pagerBeginReadTransaction();
            }

            if (State == PAGER.OPEN && rc == RC.OK)
                rc = pagerPagecount(out DBSize);

        failed:
            if (rc != RC.OK)
            {
                Debug.Assert(MemoryDB);
                pager_unlock();
                Debug.Assert(State == PAGER.OPEN);
            }
            else
                State = PAGER.READER;
            return rc;
        }

        private void UnlockIfUnused()
        {
            if (PCache.get_Refs() == 0)
                pagerUnlockAndRollback();
        }

        public RC Acquire(Pid pid, ref PgHdr page, bool noContent = false)
        {
            Debug.Assert(State >= PAGER.READER);
            Debug.Assert(assert_pager_state());
            if (pid == 0)
                return SysEx.CORRUPT_BKPT();
            // If the pager is in the error state, return an error immediately.  Otherwise, request the page from the PCache layer.
            var rc = (ErrorCode != RC.OK ? ErrorCode : PCache.Fetch(pid, 1, out page));
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

        // was:sqlite3PagerBegin
        public RC Begin(bool exFlag, bool subjInMemory)
        {
            if (_errorCode != 0)
                return _errorCode;
            Debug.Assert(_state >= PAGER.READER && _state < PAGER.ERROR);
            var rc = RC.OK;
            if (Check.ALWAYS(_state == PAGER.READER))
            {
                Debug.Assert(_inJournal == null);
                if (pagerUseWal())
                {
                    // If the pager is configured to use locking_mode=exclusive, and an exclusive lock on the database is not already held, obtain it now.
                    if (_exclusiveMode && _wal.ExclusiveMode(-1))
                    {
                        rc = pagerLockDb(VFSLOCK.EXCLUSIVE);
                        if (rc != RC.OK)
                            return rc;
                        _wal.ExclusiveMode(1);
                    }
                    // Grab the write lock on the log file. If successful, upgrade to PAGER_RESERVED state. Otherwise, return an error code to the caller.
                    // The busy-handler is not invoked if another connection already holds the write-lock. If possible, the upper layer will call it.
                    rc = _wal.BeginWriteTransaction();
                }
                else
                {
                    // Obtain a RESERVED lock on the database file. If the exFlag parameter is true, then immediately upgrade this to an EXCLUSIVE lock. The
                    // busy-handler callback can be used when upgrading to the EXCLUSIVE lock, but not when obtaining the RESERVED lock.
                    rc = pagerLockDb(VFSLOCK.RESERVED);
                    if (rc == RC.OK && exFlag)
                        rc = pager_wait_on_lock(VFSLOCK.EXCLUSIVE);
                }
                if (rc == RC.OK)
                {
                    // Change to WRITER_LOCKED state.
                    // WAL mode sets Pager.eState to PAGER_WRITER_LOCKED or CACHEMOD when it has an open transaction, but never to DBMOD or FINISHED.
                    // This is because in those states the code to roll back savepoint transactions may copy data from the sub-journal into the database 
                    // file as well as into the page cache. Which would be incorrect in WAL mode.
                    _state = PAGER.WRITER_LOCKED;
                    _dbHintSize = _dbSize;
                    _dbFileSize = _dbSize;
                    _dbOrigSize = _dbSize;
                    _journalOff = 0;
                }
                Debug.Assert(rc == RC.OK || _state == PAGER.READER);
                Debug.Assert(rc != RC.OK || _state == PAGER.WRITER_LOCKED);
                Debug.Assert(assert_pager_state());
            }
            PAGERTRACE("TRANSACTION {0}", PAGERID(this));
            return rc;
        }


        private static RC pager_write(PgHdr pPg)
        {
            var pData = pPg.Data;
            var pPager = pPg.Pager;
            var rc = RC.OK;
            // This routine is not called unless a write-transaction has already been started. The journal file may or may not be open at this point.
            // It is never called in the ERROR state.
            Debug.Assert(pPager.eState == PAGER.WRITER_LOCKED || pPager.eState == PAGER.WRITER_CACHEMOD || pPager.eState == PAGER.WRITER_DBMOD);
            Debug.Assert(pPager.assert_pager_state());
            // If an error has been previously detected, report the same error again. This should not happen, but the check provides robustness. 
            if (Check.NEVER(pPager.errCode) != RC.OK)
                return pPager.errCode;
            // Higher-level routines never call this function if database is not writable.  But check anyway, just for robustness.
            if (Check.NEVER(pPager.readOnly))
                return RC.PERM;
#if SQLITE_CHECK_PAGES
CHECK_PAGE(pPg);
#endif
            // The journal file needs to be opened. Higher level routines have already obtained the necessary locks to begin the write-transaction, but the
            // rollback journal might not yet be open. Open it now if this is the case.
            //
            // This is done before calling sqlite3PcacheMakeDirty() on the page. Otherwise, if it were done after calling sqlite3PcacheMakeDirty(), then
            // an error might occur and the pager would end up in WRITER_LOCKED state with pages marked as dirty in the cache.
            if (pPager.eState == PAGER.WRITER_LOCKED)
            {
                rc = pPager.pager_open_journal();
                if (rc != RC.OK)
                    return rc;
            }
            Debug.Assert(pPager.eState >= PAGER.WRITER_CACHEMOD);
            Debug.Assert(pPager.assert_pager_state());
            // Mark the page as dirty.  If the page has already been written to the journal then we can return right away.
            PCache.MakePageDirty(pPg);
            if (pageInJournal(pPg) && !subjRequiresPage(pPg))
                Debug.Assert(!pPager.pagerUseWal());
            else
            {
                // The transaction journal now exists and we have a RESERVED or an EXCLUSIVE lock on the main database file.  Write the current page to
                // the transaction journal if it is not there already.
                if (!pageInJournal(pPg) && !pPager.pagerUseWal())
                {
                    Debug.Assert(!pPager.pagerUseWal());
                    if (pPg.ID <= pPager.dbOrigSize && pPager.jfd.IsOpen)
                    {
                        var iOff = pPager.journalOff;
                        // We should never write to the journal file the page that contains the database locks.  The following Debug.Assert verifies that we do not.
                        Debug.Assert(pPg.ID != ((VirtualFile.PENDING_BYTE / (pPager.pageSize)) + 1));
                        Debug.Assert(pPager.journalHdr <= pPager.journalOff);
                        byte[] pData2 = null;
                        if (CODEC2(pPager, pData, pPg.ID, codec_ctx.ENCRYPT_READ_CTX, ref pData2))
                            return RC.NOMEM;
                        var cksum = pPager.pager_cksum(pData2);
                        // Even if an IO or diskfull error occurred while journalling the page in the block above, set the need-sync flag for the page.
                        // Otherwise, when the transaction is rolled back, the logic in playback_one_page() will think that the page needs to be restored
                        // in the database file. And if an IO error occurs while doing so, then corruption may follow.
                        pPg.Flags |= PgHdr.PGHDR.NEED_SYNC;
                        rc = pPager.jfd.WriteByte(iOff, pPg.ID);
                        if (rc != RC.OK)
                            return rc;
                        rc = pPager.jfd.Write(pData2, pPager.pageSize, iOff + 4);
                        if (rc != RC.OK)
                            return rc;
                        rc = pPager.jfd.WriteByte(iOff + pPager.pageSize + 4, cksum);
                        if (rc != RC.OK)
                            return rc;
                        SysEx.IOTRACE("JOUT {0:x} {1} {2,11} {3}", pPager.GetHashCode(), pPg.ID, pPager.journalOff, pPager.pageSize);
                        PAGERTRACE("JOURNAL {0} page {1} needSync={2} hash({3,08:x})", PAGERID(pPager), pPg.ID, (pPg.Flags & PgHdr.PGHDR.NEED_SYNC) != 0 ? 1 : 0, pager_pagehash(pPg));
                        pPager.journalOff += 8 + pPager.pageSize;
                        pPager.nRec++;
                        Debug.Assert(pPager.pInJournal != null);
                        rc = pPager.pInJournal.Set(pPg.ID);
                        Debug.Assert(rc == RC.OK || rc == RC.NOMEM);
                        rc |= pPager.addToSavepointBitvecs(pPg.ID);
                        if (rc != RC.OK)
                        {
                            Debug.Assert(rc == RC.NOMEM);
                            return rc;
                        }
                    }
                    else
                    {
                        if (pPager.eState != PAGER.WRITER_DBMOD)
                            pPg.Flags |= PgHdr.PGHDR.NEED_SYNC;
                        PAGERTRACE("APPEND {0} page {1} needSync={2}", PAGERID(pPager), pPg.ID, (pPg.Flags & PgHdr.PGHDR.NEED_SYNC) != 0 ? 1 : 0);
                    }
                }
                // If the statement journal is open and the page is not in it, then write the current page to the statement journal.  Note that
                // the statement journal format differs from the standard journal format in that it omits the checksums and the header.
                if (subjRequiresPage(pPg))
                    rc = subjournalPage(pPg);
            }
            // Update the database size and return.
            if (pPager.dbSize < pPg.ID)
                pPager.dbSize = pPg.ID;
            return rc;
        }

        // was:sqlite3PagerWrite
        public static RC Write(DbPage pDbPage)
        {
            var rc = RC.OK;
            var pPg = pDbPage;
            var pPager = pPg.Pager;
            var nPagePerSector = (uint)(pPager._sectorSize / pPager._pageSize);
            Debug.Assert(pPager._state >= PAGER.WRITER_LOCKED);
            Debug.Assert(pPager._state != PAGER.ERROR);
            Debug.Assert(pPager.assert_pager_state());
            if (nPagePerSector > 1)
            {
                Pgno nPageCount = 0;     // Total number of pages in database file
                Pgno pg1;                // First page of the sector pPg is located on.
                Pgno nPage = 0;          // Number of pages starting at pg1 to journal
                bool needSync = false;   // True if any page has PGHDR_NEED_SYNC

                // Set the doNotSyncSpill flag to 1. This is because we cannot allow a journal header to be written between the pages journaled by
                // this function.
                Debug.Assert(
#if SQLITE_OMIT_MEMORYDB
0==MEMDB
#else
0 == pPager._memoryDB
#endif
);
                Debug.Assert(pPager._doNotSyncSpill == 0);
                pPager._doNotSyncSpill++;
                // This trick assumes that both the page-size and sector-size are an integer power of 2. It sets variable pg1 to the identifier
                // of the first page of the sector pPg is located on.
                pg1 = (Pgno)((pPg.ID - 1) & ~(nPagePerSector - 1)) + 1;
                nPageCount = pPager._dbSize;
                if (pPg.ID > nPageCount)
                    nPage = (pPg.ID - pg1) + 1;
                else if ((pg1 + nPagePerSector - 1) > nPageCount)
                    nPage = nPageCount + 1 - pg1;
                else
                    nPage = nPagePerSector;
                Debug.Assert(nPage > 0);
                Debug.Assert(pg1 <= pPg.ID);
                Debug.Assert((pg1 + nPage) > pPg.ID);
                for (var ii = 0; ii < nPage && rc == RC.OK; ii++)
                {
                    var pg = (Pgno)(pg1 + ii);
                    var pPage = new PgHdr();
                    if (pg == pPg.ID || !pPager._inJournal.Get(pg))
                    {
                        if (pg != ((VirtualFile.PENDING_BYTE / (pPager._pageSize)) + 1))
                        {
                            rc = pPager.Get(pg, ref pPage);
                            if (rc == RC.OK)
                            {
                                rc = pager_write(pPage);
                                if ((pPage.Flags & PgHdr.PGHDR.NEED_SYNC) != 0)
                                    needSync = true;
                                Unref(pPage);
                            }
                        }
                    }
                    else if ((pPage = pPager.pager_lookup(pg)) != null)
                    {
                        if ((pPage.Flags & PgHdr.PGHDR.NEED_SYNC) != 0)
                            needSync = true;
                        Unref(pPage);
                    }
                }
                // If the PGHDR_NEED_SYNC flag is set for any of the nPage pages starting at pg1, then it needs to be set for all of them. Because
                // writing to any of these nPage pages may damage the others, the journal file must contain sync()ed copies of all of them
                // before any of them can be written out to the database file.
                if (rc == RC.OK && needSync)
                {
                    Debug.Assert(
#if SQLITE_OMIT_MEMORYDB
0==MEMDB
#else
0 == pPager._memoryDB
#endif
);
                    for (var ii = 0; ii < nPage; ii++)
                    {
                        var pPage = pPager.pager_lookup((Pgno)(pg1 + ii));
                        if (pPage != null)
                        {
                            pPage.Flags |= PgHdr.PGHDR.NEED_SYNC;
                            Unref(pPage);
                        }
                    }
                }
                Debug.Assert(pPager._doNotSyncSpill == 1);
                pPager._doNotSyncSpill--;
            }
            else
                rc = pager_write(pDbPage);
            return rc;
        }

#if DEBUG
        // was:sqlite3PagerIswriteable
        public static bool IsPageWriteable(DbPage pPg) { return true; }
#endif

        // was:sqlite3PagerDontWrite
        public static void DontWrite(PgHdr pPg)
        {
            var pPager = pPg.Pager;
            if ((pPg.Flags & PgHdr.PGHDR.DIRTY) != 0 && pPager.nSavepoint == 0)
            {
                PAGERTRACE("DONT_WRITE page {0} of {1}", pPg.ID, PAGERID(pPager));
                SysEx.IOTRACE("CLEAN {0:x} {1}", pPager.GetHashCode(), pPg.ID);
                pPg.Flags |= PgHdr.PGHDR.DONT_WRITE;
                pager_set_pagehash(pPg);
            }
        }

        private RC pager_incr_changecounter(bool isDirectMode)
        {
            var rc = RC.OK;
            Debug.Assert(this.eState == PAGER.WRITER_CACHEMOD || this.eState == PAGER.WRITER_DBMOD);
            Debug.Assert(assert_pager_state());
            // Declare and initialize constant integer 'isDirect'. If the atomic-write optimization is enabled in this build, then isDirect
            // is initialized to the value passed as the isDirectMode parameter to this function. Otherwise, it is always set to zero.
            // The idea is that if the atomic-write optimization is not enabled at compile time, the compiler can omit the tests of
            // 'isDirect' below, as well as the block enclosed in the "if( isDirect )" condition.
#if !SQLITE_ENABLE_ATOMIC_WRITE
            var DIRECT_MODE = false;
            Debug.Assert(!isDirectMode);
            SysEx.UNUSED_PARAMETER(isDirectMode);
#else
            var DIRECT_MODE = isDirectMode;
#endif
            if (!this.changeCountDone && this.dbSize > 0)
            {
                PgHdr pPgHdr = null; // Reference to page 1
                Debug.Assert(!this.tempFile && this.fd.IsOpen);
                // Open page 1 of the file for writing.
                rc = Get(1, ref pPgHdr);
                Debug.Assert(pPgHdr == null || rc == RC.OK);
                // If page one was fetched successfully, and this function is not operating in direct-mode, make page 1 writable.  When not in 
                // direct mode, page 1 is always held in cache and hence the PagerGet() above is always successful - hence the ALWAYS on rc==SQLITE.OK.
                if (!DIRECT_MODE && Check.ALWAYS(rc == RC.OK))
                    rc = Write(pPgHdr);
                if (rc == RC.OK)
                {
                    // Actually do the update of the change counter
                    pager_write_changecounter(pPgHdr);
                    // If running in direct mode, write the contents of page 1 to the file.
                    if (DIRECT_MODE)
                    {
                        byte[] zBuf = null;
                        Debug.Assert(this.dbFileSize > 0);
                        if (CODEC2(this, pPgHdr.Data, 1, codec_ctx.ENCRYPT_WRITE_CTX, ref zBuf))
                            return rc = RC.NOMEM;
                        if (rc == RC.OK)
                            rc = this.fd.Write(zBuf, this.pageSize, 0);
                        if (rc == RC.OK)
                            this.changeCountDone = true;
                    }
                    else
                        this.changeCountDone = true;
                }
                // Release the page reference.
                Unref(pPgHdr);
            }
            return rc;
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

        #endregion

        #region Commit



        #endregion
        #region X



        #endregion
    }
}
