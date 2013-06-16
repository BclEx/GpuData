using Pid = System.UInt32;
using IPage = Core.PgHdr;
using System;
using System.Diagnostics;
using System.Text;
using Core.IO;

namespace Core
{
    public partial class Btree
    {
#if DEBUG
        static bool BtreeTrace = false;
        static void TRACE(string x, params object[] args) { if (BtreeTrace) Console.WriteLine("b:" + string.Format(x, args)); }
#else
        static void TRACE(string x, params object[] args) { }
#endif

        #region Struct

        static byte[] _magicHeader = Encoding.UTF8.GetBytes(FILE_HEADER);

#if !OMIT_SHARED_CACHE
        static BtShared _sharedCacheList = null;
        bool _sharedCacheEnabled = false;
        RC sqlite3_enable_shared_cache(bool enable)
        {
            _sharedCacheEnabled = enable;
            return RC.OK;
        }
#else
        static RC querySharedCacheTableLock(Btree p, Pid table, LOCK lock_) { return RC.OK; }
        static void clearAllSharedCacheTableLocks(Btree a) { }
        static void downgradeAllSharedCacheTableLocks(Btree a) { }
        static bool hasSharedCacheTableLock(Btree a, Pid b, int c, int d) { return true; }
        static bool hasReadConflicts(Btree a, Pid b) { return false; }
#endif

        #endregion

        #region Shared Code1
#if !OMIT_SHARED_CACHE

#if DEBUG
        static bool hasSharedCacheTableLock(Btree btree, Pid root, bool isIndex, LOCK lockType)
        {
            // If this database is not shareable, or if the client is reading and has the read-uncommitted flag set, then no lock is required. 
            // Return true immediately.
            if (!btree.Sharable || (lockType == LOCK.READ && (btree.Ctx.Flags & Context.FLAG.ReadUncommitted) != 0))
                return true;

            // If the client is reading  or writing an index and the schema is not loaded, then it is too difficult to actually check to see if
            // the correct locks are held.  So do not bother - just return true. This case does not come up very often anyhow.
            var schema = (Schema)btree.Bt.Schema;
            if (isIndex && (!schema || (schema->Flags & DB_SchemaLoaded) == 0))
                return true;

            // Figure out the root-page that the lock should be held on. For table b-trees, this is just the root page of the b-tree being read or
            // written. For index b-trees, it is the root page of the associated table.
            Pid table = 0;
            if (isIndex)
                for (var p = sqliteHashFirst(schema.IdxHash); p != null; p = sqliteHashNext(p))
                {
                    var idx = (Index)sqliteHashData(p);
                    if (idx.TID == (int)root)
                        table = idx.Table.TID;
                }
            else
                table = root;

            // Search for the required lock. Either a write-lock on root-page iTab, a write-lock on the schema table, or (if the client is reading) a
            // read-lock on iTab will suffice. Return 1 if any of these are found.
            for (var lock_ = btree.Bt.Lock; lock_ != null; lock_ = lock_.Next)
                if (lock_.Btree == btree &&
                    (lock_.Table == table || (lock_.Lock == LOCK.WRITE && lock_.Table == 1)) &&
                    lock_.Lock >= lockType)
                    return true;

            // Failed to find the required lock.
            return false;
        }

        static bool hasReadConflicts(Btree btree, Pid root)
        {
            for (var p = btree.Bt.Cursor; p != null; p = p.Next)
                if (p.IDRoot == root &&
                    p.Btree != btree &&
                    (p.Btree.Ctx.Flags & Context.FLAG.ReadUncommitted) == 0)
                    return true;
            return false;
        }
#endif

        static RC querySharedCacheTableLock(Btree p, Pid table, LOCK lockType)
        {
            Debug.Assert(sqlite3BtreeHoldsMutex(p));
            Debug.Assert(lockType == LOCK.READ || lockType == LOCK.WRITE);
            Debug.Assert(p.Ctx != null);
            Debug.Assert((p.Ctx.Flags & Context.FLAG.ReadUncommitted) == 0 || lockType == LOCK.WRITE || table == 1);

            // If requesting a write-lock, then the Btree must have an open write transaction on this file. And, obviously, for this to be so there
            // must be an open write transaction on the file itself.
            var bt = p.Bt;
            Debug.Assert(lockType == LOCK.READ || (p == bt.Writer && p.InTrans == TRANS.WRITE));
            Debug.Assert(lockType == LOCK.READ || bt.InTransaction == TRANS.WRITE);

            // This routine is a no-op if the shared-cache is not enabled
            if (!p.Sharable)
                return RC.OK;

            // If some other connection is holding an exclusive lock, the requested lock may not be obtained.
            if (bt.Writer != p && (bt.BtsFlags & BTS.EXCLUSIVE) != 0)
            {
                sqlite3ConnectionBlocked(p.Ctx, bt.Writer.Ctx);
                return RC.LOCKED_SHAREDCACHE;
            }

            for (var iter = bt.Lock; iter != null; iter = iter.Next)
            {
                // The condition (pIter->eLock!=eLock) in the following if(...) statement is a simplification of:
                //
                //   (eLock==WRITE_LOCK || pIter->eLock==WRITE_LOCK)
                //
                // since we know that if eLock==WRITE_LOCK, then no other connection may hold a WRITE_LOCK on any table in this file (since there can
                // only be a single writer).
                Debug.Assert(iter.Lock == LOCK.READ || iter.Lock == LOCK.WRITE);
                Debug.Assert(lockType == LOCK.READ || iter.Btree == p || iter.Lock == LOCK.READ);
                if (iter.Btree != p && iter.Table == table && iter.Lock != lockType)
                {
                    sqlite3ConnectionBlocked(p.Ctx, iter.Btree.Ctx);
                    if (lockType == LOCK.WRITE)
                    {
                        Debug.Assert(p == bt.Writer);
                        bt.BtsFlags |= BTS.PENDING;
                    }
                    return RC.LOCKED_SHAREDCACHE;
                }
            }
            return RC.OK;
        }

        static RC setSharedCacheTableLock(Btree p, Pid table, LOCK lock_)
        {
            Debug.Assert(sqlite3BtreeHoldsMutex(p));
            Debug.Assert(lock_ == LOCK.READ || lock_ == LOCK.WRITE);
            Debug.Assert(p.Ctx != null);

            // A connection with the read-uncommitted flag set will never try to obtain a read-lock using this function. The only read-lock obtained
            // by a connection in read-uncommitted mode is on the sqlite_master table, and that lock is obtained in BtreeBeginTrans().
            Debug.Assert((p.Ctx.Flags & Context.FLAG.ReadUncommitted) == 0 || lock_ == LOCK.WRITE);

            // This function should only be called on a sharable b-tree after it has been determined that no other b-tree holds a conflicting lock.
            var bt = p.Bt;

            Debug.Assert(p.Sharable);
            Debug.Assert(RC.OK == querySharedCacheTableLock(p, table, lock_));

            // First search the list for an existing lock on this table.
            BtLock newLock = null;
            for (var iter = bt.Lock; iter != null; iter = iter.Next)
                if (iter.Table == table && iter.Btree == p)
                {
                    newLock = iter;
                    break;
                }

            // If the above search did not find a BtLock struct associating Btree p with table iTable, allocate one and link it into the list.
            if (newLock == null)
            {
                newLock = new BtLock();
                newLock.Table = table;
                newLock.Btree = p;
                newLock.Next = bt.Lock;
                bt.Lock = newLock;
            }

            // Set the BtLock.eLock variable to the maximum of the current lock and the requested lock. This means if a write-lock was already held
            // and a read-lock requested, we don't incorrectly downgrade the lock.
            Debug.Assert(LOCK.WRITE > LOCK.READ);
            if (lock_ > newLock.Lock)
                newLock.Lock = lock_;

            return RC.OK;
        }

        static void clearAllSharedCacheTableLocks(Btree p)
        {
            var bt = p.Bt;
            var iter = bt.Lock;

            Debug.Assert(sqlite3BtreeHoldsMutex(p));
            Debug.Assert(p.Sharable || iter == null);
            Debug.Assert(p.InTrans > 0);

            while (iter != null)
            {
                BtLock lock_ = iter;
                Debug.Assert((bt.BtsFlags & BTS.EXCLUSIVE) == 0 || bt.Writer == lock_.Btree);
                Debug.Assert(lock_.Btree.InTrans >= lock_.Lock);
                if (lock_.Btree == p)
                {
                    iter = lock_.Next;
                    Debug.Assert(lock_.Table != 1 || lock_ == p.Lock);
                    if (lock_.Table != 1)
                        lock_ = null;
                }
                else
                    iter = lock_.Next;
            }

            Debug.Assert((bt.BtsFlags & BTS.PENDING) == 0 || bt.Writer != null);
            if (bt.Writer == p)
            {
                bt.Writer = null;
                bt.BtsFlags &= ~(BTS.EXCLUSIVE | BTS.PENDING);
            }
            else if (bt.Transactions == 2)
            {
                // This function is called when Btree p is concluding its transaction. If there currently exists a writer, and p is not
                // that writer, then the number of locks held by connections other than the writer must be about to drop to zero. In this case
                // set the BTS_PENDING flag to 0.
                //
                // If there is not currently a writer, then BTS_PENDING must be zero already. So this next line is harmless in that case.
                bt.BtsFlags &= ~BTS.PENDING;
            }
        }

        static void downgradeAllSharedCacheTableLocks(Btree p)
        {
            var bt = p.Bt;
            if (bt.Writer == p)
            {
                bt.Writer = null;
                bt.BtsFlags &= ~(BTS.EXCLUSIVE | BTS.PENDING);
                for (var lock_ = bt.Lock; lock_ != null; lock_ = lock_.Next)
                {
                    Debug.Assert(lock_.Lock == LOCK.READ || lock_.Btree == p);
                    lock_.Lock = LOCK.READ;
                }
            }
        }

#endif
        #endregion

        #region Name2

#if DEBUG
        static bool cursorHoldsMutex(BtCursor p)
        {
            return MutexEx.Held(p.Bt.Mutex);
        }
#endif

#if !OMIT_INCRBLOB
        static void invalidateOverflowCache(BtCursor cur)
        {
            Debug.Assert(cursorHoldsMutex(cur));
            cur.Overflows = null;
        }

        static void invalidateAllOverflowCache(BtShared bt)
        {
            Debug.Assert(MutexEx.Held(bt.Mutex));
            for (var p = bt.Cursor; p != null; p = p.Next)
                invalidateOverflowCache(p);
        }

        static void invalidateIncrblobCursors(Btree btree, long rowid, bool isClearTable)
        {
            var bt = btree.Bt;
            Debug.Assert(sqlite3BtreeHoldsMutex(btree));
            for (var p = bt.Cursor; p != null; p = p.Next)
                if (p.IsIncrblobHandle && (isClearTable || p.Info.Key == rowid))
                    p.State = CURSOR.INVALID;
        }
#else
        static void invalidateOverflowCache(BtCursor cur) { }
        static void invalidateAllOverflowCache(BtShared bt) { }
        static void invalidateIncrblobCursors(Btree x, long y, int z) { }
#endif

        #endregion

        #region Name3

        static RC btreeSetHasContent(BtShared bt, Pid id)
        {
            var rc = RC.OK;
            if (bt.HasContent == null)
            {
                Debug.Assert(id <= bt.Pages);
                bt.HasContent = new Bitvec(bt.Pages);
            }
            if (rc == RC.OK && id <= bt.HasContent.Length)
                rc = bt.HasContent.Set(id);
            return rc;
        }

        static bool btreeGetHasContent(BtShared bt, Pid id)
        {
            var p = bt.HasContent;
            return (p != null && (id > p.Length || p.Get(id)));
        }

        static void btreeClearHasContent(BtShared bt)
        {
            Bitvec.Destroy(ref bt.HasContent);
            bt.HasContent = null;
        }

        static void btreeReleaseAllCursorPages(BtCursor cur)
        {
            for (int i = 0; i <= cur.PageIdx; i++)
            {
                releasePage(cur.Pages[i]);
                cur.Pages[i] = null;
            }
            cur.PageIdx = -1;
        }

        static RC saveCursorPosition(BtCursor cur)
        {
            Debug.Assert(cur.State == CURSOR.VALID);
            Debug.Assert(cur.Key == null);
            Debug.Assert(cursorHoldsMutex(cur));

            var rc = sqlite3BtreeKeySize(cur, ref cur.Key);
            Debug.Assert(rc == RC.OK);  // KeySize() cannot fail

            // If this is an intKey table, then the above call to BtreeKeySize() stores the integer key in pCur.nKey. In this case this value is
            // all that is required. Otherwise, if pCur is not open on an intKey table, then malloc space for and store the pCur.nKey bytes of key data.
            if (cur.Pages[0].IntKey == 0)
            {
                var key = SysEx.Alloc((int)cur.KeyLength);
                rc = sqlite3BtreeKey(cur, 0, (uint)cur.KeyLength, key);
                if (rc == RC.OK)
                    cur.Key = key;
            }
            Debug.Assert(cur.Pages[0].IntKey == 0 || cur.Key == null);

            if (rc == RC.OK)
            {
                btreeReleaseAllCursorPages(cur);
                cur.State = CURSOR.REQUIRESEEK;
            }

            invalidateOverflowCache(cur);
            return rc;
        }

        static RC saveAllCursors(BtShared bt, Pid root, BtCursor except)
        {
            Debug.Assert(MutexEx.Held(bt.Mutex));
            Debug.Assert(except == null || except.Bt == bt);
            for (var p = bt.Cursor; p != null; p = p.Next)
            {
                if (p != except && (root == 0 || p.IDRoot == root) && p.State == CURSOR.VALID)
                {
                    var rc = saveCursorPosition(p);
                    if (rc != RC.OK)
                        return rc;
                }
            }
            return RC.OK;
        }

        static void sqlite3BtreeClearCursor(BtCursor cur)
        {
            Debug.Assert(cursorHoldsMutex(cur));
            SysEx.Free(ref cur.Key);
            cur.State = CURSOR.INVALID;
        }

        static RC btreeMoveto(BtCursor cur, byte[] key, long keyLength, int bias, ref int res)
        {
            UnpackedRecord idxKey; // Unpacked index key
            var space = new UnpackedRecord(); // Temp space for pIdxKey - to avoid a malloc
            if (key != null)
            {
                Debug.Assert(keyLength == (long)(int)keyLength);
                idxKey = sqlite3VdbeRecordUnpack(cur.KeyInfo, (int)keyLength, key, space, 16);
            }
            else
                idxKey = null;
            var rc = sqlite3BtreeMovetoUnpacked(cur, idxKey, keyLength, (bias != 0 ? 1 : 0), ref res);
            return rc;
        }

        static int btreeRestoreCursorPosition(BtCursor cur)
        {
            Debug.Assert(cursorHoldsMutex(cur));
            Debug.Assert(cur.State >= CURSOR.REQUIRESEEK);
            if (cur.State == CURSOR.FAULT)
                return cur.SkipNext;
            cur.State = CURSOR.INVALID;
            var rc = btreeMoveto(cur, cur.Key, cur.KeyLength, 0, ref cur.SkipNext);
            if (rc == RC.OK)
            {
                cur.Key = null;
                Debug.Assert(cur.State == CURSOR.VALID || cur.State == CURSOR.INVALID);
            }
            return rc;
        }

        static RC restoreCursorPosition(BtCursor cur)
        {
            return (cur.State >= CURSOR.REQUIRESEEK ? btreeRestoreCursorPosition(cur) : RC.OK);
        }

        static RC sqlite3BtreeCursorHasMoved(BtCursor cur, out bool hasMoved)
        {
            var rc = restoreCursorPosition(cur);
            if (rc != RC.OK)
            {
                hasMoved = true;
                return rc;
            }
            hasMoved = (cur.State != CURSOR.VALID || cur.SkipNext != 0);
            return RC.OK;
        }

        #endregion

        #region Parse Cell

#if !OMIT_AUTOVACUUM
        static Pid ptrmapPageno(BtShared bt, Pid id)
        {
            Debug.Assert(MutexEx.Held(bt.Mutex));
            if (id < 2) return 0;
            var pagesPerMapPage = (int)(bt.UsableSize / 5 + 1);
            var ptrMap = (Pid)((id - 2) / pagesPerMapPage);
            var ret = (Pid)(ptrMap * pagesPerMapPage) + 2;
            if (ret == PENDING_BYTE_PAGE(bt))
                ret++;
            return ret;
        }

        static void ptrmapPut(BtShared bt, Pid key, byte type, Pid parent, ref RC rcRef)
        {
            if (rcRef != RC.OK) return;

            Debug.Assert(MutexEx.Held(bt.Mutex));
            // The master-journal page number must never be used as a pointer map page
            Debug.Assert(!PTRMAP_ISPAGE(bt, PENDING_BYTE_PAGE(bt)));

            Debug.Assert(bt.AutoVacuum);
            if (key == 0)
            {
                rcRef = SysEx.CORRUPT_BKPT();
                return;
            }
            var ptrmapIdx = PTRMAP_PAGENO(bt, key); // The pointer map page number
            var page = (IPage)new PgHdr(); // The pointer map page
            var rc = bt.Pager.Acquire(ptrmapIdx, ref page, false);
            if (rc != RC.OK)
            {
                rcRef = rc;
                return;
            }
            var offset = (int)PTRMAP_PTROFFSET(ptrmapIdx, key); // Offset in pointer map page
            if (offset < 0)
            {
                rcRef = SysEx.CORRUPT_BKPT();
                goto ptrmap_exit;
            }
            Debug.Assert(offset <= (int)bt.UsableSize - 5);
            var ptrmap = Pager.GetData(page); // The pointer map page

            if (type != ptrmap[offset] || ConvertEx.Get4(ptrmap, offset + 1) != parent)
            {
                TRACE("PTRMAP_UPDATE: %d->(%d,%d)\n", key, type, parent);
                rcRef = rc = Pager.Write(page);
                if (rc == RC.OK)
                {
                    ptrmap[offset] = type;
                    ConvertEx.Put4(ptrmap, offset + 1, parent);
                }
            }

        ptrmap_exit:
            Pager.Unref(page);
        }

        static RC ptrmapGet(BtShared bt, Pid key, ref byte type, ref Pid id)
        {
            Debug.Assert(MutexEx.Held(bt.Mutex));

            var page = (IPage)new PgHdr(); // The pointer map page
            var ptrmapIdx = (Pid)PTRMAP_PAGENO(bt, key); // Pointer map page index
            var rc = bt.Pager.Acquire(ptrmapIdx, ref page, false);
            if (rc != RC.OK)
                return rc;
            var ptrmap = Pager.GetData(page); // Pointer map page data

            var offset = (int)PTRMAP_PTROFFSET(ptrmapIdx, key); // Offset of entry in pointer map
            if (offset < 0)
            {
                Pager.Unref(page);
                return SysEx.CORRUPT_BKPT();
            }
            Debug.Assert(offset <= (int)bt.UsableSize - 5);
            Debug.Assert(type != 0);
            type = ptrmap[offset];
            id = ConvertEx.Get4(ptrmap, offset + 1);

            Pager.Unref(page);
            if (type < 1 || type > 5) return SysEx.CORRUPT_BKPT();
            return RC.OK;
        }

#else
//#define ptrmapPut(w,x,y,z,rc)
//#define ptrmapGet(w,x,y,z) RC.OK
//#define ptrmapPutOvflPtr(x, y, rc)
#endif

        static int findCell(MemPage page, int cell) { return ConvertEx.Get2(page.Data, page.CellOffset + 2 * cell); }
        //static uint8[] findCellv2(u8[] page, u16 cell, u16 o, int i) { Debugger.Break(); return pPage; }

        static int findOverflowCell(MemPage page, int cell)
        {
            Debug.Assert(MutexEx.Held(page.Bt.Mutex));
            for (var i = page.OverflowsUsed - 1; i >= 0; i--)
            {
                var ovfl = page.Overflows[i];
                var k = ovfl.Idx;
                if (k <= cell)
                {
                    if (k == cell)
                    {
                        //return ovfl.Cell;
                        return -i - 1; // Negative Offset means overflow cells
                    }
                    cell--;
                }
            }
            return findCell(page, cell);
        }

        static void btreeParseCellPtr(MemPage page, int cellIdx, ref CellInfo info) { btreeParseCellPtr(page, page.Data, cellIdx, ref info); }
        static void btreeParseCellPtr(MemPage page, byte[] cell, ref CellInfo info) { btreeParseCellPtr(page, cell, 0, ref info); }
        static void btreeParseCellPtr(MemPage page, byte[] cell, int cellIdx, ref CellInfo info)
        {
            Debug.Assert(MutexEx.Held(page.Bt.Mutex));

            if (info.Cell != cell) info.Cell = cell;
            info.CellIdx = cellIdx;
            Debug.Assert(page.Leaf == 0 || page.Leaf == 1);
            ushort n = page.ChildPtrSize; // Number bytes in cell content header
            Debug.Assert(n == 4 - 4 * page.Leaf);
            uint payloadLength = 0; // Number of bytes of cell payload
            if (page.IntKey != 0)
            {
                if (page.HasData != 0)
                    n += (ushort)ConvertEx.GetVaraint4(cell, cellIdx + n, out payloadLength);
                else
                    payloadLength = 0;
                n += (ushort)ConvertEx.GetVaraint(cell, cellIdx + n, out info.Key);
                info.Data = payloadLength;
            }
            else
            {
                info.Data = 0;
                n += (ushort)ConvertEx.GetVaraint4(cell, cellIdx + n, out payloadLength);
                info.Key = payloadLength;
            }
            info.Payload = payloadLength;
            info.Header = n;
            if (payloadLength <= page.MaxLocal)
            {
                // This is the (easy) common case where the entire payload fits on the local page.  No overflow is required.
                if ((info.Size = (ushort)(n + payloadLength)) < 4) info.Size = 4;
                info.Local = (ushort)payloadLength;
                info.Overflow = 0;
            }
            else
            {
                // If the payload will not fit completely on the local page, we have to decide how much to store locally and how much to spill onto
                // overflow pages.  The strategy is to minimize the amount of unused space on overflow pages while keeping the amount of local storage
                // in between minLocal and maxLocal.
                //
                // Warning:  changing the way overflow payload is distributed in any way will result in an incompatible file format.
                int minLocal = page.MinLocal; // Minimum amount of payload held locally
                int maxLocal = page.MaxLocal; // Maximum amount of payload held locally
                int surplus = (int)(minLocal + (payloadLength - minLocal) % (page.Bt.UsableSize - 4)); // Overflow payload available for local storage
                if (surplus <= maxLocal)
                    info.Local = (ushort)surplus;
                else
                    info.Local = (ushort)minLocal;
                info.Overflow = (ushort)(info.Local + n);
                info.Size = (ushort)(info.Overflow + 4);
            }
        }

        static void parseCell(MemPage page, int cell, ref CellInfo info) { btreeParseCellPtr(page, findCell(page, cell), ref info); }
        static void btreeParseCell(MemPage page, int cell, ref CellInfo info) { parseCell(page, cell, ref info); }

        // Alternative form for C#
        static ushort cellSizePtr(MemPage page, int cellIdx)
        {
            var info = new CellInfo();
            var cell = new byte[13];
            // Minimum Size = (2 bytes of Header  or (4) Child Pointer) + (maximum of) 9 bytes data
            if (cellIdx < 0) // Overflow Cell
                Buffer.BlockCopy(page.Overflows[-(cellIdx + 1)].Cell, 0, cell, 0, cell.Length < page.Overflows[-(cellIdx + 1)].Cell.Length ? cell.Length : page.Overflows[-(cellIdx + 1)].Cell.Length);
            else if (cellIdx >= page.Data.Length + 1 - cell.Length)
                Buffer.BlockCopy(page.Data, cellIdx, cell, 0, page.Data.Length - cellIdx);
            else
                Buffer.BlockCopy(page.Data, cellIdx, cell, 0, cell.Length);
            btreeParseCellPtr(page, cell, ref info);
            return info.Size;
        }
        // Alternative form for C#
        static ushort cellSizePtr(MemPage page, byte[] cell, int offset)
        {
            var info = new CellInfo();
            info.Cell = SysEx.Alloc(cell.Length);
            Buffer.BlockCopy(cell, offset, info.Cell, 0, cell.Length - offset);
            btreeParseCellPtr(page, info.Cell, ref info);
            return info.Size;
        }
        static ushort cellSizePtr(MemPage page, byte[] cell)
        {
#if DEBUG
            // The value returned by this function should always be the same as the (CellInfo.nSize) value found by doing a full parse of the
            // cell. If SQLITE_DEBUG is defined, an assert() at the bottom of this function verifies that this invariant is not violated.
            var debuginfo = new CellInfo();
            btreeParseCellPtr(page, cell, ref debuginfo);
#else
            var debuginfo = new CellInfo();
#endif
            var iterIdx = page.ChildPtrSize; //var iter = &cell[page.ChildPtrSize];
            uint size = 0;
            if (page.IntKey != 0)
            {
                if (page.HasData != 0)
                    iterIdx += ConvertEx.GetVarint4(cell, out size); // iter += ConvertEx.GetVarint4(iter, out size);
                else
                    size = 0;

                // pIter now points at the 64-bit integer key value, a variable length integer. The following block moves pIter to point at the first byte
                // past the end of the key value.
                int end = iterIdx + 9; // end = &pIter[9];
                while (((cell[iterIdx++]) & 0x80) != 0 && iterIdx < end) { } // while ((iter++) & 0x80 && iter < end);
            }
            else
                iterIdx += ConvertEx.GetVarint4(cell, iterIdx, out size); //pIter += getVarint32( pIter, out nSize );

            if (size > page.MaxLocal)
            {
                int minLocal = page.MinLocal;
                size = (uint)(minLocal + (size - minLocal) % (page.Bt.UsableSize - 4));
                if (size > page.MaxLocal)
                    size = (uint)minLocal;
                size += 4;
            }
            size += (uint)iterIdx; // size += (u32)(iter - cell);

            // The minimum size of any cell is 4 bytes.
            if (size < 4)
                size = 4;

            Debug.Assert(size == debuginfo.Size);
            return (ushort)size;
        }

#if DEBUG
        static ushort cellSize(MemPage page, int cell)
        {
            return cellSizePtr(page, findCell(page, cell));
        }
#else
        static int cellSize(MemPage pPage, int iCell) { return -1; }
#endif

#if !OMIT_AUTOVACUUM
        static void ptrmapPutOvflPtr(MemPage page, int cell, ref RC rcRef)
        {
            if (rcRef != RC.OK) return;
            var info = new CellInfo();
            Debug.Assert(cell != 0);
            btreeParseCellPtr(page, cell, ref info);
            Debug.Assert((info.Data + (page.IntKey != 0 ? 0 : info.Key)) == info.Payload);
            if (info.Overflow != 0)
            {
                Pid ovfl = ConvertEx.Get4(page.Data, cell, info.Overflow);
                ptrmapPut(page.Bt, ovfl, PTRMAP_OVERFLOW1, page.ID, ref rcRef);
            }
        }

        static void ptrmapPutOvflPtr(MemPage page, byte[] cell, ref RC rcRef)
        {
            if (rcRef != RC.OK) return;
            Debug.Assert(cell != null);
            var info = new CellInfo();
            btreeParseCellPtr(page, cell, ref info);
            Debug.Assert((info.Data + (page.IntKey != 0 ? 0 : info.Key)) == info.Payload);
            if (info.Overflow != 0)
            {
                Pid ovfl = ConvertEx.Get4(cell, info.Overflow);
                ptrmapPut(page.Bt, ovfl, PTRMAP_OVERFLOW1, page.ID, ref rcRef);
            }
        }
#endif

        #endregion

        #region Allocate / Defragment

        static RC defragmentPage(MemPage page)
        {
            Debug.Assert(Pager.Iswriteable(page.DBPage));
            Debug.Assert(page.Bt != null);
            Debug.Assert(page.Bt.UsableSize <= MAX_PAGE_SIZE);
            Debug.Assert(page.OverflowsUsed == 0);
            Debug.Assert(MutexEx.Held(page.Bt.Mutex));
            var temp = page.Bt.Pager.get_TempSpace(); // Temp area for cell content
            var data = page.Data; // The page data
            var hdr = page.HdrOffset; // Offset to the page header
            var cellOffset = page.CellOffset; // Offset to the cell pointer array
            var cells = page.Cells; // Number of cells on the page
            Debug.Assert(cells == ConvertEx.Get2(data, hdr + 3));
            var usableSize = (int)page.Bt.UsableSize; // Number of usable bytes on a page
            var cbrk = (int)ConvertEx.Get2(data, hdr + 5); // Offset to the cell content area
            Buffer.BlockCopy(data, cbrk, temp, cbrk, usableSize - cbrk); // memcpy(temp[cbrk], ref data[cbrk], usableSize - cbrk);
            cbrk = usableSize;
            var cellFirst = cellOffset + 2 * cells; // First allowable cell index
            var cellLast = usableSize - 4; // Last possible cell index
            var addr = 0;  // The i-th cell pointer
            for (var i = 0; i < cells; i++)
            {
                addr = cellOffset + i * 2;
                int pc = ConvertEx.Get2(data, addr); // Address of a i-th cell
#if !ENABLE_OVERSIZE_CELL_CHECK
                // These conditions have already been verified in btreeInitPage() if ENABLE_OVERSIZE_CELL_CHECK is defined
                if (pc < cellFirst || pc > cellLast)
                    return SysEx.CORRUPT_BKPT();
#endif
                Debug.Assert(pc >= cellFirst && pc <= cellLast);
                int size = cellSizePtr(page, temp, pc); // Size of a cell
                cbrk -= size;
#if ENABLE_OVERSIZE_CELL_CHECK
                if (cbrk < cellFirst || pc + size > usableSize)
                    return SysEx.CORRUPT_BKPT();
#else
                if (cbrk < cellFirst || pc + size > usableSize)
                    return SysEx.CORRUPT_BKPT();
#endif
                Debug.Assert(cbrk + size <= usableSize && cbrk >= cellFirst);
                Buffer.BlockCopy(temp, pc, data, cbrk, size);
                ConvertEx.Put2(data, addr, cbrk);
            }
            Debug.Assert(cbrk >= cellFirst);
            ConvertEx.Put2(data, hdr + 5, cbrk);
            data[hdr + 1] = 0;
            data[hdr + 2] = 0;
            data[hdr + 7] = 0;
            addr = cellOffset + 2 * cells;
            Array.Clear(data, addr, cbrk - addr);
            Debug.Assert(Pager.Iswriteable(page.DBPage));
            if (cbrk - cellFirst != page.Frees)
                return SysEx.CORRUPT_BKPT();
            return RC.OK;
        }

        static RC allocateSpace(MemPage page, int bytes, ref int idx)
        {
            Debug.Assert(Pager.Iswriteable(page.DBPage));
            Debug.Assert(page.Bt != null);
            Debug.Assert(MutexEx.Held(page.Bt.Mutex));
            Debug.Assert(bytes >= 0);  // Minimum cell size is 4
            Debug.Assert(page.Frees >= bytes);
            Debug.Assert(page.OverflowsUsed == 0);
            var usableSize = page.Bt.UsableSize; // Usable size of the page
            Debug.Assert(bytes < usableSize - 8);

            var hdr = page.HdrOffset;  // Local cache of pPage.hdrOffset
            var data = page.Data;    // Local cache of pPage.aData
            var frags = data[hdr + 7]; // Number of fragmented bytes on pPage
            Debug.Assert(page.CellOffset == hdr + 12 - 4 * page.Leaf);
            var gap = page.CellOffset + 2 * page.Cells; // First byte of gap between cell pointers and cell content
            var top = ConvertEx.Get2nz(data, hdr + 5); // First byte of cell content area
            if (gap > top) return SysEx.CORRUPT_BKPT();

            RC rc;
            if (frags >= 60)
            {
                // Always defragment highly fragmented pages
                rc = defragmentPage(page);
                if (rc != RC.OK) return rc;
                top = ConvertEx.Get2nz(data, hdr + 5);
            }
            else if (gap + 2 <= top)
            {
                // Search the freelist looking for a free slot big enough to satisfy the request. The allocation is made from the first free slot in 
                // the list that is large enough to accomadate it.
                int pc;
                for (int addr = hdr + 1; (pc = ConvertEx.Get2(data, addr)) > 0; addr = pc)
                {
                    if (pc > usableSize - 4 || pc < addr + 4)
                        return SysEx.CORRUPT_BKPT();
                    int size = ConvertEx.Get2(data, pc + 2); // Size of free slot
                    if (size >= bytes)
                    {
                        int x = size - bytes;
                        if (x < 4)
                        {
                            // Remove the slot from the free-list. Update the number of fragmented bytes within the page.
                            data[addr + 0] = data[pc + 0]; // memcpy( data[addr], ref data[pc], 2 );
                            data[addr + 1] = data[pc + 1];
                            data[hdr + 7] = (byte)(frags + x);
                        }
                        else if (size + pc > usableSize)
                            return SysEx.CORRUPT_BKPT();
                        else // The slot remains on the free-list. Reduce its size to account for the portion used by the new allocation.
                            ConvertEx.Put2(data, pc + 2, x);
                        idx = pc + x;
                        return RC.OK;
                    }
                }
            }

            // Check to make sure there is enough space in the gap to satisfy the allocation.  If not, defragment.
            if (gap + 2 + bytes > top)
            {
                rc = defragmentPage(page);
                if (rc != RC.OK) return rc;
                top = ConvertEx.Get2nz(data, hdr + 5);
                Debug.Assert(gap + bytes <= top);
            }

            // Allocate memory from the gap in between the cell pointer array and the cell content area.  The btreeInitPage() call has already
            // validated the freelist.  Given that the freelist is valid, there is no way that the allocation can extend off the end of the page.
            // The assert() below verifies the previous sentence.
            top -= bytes;
            ConvertEx.Put2(data, hdr + 5, top);
            Debug.Assert(top + bytes <= (int)page.Bt.UsableSize);
            idx = top;
            return RC.OK;
        }

        static RC freeSpace(MemPage page, uint start, int size) { return freeSpace(page, (int)start, size); }
        static RC freeSpace(MemPage page, int start, int size)
        {
            Debug.Assert(page.Bt != null);
            Debug.Assert(Pager.Iswriteable(page.DBPage));
            Debug.Assert(start >= page.HdrOffset + 6 + page.ChildPtrSize);
            Debug.Assert((start + size) <= (int)page.Bt.UsableSize);
            Debug.Assert(MutexEx.Held(page.Bt.Mutex));
            Debug.Assert(size >= 0); // Minimum cell size is 4

            var data = page.Data;
            if ((page.Bt.BtsFlags & BTS.SECURE_DELETE) != 0) // Overwrite deleted information with zeros when the secure_delete option is enabled
                Array.Clear(data, start, size);

            // Add the space back into the linked list of freeblocks.  Note that even though the freeblock list was checked by btreeInitPage(),
            // btreeInitPage() did not detect overlapping cells or freeblocks that overlapped cells.   Nor does it detect when the
            // cell content area exceeds the value in the page header.  If these situations arise, then subsequent insert operations might corrupt
            // the freelist.  So we do need to check for corruption while scanning the freelist.
            int hdr = page.HdrOffset;
            int addr = hdr + 1;
            int last = (int)page.Bt.UsableSize - 4; // Largest possible freeblock offset
            Debug.Assert(start <= last);
            int pbegin;
            while ((pbegin = ConvertEx.Get2(data, addr)) < start && pbegin > 0)
            {
                if (pbegin < addr + 4)
                    return SysEx.CORRUPT_BKPT();
                addr = pbegin;
            }
            if (pbegin > last)
                return SysEx.CORRUPT_BKPT();
            Debug.Assert(pbegin > addr || pbegin == 0);
            ConvertEx.Put2(data, addr, start);
            ConvertEx.Put2(data, start, pbegin);
            ConvertEx.Put2(data, start + 2, size);
            page.Frees = (ushort)(page.Frees + size);

            // Coalesce adjacent free blocks
            addr = hdr + 1;
            while ((pbegin = ConvertEx.Get2(data, addr)) > 0)
            {
                Debug.Assert(pbegin > addr);
                Debug.Assert(pbegin <= (int)page.Bt.UsableSize - 4);
                int pnext = ConvertEx.Get2(data, pbegin);
                int psize = ConvertEx.Get2(data, pbegin + 2);
                if (pbegin + psize + 3 >= pnext && pnext > 0)
                {
                    int frag = pnext - (pbegin + psize);
                    if (frag < 0 || frag > (int)data[hdr + 7])
                        return SysEx.CORRUPT_BKPT();
                    data[hdr + 7] -= (byte)frag;
                    int x = ConvertEx.Get2(data, pnext);
                    ConvertEx.Put2(data, pbegin, x);
                    x = pnext + ConvertEx.Get2(data, pnext + 2) - pbegin;
                    ConvertEx.Put2(data, pbegin + 2, x);
                }
                else
                    addr = pbegin;
            }

            // If the cell content area begins with a freeblock, remove it.
            if (data[hdr + 1] == data[hdr + 5] && data[hdr + 2] == data[hdr + 6])
            {
                pbegin = ConvertEx.Get2(data, hdr + 1);
                ConvertEx.Put2(data, hdr + 1, ConvertEx.Get2(data, pbegin)); // memcpy( data[hdr + 1], ref data[pbegin], 2 );
                int top = ConvertEx.Get2(data, hdr + 5) + ConvertEx.Get2(data, pbegin + 2);
                ConvertEx.Put2(data, hdr + 5, top);
            }
            Debug.Assert(Pager.Iswriteable(page.DBPage));
            return RC.OK;
        }

        static RC decodeFlags(MemPage page, int flagByte)
        {
            Debug.Assert(page.HdrOffset == (page.ID == 1 ? 100 : 0));
            Debug.Assert(MutexEx.Held(page.Bt.Mutex));
            page.Leaf = (byte)(flagByte >> 3); Debug.Assert(PTF_LEAF == 1 << 3);
            flagByte &= ~PTF_LEAF;
            page.ChildPtrSize = (byte)(4 - 4 * page.Leaf);
            BtShared bt = page.Bt; // A copy of pPage.pBt
            if (flagByte == (PTF_LEAFDATA | PTF_INTKEY))
            {
                page.IntKey = 1;
                page.HasData = page.Leaf;
                page.MaxLocal = bt.MaxLeaf;
                page.MinLocal = bt.MinLeaf;
            }
            else if (flagByte == PTF_ZERODATA)
            {
                page.IntKey = 0;
                page.HasData = 0;
                page.MaxLocal = bt.MaxLocal;
                page.MinLocal = bt.MinLocal;
            }
            else
                return SysEx.CORRUPT_BKPT();
            page.Max1bytePayload = bt.Max1bytePayload;
            return RC.OK;
        }

        static RC btreeInitPage(MemPage page)
        {
            Debug.Assert(page.Bt != null);
            Debug.Assert(MutexEx.Held(page.Bt.Mutex));
            Debug.Assert(page.ID == Pager.GetPageID(page.DBPage));
            Debug.Assert(page == Pager.GetExtra(page.DBPage));
            Debug.Assert(page.Data == Pager.GetData(page.DBPage));

            if (!page.IsInit)
            {
                var bt = page.Bt; // The main btree structure

                var hdr = page.HdrOffset; // Offset to beginning of page header 
                var data = page.Data; // Equal to pPage.aData
                if (decodeFlags(page, data[hdr]) != RC.OK) return SysEx.CORRUPT_BKPT();
                Debug.Assert(bt.PageSize >= 512 && bt.PageSize <= 65536);
                page.MaskPage = (ushort)(bt.PageSize - 1);
                page.OverflowsUsed = 0;
                int usableSize = (int)bt.UsableSize; // Amount of usable space on each page
                ushort cellOffset; // Offset from start of page to first cell pointer
                page.CellOffset = (cellOffset = (ushort)(hdr + 12 - 4 * page.Leaf));
                int top = ConvertEx.Get2nz(data, hdr + 5); // First byte of the cell content area
                page.Cells = (ushort)(ConvertEx.Get2(data, hdr + 3));
                if (page.Cells > MX_CELL(bt))
                    // To many cells for a single page.  The page must be corrupt
                    return SysEx.CORRUPT_BKPT();

                // A malformed database page might cause us to read past the end of page when parsing a cell.  
                //
                // The following block of code checks early to see if a cell extends past the end of a page boundary and causes SQLITE_CORRUPT to be 
                // returned if it does.
                int cellFirst = cellOffset + 2 * page.Cells; // First allowable cell or freeblock offset
                int cellLast = usableSize - 4; // Last possible cell or freeblock offset
                ushort pc; // Address of a freeblock within pPage.aData[]
#if ENABLE_OVERSIZE_CELL_CHECK
                {
                    if (page.Leaf == 0) cellLast--;
                    for (var i = 0; i < page.Cells; i++)
                    {
                        pc = (ushort)ConvertEx.Get2(data, cellOffset + i * 2);
                        if (pc < cellFirst || pc > cellLast)
                            return SysEx.CORRUPT_BKPT();
                        int sz = cellSizePtr(page, data, pc); // Size of a cell
                        if (pc + sz > usableSize)
                            return SysEx.CORRUPT_BKPT();
                    }
                    if (page.Leaf == 0) cellLast++;
                }
#endif

                // Compute the total free space on the page
                pc = (ushort)get2byte(data, hdr + 1);
                int free = (ushort)(data[hdr + 7] + top); // Number of unused bytes on the page
                while (pc > 0)
                {
                    if (pc < cellFirst || pc > cellLast)
                        // Start of free block is off the page
                        return SysEx.CORRUPT_BKPT();
                    var next = (ushort)ConvertEx.Get2(data, pc);
                    var size = (ushort)ConvertEx.Get2(data, pc + 2);
                    if ((next > 0 && next <= pc + size + 3) || pc + size > usableSize)
                        // Free blocks must be in ascending order. And the last byte of the free-block must lie on the database page.
                        return SysEx.CORRUPT_BKPT();
                    free = (ushort)(free + size);
                    pc = next;
                }

                // At this point, nFree contains the sum of the offset to the start of the cell-content area plus the number of free bytes within
                // the cell-content area. If this is greater than the usable-size of the page, then the page must be corrupted. This check also
                // serves to verify that the offset to the start of the cell-content area, according to the page header, lies within the page.
                if (free > usableSize)
                    return SysEx.CORRUPT_BKPT();
                page.Frees = (ushort)(free - cellFirst);
                page.IsInit = true;
            }
            return RC.OK;
        }

        static void zeroPage(MemPage page, int flags)
        {
            var bt = page.Bt;
            var data = page.Data;
            Debug.Assert(Pager.GetPageID(page.DBPage) == page.ID);
            Debug.Assert(Pager.GetExtra(page.DBPage) == page);
            Debug.Assert(Pager.GetData(page.DBPage) == data);
            Debug.Assert(Pager.Iswriteable(page.DBPage));
            Debug.Assert(MutexEx.Held(bt.Mutex));
            var hdr = page.HdrOffset;
            if ((bt.BtsFlags & BTS.SECURE_DELETE) != 0)
                Array.Clear(data, hdr, (int)(bt.UsableSize - hdr));
            data[hdr] = (byte)flags;
            var first = (ushort)(hdr + 8 + 4 * ((flags & PTF_LEAF) == 0 ? 1 : 0));
            Array.Clear(data, hdr + 1, 4);
            data[hdr + 7] = 0;
            ConvertEx.Put2(data, hdr + 5, bt.UsableSize);
            page.Frees = (ushort)(bt.UsableSize - first);
            decodeFlags(page, flags);
            page.HdrOffset = hdr;
            //page.DataEnd = &data[pt.UsableSize];
            //page.CellIdx = &data[first];
            page.CellOffset = first;
            page.OverflowsUsed = 0;
            Debug.Assert(bt.PageSize >= 512 && bt.PageSize <= 65536);
            page.MaskPage = (ushort)(bt.PageSize - 1);
            page.Cells = 0;
            page.IsInit = true;
        }

        #endregion

        #region Page

        static MemPage btreePageFromDbPage(IPage dbPage, Pid id, BtShared bt)
        {
            MemPage page = (MemPage)Pager.GetExtra(dbPage);
            page.Data = Pager.GetData(dbPage);
            page.DBPage = dbPage;
            page.Bt = bt;
            page.ID = id;
            page.HdrOffset = (byte)(page.ID == 1 ? 100 : 0);
            return page;
        }

        static RC btreeGetPage(BtShared bt, Pid id, ref MemPage page, bool noContent)
        {
            Debug.Assert(MutexEx.Held(bt.Mutex));
            IPage dbPage = null;
            var rc = bt.Pager.Acquire(id, ref dbPage, noContent);
            if (rc != RC.OK) return rc;
            page = btreePageFromDbPage(dbPage, id, bt);
            return RC.OK;
        }

        static MemPage btreePageLookup(BtShared bt, Pid id)
        {
            Debug.Assert(MutexEx.Held(bt.Mutex));
            var dbPage = bt.Pager.Lookup(id);
            return (dbPage != null ? btreePageFromDbPage(dbPage, id, bt) : null);
        }

        static Pid btreePagecount(BtShared bt)
        {
            return bt.Pages;
        }

        public Pid LastPage()
        {
            Debug.Assert(HoldsMutex());
            Debug.Assert(((Bt.Pages) & 0x8000000) == 0);
            return (Pid)btreePagecount(Bt);
        }

        static RC getAndInitPage(BtShared bt, Pid id, ref MemPage page)
        {
            Debug.Assert(MutexEx.Held(bt.Mutex));

            RC rc;
            if (id > btreePagecount(bt))
                rc = SysEx.CORRUPT_BKPT();
            else
            {
                rc = btreeGetPage(bt, id, ref page, false);
                if (rc == RC.OK)
                {
                    rc = btreeInitPage(page);
                    if (rc != RC.OK)
                        releasePage(page);
                }
            }

            Debug.Assert(id != 0 || rc == RC.CORRUPT);
            return rc;
        }

        static void releasePage(MemPage page)
        {
            if (page != null)
            {
                Debug.Assert(page.Data != null);
                Debug.Assert(page.Bt != null);
                Debug.Assert(Pager.GetExtra(page.DBPage) == page);
                Debug.Assert(Pager.GetData(page.DBPage) == page.Data);
                Debug.Assert(MutexEx.Held(page.Bt.Mutex));
                Pager.Unref(page.DBPage);
            }
        }

        static void pageReinit(IPage dbPage)
        {
            MemPage page = Pager.GetExtra(dbPage);
            Debug.Assert(Pager.get_PageRefs(dbPage) > 0);
            if (page.IsInit)
            {
                Debug.Assert(MutexEx.Held(page.Bt.Mutex));
                page.IsInit = false;
                if (Pager.get_PageRefs(dbPage) > 1)
                {
                    // pPage might not be a btree page;  it might be an overflow page or ptrmap page or a free page.  In those cases, the following
                    // call to btreeInitPage() will likely return SQLITE_CORRUPT. But no harm is done by this.  And it is very important that
                    // btreeInitPage() be called on every btree page so we make the call for every page that comes in for re-initing.
                    btreeInitPage(page);
                }
            }
        }

        #endregion

        #region Open / Close

        static int btreeInvokeBusyHandler(object arg)
        {
            var bt = (BtShared)arg;
            Debug.Assert(bt.Ctx != null);
            Debug.Assert(MutexEx.Held(bt.Ctx.Mutex));
            return sqlite3InvokeBusyHandler(bt.Ctx.BusyHandler);
        }

        static RC Open(VFileSystem vfs, string filename, Context ctx, ref Btree btree, OPEN flags, VFileSystem.OPEN vfsFlags)
        {
            // True if opening an ephemeral, temporary database
            bool tempDB = string.IsNullOrEmpty(filename);

            // Set the variable isMemdb to true for an in-memory database, or false for a file-based database.
            bool memoryDB = (filename == ":memory:") ||
                (tempDB && sqlite3TempInMemory(ctx)) ||
                (vfsFlags & VFileSystem.OPEN.MEMORY) != 0;

            Debug.Assert(ctx != null);
            Debug.Assert(vfs != null);
            Debug.Assert(MutexEx.Held(ctx.Mutex));
            Debug.Assert(((int)flags & 0xff) == (int)flags); // flags fit in 8 bits

            // Only a BTREE_SINGLE database can be BTREE_UNORDERED
            Debug.Assert((flags & OPEN.UNORDERED) == 0 || (flags & OPEN.SINGLE) != 0);

            // A BTREE_SINGLE database is always a temporary and/or ephemeral
            Debug.Assert((flags & OPEN.SINGLE) == 0 || tempDB);

            if (memoryDB)
                flags |= OPEN.MEMORY;
            if ((vfsFlags & VFileSystem.OPEN.MAIN_DB) != 0 && (memoryDB || tempDB))
                vfsFlags = (vfsFlags & ~VFileSystem.OPEN.MAIN_DB) | VFileSystem.OPEN.TEMP_DB;
            var p = new Btree(); // Handle to return
            p.InTrans = TRANS.NONE;
            p.Ctx = ctx;
#if !OMIT_SHARED_CACHE
            p.Lock.Btree = p;
            p.Lock.Table = 1;
#endif

            RC rc = RC.OK; // Result code from this function
            BtShared bt = null; // Shared part of btree structure
            MutexEx mutexOpen;
#if !OMIT_SHARED_CACHE && !OMIT_DISKIO
            // If this Btree is a candidate for shared cache, try to find an existing BtShared object that we can share with
            if (!tempDB && (!memoryDB || (vfsFlags & VFileSystem.OPEN.URI) != 0))
                if ((vfsFlags & VFileSystem.OPEN.SHAREDCACHE) != 0)
                {
                    string fullPathname;
                    p.Sharable = true;
                    if (memoryDB)
                        fullPathname = filename;
                    else
                        vfs.FullPathname(filename, out fullPathname);
                    MutexEx mutexShared;
#if THREADSAFE
                    mutexOpen = MutexEx.Alloc(MutexEx.MUTEX.STATIC_OPEN); // Prevents a race condition. Ticket #3537
                    MutexEx.Enter(mutexOpen);
                    mutexShared = MutexEx.Alloc(MutexEx.MUTEX.STATIC_MASTER);
                    MutexEx.Enter(mutexShared);
#endif
                    for (bt = _sharedCacheList; bt != null; bt = bt.Next)
                    {
                        Debug.Assert(bt.Refs > 0);
                        if (fullPathname == bt.Pager.get_Filename(false) && bt.Pager.get_Vfs() == vfs)
                        {
                            for (var i = ctx.DBs.Length - 1; i >= 0; i--)
                            {
                                var existing = ctx.DBs[i].Bt;
                                if (existing && existing.Bt == bt)
                                {
                                    MutexEx.Leave(mutexShared);
                                    MutexEx.Leave(mutexOpen);
                                    fullPathname = null;
                                    p = null;
                                    return RC.CONSTRAINT;
                                }
                            }
                            p.Bt = bt;
                            bt.Refs++;
                            break;
                        }
                    }
                    MutexEx.Leave(mutexShared);
                    fullPathname = null;
                }
#if DEBUG
                else
                    // In debug mode, we mark all persistent databases as sharable even when they are not.  This exercises the locking code and
                    // gives more opportunity for asserts(sqlite3_mutex_held()) statements to find locking problems.
                    p.Sharable = true;
#endif
#endif

            byte reserves; // Byte of unused space on each page
            var dbHeader = new byte[100]; // Database header content
            if (bt == null)
            {
                // The following asserts make sure that structures used by the btree are the right size.  This is to guard against size changes that result
                // when compiling on a different architecture.
                Debug.Assert(sizeof(long) == 8 || sizeof(long) == 4);
                Debug.Assert(sizeof(ulong) == 8 || sizeof(ulong) == 4);
                Debug.Assert(sizeof(uint) == 4);
                Debug.Assert(sizeof(ushort) == 2);
                Debug.Assert(sizeof(Pid) == 4);

                bt = new BtShared();
                rc = Pager.Open(vfs, out bt.Pager, filename, EXTRA_SIZE, (IPager.PAGEROPEN)flags, vfsFlags, pageReinit);
                if (rc == RC.OK)
                    rc = bt.Pager.ReadFileHeader(dbHeader.Length, dbHeader);
                if (rc != RC.OK)
                    goto btree_open_out;
                bt.OpenFlags = flags;
                bt.Ctx = ctx;
                bt.Pager.SetBusyHandler(btreeInvokeBusyHandler, bt);
                p.Bt = bt;

                bt.Cursor = null;
                bt.Page1 = null;
                if (Pager.Isreadonly(bt.Pager)) bt.BtsFlags |= BTS.READ_ONLY;
#if SECURE_DELETE
                bt.BtsFlags |= BTS.SECURE_DELETE;
#endif
                bt.PageSize = (Pid)((dbHeader[16] << 8) | (dbHeader[17] << 16));
                if (bt.PageSize < 512 || bt.PageSize > MAX_PAGE_SIZE || ((bt.PageSize - 1) & bt.PageSize) != 0)
                {
                    bt.PageSize = 0;
#if !OMIT_AUTOVACUUM
                    // If the magic name ":memory:" will create an in-memory database, then leave the autoVacuum mode at 0 (do not auto-vacuum), even if
                    // SQLITE_DEFAULT_AUTOVACUUM is true. On the other hand, if SQLITE_OMIT_MEMORYDB has been defined, then ":memory:" is just a
                    // regular file-name. In this case the auto-vacuum applies as per normal.
                    if (filename != null && !memoryDB)
                    {
                        bt.AutoVacuum = (DEFAULT_AUTOVACUUM != 0);
                        bt.IncrVacuum = (DEFAULT_AUTOVACUUM == AUTOVACUUM.INCR);
                    }
#endif
                    reserves = 0;
                }
                else
                {
                    reserves = dbHeader[20];
                    bt.BtsFlags |= BTS.PAGESIZE_FIXED;
#if !OMIT_AUTOVACUUM
                    bt.AutoVacuum = (ConvertEx.Get4(dbHeader, 36 + 4 * 4) != 0);
                    bt.IncrVacuum = (ConvertEx.Get4(dbHeader, 36 + 7 * 4) != 0);
#endif
                }
                rc = bt.Pager.SetPageSize(ref bt.PageSize, reserves);
                if (rc != RC.OK) goto btree_open_out;
                bt.UsableSize = (ushort)(bt.PageSize - reserves);
                Debug.Assert((bt.PageSize & 7) == 0); // 8-byte alignment of pageSize

#if !SHARED_CACHE && !OMIT_DISKIO
                // Add the new BtShared object to the linked list sharable BtShareds.
                if (p.Sharable)
                {
                    bt.Refs = 1;
                    MutexEx mutexShared;
#if THREADSAFE
                    mutexShared = MutexEx.Alloc(MutexEx.MUTEX.STATIC_MASTER);
                    bt.Mutex = MutexEx.Alloc(MutexEx.MUTEX.FAST);
                    //if (bt.Mutex == null)
                    //{
                    //    rc = RC.NOMEM;
                    //    ctx.MallocFailed = 0;
                    //    goto btree_open_out;
                    //}
#endif
                    MutexEx.Enter(mutexShared);
                    bt.Next = _sharedCacheList;
                    _sharedCacheList = bt;
                    MutexEx.Leave(mutexShared);
                }
#endif
            }

#if !OMIT_SHARED_CACHE && !OMIT_DISKIO
            // If the new Btree uses a sharable pBtShared, then link the new Btree into the list of all sharable Btrees for the same connection.
            // The list is kept in ascending order by pBt address.
            if (p.Sharable)
            {
                Btree sib;
                for (var i = 0; i < ctx.DBs.Length; i++)
                    if ((sib = ctx.DBs[i].pBt) != null && sib.Sharable)
                    {
                        while (sib.Prev != null) { sib = sib.Prev; }
                        if (p.Bt < sib.Bt)
                        {
                            p.Next = sib;
                            p.Prev = null;
                            sib.Prev = p;
                        }
                        else
                        {
                            while (sib.Next && sib.Next.Bt < p.Bt)
                                sib = sib.Next;
                            p.Next = sib.Next;
                            p.Prev = sib;
                            if (p.Next != null)
                                p.Next.Prev = p;
                            sib.Next = p;
                        }
                        break;
                    }
            }
#endif
            btree = p;

        btree_open_out:
            if (rc != RC.OK)
            {
                if (bt != null && bt.Pager != null)
                    bt.Pager.Close();
                bt = null;
                p = null;
                btree = null;
            }
            else
                // If the B-Tree was successfully opened, set the pager-cache size to the default value. Except, when opening on an existing shared pager-cache,
                // do not change the pager-cache size.
                if (Schema(p, 0, null) == null)
                    p.Bt.Pager.SetCacheSize(DEFAULT_CACHE_SIZE);
#if THREADSAFE
            Debug.Assert(MutexEx.Held(mutexOpen));
            MutexEx.Leave(mutexOpen);
#endif
            return rc;
        }

        static bool removeFromSharingList(BtShared bt)
        {
#if !OMIT_SHARED_CACHE
            Debug.Assert(MutexEx.Held(bt.Mutex));
#if THREADSAFE
            var master = MutexEx.Alloc(MutexEx.MUTEX.STATIC_MASTER);
#endif
            var removed = false;
            MutexEx.Enter(master);
            bt.Refs--;
            if (bt.Refs <= 0)
            {
                if (_sharedCacheList == bt)
                    _sharedCacheList = bt.Next;
                else
                {
                    var list = _sharedCacheList;
                    while (SysEx.ALWAYS(list != null) && list.Next != bt)
                        list = list.Next;
                    if (SysEx.ALWAYS(list != null))
                        list.Next = bt.Next;
                }
#if THREADSAFE
                MutexEx.Free(bt.Mutex);
#endif
                removed = true;
            }
            MutexEx.Leave(master);
            return removed;
#else
            return true;
#endif
        }

        static void allocateTempSpace(BtShared bt)
        {
            if (bt.TmpSpace == null)
                bt.TmpSpace = SysEx.Alloc(bt.PageSize);
        }

        static void freeTempSpace(BtShared bt)
        {
            sqlite3PageFree(ref bt.TmpSpace);
        }

        public RC Close()
        {
            // Close all cursors opened via this handle.
            Debug.Assert(MutexEx.Held(Ctx.Mutex));
            Enter();
            var bt = Bt;
            var cur = bt.Cursor;
            while (cur != null)
            {
                var tmp = cur;
                cur = cur.Next;
                if (tmp.Btree == this)
                    CloseCursor(tmp);
            }

            // Rollback any active transaction and free the handle structure. The call to sqlite3BtreeRollback() drops any table-locks held by this handle.
            Rollback();
            Leave();

            // If there are still other outstanding references to the shared-btree structure, return now. The remainder of this procedure cleans up the shared-btree.
            Debug.Assert(WantToLock == 0 && !Locked);
            if (!Sharable || removeFromSharingList(bt))
            {
                // The pBt is no longer on the sharing list, so we can access it without having to hold the mutex.
                //
                // Clean out and delete the BtShared object.
                Debug.Assert(bt.Cursor == null);
                bt.Pager.Close();
                if (bt.FreeSchema != null && bt.Schema != null)
                    bt.FreeSchema(bt.Schema);
                bt.Schema = null;
                freeTempSpace(bt);
                bt = null;
            }

#if !OMIT_SHARED_CACHE
            Debug.Assert(WantToLock == null && Locked == null);
            if (Prev) Prev.pNext = Next;
            if (Next) Next.pPrev = Prev;
#endif

            return RC.OK;
        }

        #endregion

        #region Settings

        public RC SetCacheSize(int maxPage)
        {
            Debug.Assert(MutexEx.Held(Ctx.Mutex));
            Enter();
            Bt.Pager.SetCacheSize(maxPage);
            Leave();
            return RC.OK;
        }

#if !OMIT_PAGER_PRAGMAS
        public RC SetSafetyLevel(int level, bool fullSync, bool ckptFullSync)
        {
            Debug.Assert(MutexEx.Held(Ctx.Mutex));
            Debug.Assert(level >= 1 && level <= 3);
            Enter();
            Bt.Pager.SetSafetyLevel(level, fullSync, ckptFullSync);
            Leave();
            return RC.OK;
        }
#endif

        public bool SyncDisabled()
        {
            Debug.Assert(MutexEx.Held(Ctx.Mutex));
            Enter();
            Debug.Assert(Bt != null && Bt.Pager != null);
            var rc = Bt.Pager.get_NoSync();
            Leave();
            return rc;
        }

        public RC SetPageSize(int pageSize, int reserves, bool fix)
        {
            Debug.Assert(reserves >= -1 && reserves <= 255);
            Enter();
            BtShared bt = Bt;
            if ((bt.BtsFlags & BTS.PAGESIZE_FIXED) != 0)
            {
                Leave();
                return RC.READONLY;
            }
            if (reserves < 0)
                reserves = (int)(bt.PageSize - bt.UsableSize);
            Debug.Assert(reserves >= 0 && reserves <= 255);
            if (pageSize >= 512 && pageSize <= MAX_PAGE_SIZE && ((pageSize - 1) & pageSize) == 0)
            {
                Debug.Assert((pageSize & 7) == 0);
                Debug.Assert(bt.Page1 == null && bt.Cursor == null);
                bt.PageSize = (uint)pageSize;
                freeTempSpace(bt);
            }
            var rc = bt.Pager.SetPageSize(ref bt.PageSize, reserves);
            bt.UsableSize = (ushort)(bt.PageSize - reserves);
            if (fix) bt.BtsFlags |= BTS.PAGESIZE_FIXED;
            Leave();
            return rc;
        }

        public int GetPageSize()
        {
            return (int)Bt.PageSize;
        }

#if HAS_CODEC || _DEBUG
        public int GetReserveNoMutex()
        {
            Debug.Assert(MutexEx.Held(Bt.Mutex));
            return (int)(Bt.PageSize - Bt.UsableSize);
        }
#endif

#if !OMIT_PAGER_PRAGMAS || !OMIT_VACUUM
        public int GetReserve()
        {
            Enter();
            var n = (int)(Bt.PageSize - Bt.UsableSize);
            Leave();
            return n;
        }

        public int MaxPageCount(int maxPage)
        {
            Enter();
            var n = (int)Bt.Pager.MaxPages(maxPage);
            Leave();
            return n;
        }

        public bool SecureDelete(bool newFlag)
        {
            Enter();
            Bt.BtsFlags &= ~BTS.SECURE_DELETE;
            if (newFlag) Bt.BtsFlags |= BTS.SECURE_DELETE;
            bool b = (Bt.BtsFlags & BTS.SECURE_DELETE) != 0;
            Leave();
            return b;
        }
#endif

        public RC SetAutoVacuum(AUTOVACUUM autoVacuum)
        {
#if OMIT_AUTOVACUUM
            return RC.READONLY;
#else
            var rc = RC.OK;
            Enter();
            var bt = Bt;
            if ((bt.BtsFlags & BTS.PAGESIZE_FIXED) != 0 && (autoVacuum != 0) != bt.AutoVacuum)
                rc = RC.READONLY;
            else
            {
                bt.AutoVacuum = (autoVacuum != 0);
                bt.IncrVacuum = (autoVacuum == AUTOVACUUM.INCR);
            }
            Leave();
            return rc;
#endif
        }

        public AUTOVACUUM GetAutoVacuum()
        {
#if OMIT_AUTOVACUUM
            return AUTOVACUUM.NONE;
#else
            Enter();
            var bt = Bt;
            var rc = (!bt.AutoVacuum ? AUTOVACUUM.NONE :
                !bt.IncrVacuum ? AUTOVACUUM.FULL :
                AUTOVACUUM.INCR);
            Leave();
            return rc;
#endif
        }

        #endregion

        #region Lock / Unlock

        static RC lockBtree(BtShared bt)
        {
            Debug.Assert(MutexEx.Held(bt.Mutex));
            Debug.Assert(bt.Page1 == null);
            var rc = bt.Pager.SharedLock();
            if (rc != RC.OK) return rc;
            MemPage page1 = null; // Page 1 of the database file
            rc = btreeGetPage(bt, 1, ref page1, false);
            if (rc != RC.OK) return rc;

            // Do some checking to help insure the file we opened really is a valid database file. 
            Pid pagesHeader; // Number of pages in the database according to hdr
            Pid pages = pagesHeader = ConvertEx.Get4(page1.Data, 28); // Number of pages in the database
            Pid pagesFile = 0; // Number of pages in the database file
            bt.Pager.Pages(out pagesFile);
            if (pages == 0 || cs.memcmp(page1.Data, 24, page1.Data, 92, 4) != 0)
                pages = pagesFile;
            if (pages > 0)
            {
                var page1Data = page1.Data;
                rc = RC.NOTADB;
                if (cs.memcmp(page1Data, _magicHeader, 16) != 0)
                    goto page1_init_failed;

#if OMIT_WAL
                if (page1Data[18] > 1)
                    bt.BtsFlags |= BTS.READ_ONLY;
                if (page1Data[19] > 1)
                    goto page1_init_failed;
#else
                if (page1Data[18] > 2)
                    bt.BtsFlags |= BTS.READ_ONLY;
                if (page1Data[19] > 2)
                    goto page1_init_failed;

                // return SQLITE_OK and return without populating BtShared.pPage1. The caller detects this and calls this function again. This is
                // required as the version of page 1 currently in the page1 buffer may not be the latest version - there may be a newer one in the log file.
                if (page1Data[19] == 2 && (bt.BtsFlags & BTS.NO_WAL) == 0)
                {
                    int isOpen = 0;
                    rc = bt.Pager.OpenWal(ref isOpen);
                    if (rc != RC.OK)
                        goto page1_init_failed;
                    else if (isOpen == 0)
                    {
                        releasePage(page1);
                        return RC.OK;
                    }
                    rc = RC.NOTADB;
                }
#endif

                // The maximum embedded fraction must be exactly 25%.  And the minimum embedded fraction must be 12.5% for both leaf-data and non-leaf-data.
                // The original design allowed these amounts to vary, but as of version 3.6.0, we require them to be fixed.
                if (cs.memcmp(page1Data, 21, "\x0040\x0020\x0020", 3) != 0) // "\100\040\040"
                    goto page1_init_failed;
                uint pageSize = (uint)((page1Data[16] << 8) | (page1Data[17] << 16));
                if (((pageSize - 1) & pageSize) != 0 ||
                    pageSize > MAX_PAGE_SIZE ||
                    pageSize <= 256)
                    goto page1_init_failed;
                Debug.Assert((pageSize & 7) == 0);
                uint usableSize = pageSize - page1Data[20];
                if (pageSize != bt.PageSize)
                {
                    // After reading the first page of the database assuming a page size of BtShared.pageSize, we have discovered that the page-size is
                    // actually pageSize. Unlock the database, leave pBt->pPage1 at zero and return SQLITE_OK. The caller will call this function
                    // again with the correct page-size.
                    releasePage(page1);
                    bt.UsableSize = usableSize;
                    bt.PageSize = pageSize;
                    freeTempSpace(bt);
                    rc = bt.Pager.SetPageSize(ref bt.PageSize, (int)(pageSize - usableSize));
                    return rc;
                }
                if ((bt.Ctx.Flags & Context.FLAG.RecoveryMode) == 0 && pages > pagesFile)
                {
                    rc = SysEx.CORRUPT_BKPT();
                    goto page1_init_failed;
                }
                if (usableSize < 480)
                    goto page1_init_failed;
                bt.PageSize = pageSize;
                bt.UsableSize = usableSize;
#if !OMIT_AUTOVACUUM
                bt.AutoVacuum = (ConvertEx.Get4(page1Data, 36 + 4 * 4) != 0);
                bt.IncrVacuum = (ConvertEx.Get4(page1Data, 36 + 7 * 4) != 0);
#endif
            }

            // maxLocal is the maximum amount of payload to store locally for a cell.  Make sure it is small enough so that at least minFanout
            // cells can will fit on one page.  We assume a 10-byte page header. Besides the payload, the cell must store:
            //     2-byte pointer to the cell
            //     4-byte child pointer
            //     9-byte nKey value
            //     4-byte nData value
            //     4-byte overflow page pointer
            // So a cell consists of a 2-byte pointer, a header which is as much as 17 bytes long, 0 to N bytes of payload, and an optional 4 byte overflow
            // page pointer.
            bt.MaxLocal = (ushort)((bt.UsableSize - 12) * 64 / 255 - 23);
            bt.MinLocal = (ushort)((bt.UsableSize - 12) * 32 / 255 - 23);
            bt.MaxLeaf = (ushort)(bt.UsableSize - 35);
            bt.MinLeaf = (ushort)((bt.UsableSize - 12) * 32 / 255 - 23);
            Debug.Assert(bt.MaxLeaf + 23 <= MX_CELL_SIZE(bt));
            bt.Page1 = page1;
            bt.Pages = pages;
            return RC.OK;

        page1_init_failed:
            releasePage(page1);
            bt.Page1 = null;
            return rc;
        }

        static void unlockBtreeIfUnused(BtShared bt)
        {
            Debug.Assert(MutexEx.Held(bt.Mutex));
            Debug.Assert(bt.Cursor == null || bt.InTransaction > TRANS.NONE);
            if (bt.InTransaction == TRANS.NONE && bt.Page1 != null)
            {
                Debug.Assert(bt.Page1.Data != null);
                Debug.Assert(bt.Pager.get_Refs() == 1);
                releasePage(bt.Page1);
                bt.Page1 = null;
            }
        }

        #endregion

        #region NewDB

        static RC newDatabase(BtShared bt)
        {
            Debug.Assert(MutexEx.Held(bt.Mutex));
            if (bt.Pages > 0)
                return RC.OK;
            var p1 = bt.Page1;
            Debug.Assert(p1 != null);
            var data = p1.Data;
            var rc = Pager.Write(p1.DBPage);
            if (rc != RC.OK) return rc;
            Buffer.BlockCopy(_magicHeader, 0, data, 0, _magicHeader.Length);
            Debug.Assert(_magicHeader.Length == 16);
            data[16] = (byte)((bt.PageSize >> 8) & 0xff);
            data[17] = (byte)((bt.PageSize >> 16) & 0xff);
            data[18] = 1;
            data[19] = 1;
            Debug.Assert(bt.UsableSize <= bt.PageSize && bt.UsableSize + 255 >= bt.PageSize);
            data[20] = (byte)(bt.PageSize - bt.UsableSize);
            data[21] = 64;
            data[22] = 32;
            data[23] = 32;
            //_memset(&data[24], 0, 100 - 24);
            zeroPage(p1, PTF_INTKEY | PTF_LEAF | PTF_LEAFDATA);
            bt.BtsFlags |= BTS.PAGESIZE_FIXED;
#if !SQLITE_OMIT_AUTOVACUUM
            ConvertEx.Put4(data, 36 + 4 * 4, bt.AutoVacuum ? 1 : 0);
            ConvertEx.Put4(data, 36 + 7 * 4, bt.IncrVacuum ? 1 : 0);
#endif
            bt.Pages = 1;
            data[31] = 1;
            return RC.OK;
        }

        public RC NewDb()
        {
            Enter();
            Bt.Pages = 0;
            RC rc = newDatabase(Bt);
            Leave();
            return rc;
        }

        #endregion

        #region Transactions

        public int BeginTrans(int wrflag)
        {
            Enter();
            btreeIntegrity(this);

            // If the btree is already in a write-transaction, or it is already in a read-transaction and a read-transaction
            // is requested, this is a no-op.
            var bt = Bt;
            if (InTrans == TRANS.WRITE || (InTrans == TRANS.READ && wrflag == 0))
                goto trans_begun;
            Debug.Assert(IfNotOmitAV(Bt.DoTruncate) == 0);

            // Write transactions are not possible on a read-only database
            RC rc = RC.OK;
            if ((bt.BtsFlags & BTS.READ_ONLY) != 0 && wrflag != 0)
            {
                rc = RC.READONLY;
                goto trans_begun;
            }

#if !OMIT_SHARED_CACHE
            // If another database handle has already opened a write transaction on this shared-btree structure and a second write transaction is
            // requested, return SQLITE_LOCKED.
            Context blockingCtx = null;
            if ((wrflag && bt.InTransaction == TRANS.WRITE) || (bt.BtsFlags & BTS.PENDING) != 0)
                blockingCtx = bt.Writer.Ctx;
            else if (wrflag > 1)
            {
                for (var iter = bt.Lock; iter != null; iter = iter.Next)
                    if (iter.Btree != this)
                    {
                        blockingCtx = iter.Btree.Ctx;
                        break;
                    }
            }

            if (blockingCtx != null)
            {
                sqlite3ConnectionBlocked(Ctx, blockingCtx);
                rc = RC.LOCKED_SHAREDCACHE;
                goto trans_begun;
            }
#endif

            // Any read-only or read-write transaction implies a read-lock on page 1. So if some other shared-cache client already has a write-lock 
            // on page 1, the transaction cannot be opened. */
            rc = querySharedCacheTableLock(this, MASTER_ROOT, LOCK.READ);
            if (rc != RC.OK) goto trans_begun;

            bt.BtsFlags &= ~BTS.INITIALLY_EMPTY;
            if (bt.Pages == 0) bt.BtsFlags |= BTS.INITIALLY_EMPTY;
            do
            {
                // Call lockBtree() until either pBt->pPage1 is populated or lockBtree() returns something other than SQLITE_OK. lockBtree()
                // may return SQLITE_OK but leave pBt->pPage1 set to 0 if after reading page 1 it discovers that the page-size of the database 
                // file is not pBt->pageSize. In this case lockBtree() will update pBt->pageSize to the page-size of the file on disk.
                while (bt.Page1 == null && (rc = lockBtree(bt)) == RC.OK) ;

                if (rc == RC.OK && wrflag != 0)
                {
                    if ((bt.BtsFlags & BTS.READ_ONLY) != 0)
                        rc = RC.READONLY;
                    else
                    {
                        rc = bt.Pager.Begin(wrflag > 1, sqlite3TempInMemory(Ctx));
                        if (rc == RC.OK)
                            rc = newDatabase(bt);
                    }
                }

                if (rc != RC.OK)
                    unlockBtreeIfUnused(bt);
            } while ((rc & 0xFF) == RC.BUSY && bt.InTransaction == TRANS.NONE && btreeInvokeBusyHandler(bt) != 0);

            if (rc == RC.OK)
            {
                if (InTrans == TRANS.NONE)
                {
                    bt.Transactions++;
#if !OMIT_SHARED_CACHE
                    if (Sharable)
                    {
                        Debug.Assert(Lock.Btree == this && Lock.Table == 1);
                        Lock.Lock = LOCK.READ;
                        Lock.Next = bt.Lock;
                        bt.Lock = &Lock;
                    }
#endif
                }
                InTrans = (wrflag != 0 ? TRANS.WRITE : TRANS.READ);
                if (InTrans > bt.InTransaction)
                    bt.InTransaction = InTrans;
                if (wrflag != 0)
                {
                    var page1 = bt.Page1;
#if !OMIT_SHARED_CACHE
                    Debug.Assert(!bt.Writer);
                    bt.Writer = this;
                    bt.BtsFlags &= ~BTS.EXCLUSIVE;
                    if (wrflag > 1) bt.BtsFlags |= BTS.EXCLUSIVE;
#endif

                    // If the db-size header field is incorrect (as it may be if an old client has been writing the database file), update it now. Doing
                    // this sooner rather than later means the database size can safely re-read the database size from page 1 if a savepoint or transaction
                    // rollback occurs within the transaction.
                    if (bt.Pages != ConvertEx.Get4(page1.Data, 28))
                    {
                        rc = Pager.Write(page1.DBPage);
                        if (rc == RC.OK)
                            ConvertEx.Put4(page1.Data, 28, bt.Pages);
                    }
                }
            }

        trans_begun:
            if (rc == RC.OK && wrflag != 0)
            {
                // This call makes sure that the pager has the correct number of open savepoints. If the second parameter is greater than 0 and
                // the sub-journal is not already open, then it will be opened here.
                rc = sqlite3PagerOpenSavepoint(pBt.pPager, p.db.nSavepoint);
            }

            btreeIntegrity(this);
            Leave();
            return rc;
        }

        #endregion

#if false

        //#region Autovacuum

#if !SQLITE_OMIT_AUTOVACUUM

/*
** Set the pointer-map entries for all children of page pPage. Also, if
** pPage contains cells that point to overflow pages, set the pointer
** map entries for the overflow pages as well.
*/
static int setChildPtrmaps( MemPage pPage )
{
  int i;                             /* Counter variable */
  int nCell;                         /* Number of cells in page pPage */
  int rc;                            /* Return code */
  BtShared pBt = pPage.pBt;
  u8 isInitOrig = pPage.isInit;
  Pgno pgno = pPage.pgno;

  Debug.Assert( sqlite3_mutex_held( pPage.pBt.mutex ) );
  rc = btreeInitPage( pPage );
  if ( rc != SQLITE_OK )
  {
    goto set_child_ptrmaps_out;
  }
  nCell = pPage.nCell;

  for ( i = 0; i < nCell; i++ )
  {
    int pCell = findCell( pPage, i );

    ptrmapPutOvflPtr( pPage, pCell, ref rc );

    if ( 0 == pPage.leaf )
    {
      Pgno childPgno = sqlite3Get4byte( pPage.aData, pCell );
      ptrmapPut( pBt, childPgno, PTRMAP_BTREE, pgno, ref rc );
    }
  }

  if ( 0 == pPage.leaf )
  {
    Pgno childPgno = sqlite3Get4byte( pPage.aData, pPage.hdrOffset + 8 );
    ptrmapPut( pBt, childPgno, PTRMAP_BTREE, pgno, ref rc );
  }

set_child_ptrmaps_out:
  pPage.isInit = isInitOrig;
  return rc;
}

/*
** Somewhere on pPage is a pointer to page iFrom.  Modify this pointer so
** that it points to iTo. Parameter eType describes the type of pointer to
** be modified, as  follows:
**
** PTRMAP_BTREE:     pPage is a btree-page. The pointer points at a child
**                   page of pPage.
**
** PTRMAP_OVERFLOW1: pPage is a btree-page. The pointer points at an overflow
**                   page pointed to by one of the cells on pPage.
**
** PTRMAP_OVERFLOW2: pPage is an overflow-page. The pointer points at the next
**                   overflow page in the list.
*/
static int modifyPagePointer( MemPage pPage, Pgno iFrom, Pgno iTo, u8 eType )
{
  Debug.Assert( sqlite3_mutex_held( pPage.pBt.mutex ) );
  Debug.Assert( sqlite3PagerIswriteable( pPage.pDbPage ) );
  if ( eType == PTRMAP_OVERFLOW2 )
  {
    /* The pointer is always the first 4 bytes of the page in this case.  */
    if ( sqlite3Get4byte( pPage.aData ) != iFrom )
    {
      return SQLITE_CORRUPT_BKPT();
    }
    sqlite3Put4byte( pPage.aData, iTo );
  }
  else
  {
    u8 isInitOrig = pPage.isInit;
    int i;
    int nCell;

    btreeInitPage( pPage );
    nCell = pPage.nCell;

    for ( i = 0; i < nCell; i++ )
    {
      int pCell = findCell( pPage, i );
      if ( eType == PTRMAP_OVERFLOW1 )
      {
        CellInfo info = new CellInfo();
        btreeParseCellPtr( pPage, pCell, ref info );
        if ( info.Overflow != 0 )
        {
          if ( iFrom == sqlite3Get4byte( pPage.aData, pCell, info.Overflow ) )
          {
            sqlite3Put4byte( pPage.aData, pCell + info.Overflow, (int)iTo );
            break;
          }
        }
      }
      else
      {
        if ( sqlite3Get4byte( pPage.aData, pCell ) == iFrom )
        {
          sqlite3Put4byte( pPage.aData, pCell, (int)iTo );
          break;
        }
      }
    }

    if ( i == nCell )
    {
      if ( eType != PTRMAP_BTREE ||
      sqlite3Get4byte( pPage.aData, pPage.hdrOffset + 8 ) != iFrom )
      {
        return SQLITE_CORRUPT_BKPT();
      }
      sqlite3Put4byte( pPage.aData, pPage.hdrOffset + 8, iTo );
    }

    pPage.isInit = isInitOrig;
  }
  return SQLITE_OK;
}


/*
** Move the open database page pDbPage to location iFreePage in the
** database. The pDbPage reference remains valid.
**
** The isCommit flag indicates that there is no need to remember that
** the journal needs to be sync()ed before database page pDbPage.pgno
** can be written to. The caller has already promised not to write to that
** page.
*/
static int relocatePage(
BtShared pBt,           /* Btree */
MemPage pDbPage,        /* Open page to move */
u8 eType,                /* Pointer map 'type' entry for pDbPage */
Pgno iPtrPage,           /* Pointer map 'page-no' entry for pDbPage */
Pgno iFreePage,          /* The location to move pDbPage to */
int isCommit             /* isCommit flag passed to sqlite3PagerMovepage */
)
{
  MemPage pPtrPage = new MemPage();   /* The page that contains a pointer to pDbPage */
  Pgno iDbPage = pDbPage.pgno;
  Pager pPager = pBt.pPager;
  int rc;

  Debug.Assert( eType == PTRMAP_OVERFLOW2 || eType == PTRMAP_OVERFLOW1 ||
  eType == PTRMAP_BTREE || eType == PTRMAP_ROOTPAGE );
  Debug.Assert( sqlite3_mutex_held( pBt.mutex ) );
  Debug.Assert( pDbPage.pBt == pBt );

  /* Move page iDbPage from its current location to page number iFreePage */
  TRACE( "AUTOVACUUM: Moving %d to free page %d (ptr page %d type %d)\n",
  iDbPage, iFreePage, iPtrPage, eType );
  rc = sqlite3PagerMovepage( pPager, pDbPage.pDbPage, iFreePage, isCommit );
  if ( rc != SQLITE_OK )
  {
    return rc;
  }
  pDbPage.pgno = iFreePage;

  /* If pDbPage was a btree-page, then it may have child pages and/or cells
  ** that point to overflow pages. The pointer map entries for all these
  ** pages need to be changed.
  **
  ** If pDbPage is an overflow page, then the first 4 bytes may store a
  ** pointer to a subsequent overflow page. If this is the case, then
  ** the pointer map needs to be updated for the subsequent overflow page.
  */
  if ( eType == PTRMAP_BTREE || eType == PTRMAP_ROOTPAGE )
  {
    rc = setChildPtrmaps( pDbPage );
    if ( rc != SQLITE_OK )
    {
      return rc;
    }
  }
  else
  {
    Pgno nextOvfl = sqlite3Get4byte( pDbPage.aData );
    if ( nextOvfl != 0 )
    {
      ptrmapPut( pBt, nextOvfl, PTRMAP_OVERFLOW2, iFreePage, ref rc );
      if ( rc != SQLITE_OK )
      {
        return rc;
      }
    }
  }

  /* Fix the database pointer on page iPtrPage that pointed at iDbPage so
  ** that it points at iFreePage. Also fix the pointer map entry for
  ** iPtrPage.
  */
  if ( eType != PTRMAP_ROOTPAGE )
  {
    rc = btreeGetPage( pBt, iPtrPage, ref pPtrPage, 0 );
    if ( rc != SQLITE_OK )
    {
      return rc;
    }
    rc = sqlite3PagerWrite( pPtrPage.pDbPage );
    if ( rc != SQLITE_OK )
    {
      releasePage( pPtrPage );
      return rc;
    }
    rc = modifyPagePointer( pPtrPage, iDbPage, iFreePage, eType );
    releasePage( pPtrPage );
    if ( rc == SQLITE_OK )
    {
      ptrmapPut( pBt, iFreePage, eType, iPtrPage, ref rc );
    }
  }
  return rc;
}

/* Forward declaration required by incrVacuumStep(). */
//static int allocateBtreePage(BtShared *, MemPage **, Pgno *, Pgno, u8);

/*
** Perform a single step of an incremental-vacuum. If successful,
** return SQLITE_OK. If there is no work to do (and therefore no
** point in calling this function again), return SQLITE_DONE.
**
** More specificly, this function attempts to re-organize the
** database so that the last page of the file currently in use
** is no longer in use.
**
** If the nFin parameter is non-zero, this function assumes
** that the caller will keep calling incrVacuumStep() until
** it returns SQLITE_DONE or an error, and that nFin is the
** number of pages the database file will contain after this
** process is complete.  If nFin is zero, it is assumed that
** incrVacuumStep() will be called a finite amount of times
** which may or may not empty the freelist.  A full autovacuum
** has nFin>0.  A "PRAGMA incremental_vacuum" has nFin==null.
*/
static int incrVacuumStep( BtShared pBt, Pgno nFin, Pgno iLastPg )
{
  Pgno nFreeList;           /* Number of pages still on the free-list */
  int rc;

  Debug.Assert( sqlite3_mutex_held( pBt.mutex ) );
  Debug.Assert( iLastPg > nFin );

  if ( !PTRMAP_ISPAGE( pBt, iLastPg ) && iLastPg != PENDING_BYTE_PAGE( pBt ) )
  {
    u8 eType = 0;
    Pgno iPtrPage = 0;

    nFreeList = sqlite3Get4byte( pBt.pPage1.aData, 36 );
    if ( nFreeList == 0 )
    {
      return SQLITE_DONE;
    }

    rc = ptrmapGet( pBt, iLastPg, ref eType, ref iPtrPage );
    if ( rc != SQLITE_OK )
    {
      return rc;
    }
    if ( eType == PTRMAP_ROOTPAGE )
    {
      return SQLITE_CORRUPT_BKPT();
    }

    if ( eType == PTRMAP_FREEPAGE )
    {
      if ( nFin == 0 )
      {
        /* Remove the page from the files free-list. This is not required
        ** if nFin is non-zero. In that case, the free-list will be
        ** truncated to zero after this function returns, so it doesn't
        ** matter if it still contains some garbage entries.
        */
        Pgno iFreePg = 0;
        MemPage pFreePg = new MemPage();
        rc = allocateBtreePage( pBt, ref pFreePg, ref iFreePg, iLastPg, 1 );
        if ( rc != SQLITE_OK )
        {
          return rc;
        }
        Debug.Assert( iFreePg == iLastPg );
        releasePage( pFreePg );
      }
    }
    else
    {
      Pgno iFreePg = 0;             /* Index of free page to move pLastPg to */
      MemPage pLastPg = new MemPage();

      rc = btreeGetPage( pBt, iLastPg, ref pLastPg, 0 );
      if ( rc != SQLITE_OK )
      {
        return rc;
      }

      /* If nFin is zero, this loop runs exactly once and page pLastPg
      ** is swapped with the first free page pulled off the free list.
      **
      ** On the other hand, if nFin is greater than zero, then keep
      ** looping until a free-page located within the first nFin pages
      ** of the file is found.
      */
      do
      {
        MemPage pFreePg = new MemPage();
        rc = allocateBtreePage( pBt, ref pFreePg, ref iFreePg, 0, 0 );
        if ( rc != SQLITE_OK )
        {
          releasePage( pLastPg );
          return rc;
        }
        releasePage( pFreePg );
      } while ( nFin != 0 && iFreePg > nFin );
      Debug.Assert( iFreePg < iLastPg );

      rc = sqlite3PagerWrite( pLastPg.pDbPage );
      if ( rc == SQLITE_OK )
      {
        rc = relocatePage( pBt, pLastPg, eType, iPtrPage, iFreePg, ( nFin != 0 ) ? 1 : 0 );
      }
      releasePage( pLastPg );
      if ( rc != SQLITE_OK )
      {
        return rc;
      }
    }
  }

  if ( nFin == 0 )
  {
    iLastPg--;
    while ( iLastPg == PENDING_BYTE_PAGE( pBt ) || PTRMAP_ISPAGE( pBt, iLastPg ) )
    {
      if ( PTRMAP_ISPAGE( pBt, iLastPg ) )
      {
        MemPage pPg = new MemPage();
        rc = btreeGetPage( pBt, iLastPg, ref pPg, 0 );
        if ( rc != SQLITE_OK )
        {
          return rc;
        }
        rc = sqlite3PagerWrite( pPg.pDbPage );
        releasePage( pPg );
        if ( rc != SQLITE_OK )
        {
          return rc;
        }
      }
      iLastPg--;
    }
    sqlite3PagerTruncateImage( pBt.pPager, iLastPg );
    pBt.nPage = iLastPg;
  }
  return SQLITE_OK;
}

/*
** A write-transaction must be opened before calling this function.
** It performs a single unit of work towards an incremental vacuum.
**
** If the incremental vacuum is finished after this function has run,
** SQLITE_DONE is returned. If it is not finished, but no error occurred,
** SQLITE_OK is returned. Otherwise an SQLite error code.
*/
static int sqlite3BtreeIncrVacuum( Btree p )
{
  int rc;
  BtShared pBt = p.pBt;

  sqlite3BtreeEnter( p );
  Debug.Assert( pBt.inTransaction == TRANS_WRITE && p.inTrans == TRANS_WRITE );
  if ( !pBt.autoVacuum )
  {
    rc = SQLITE_DONE;
  }
  else
  {
    invalidateAllOverflowCache( pBt );
    rc = incrVacuumStep( pBt, 0, btreePagecount( pBt ) );
    if ( rc == SQLITE_OK )
    {
      rc = sqlite3PagerWrite( pBt.pPage1.pDbPage );
      sqlite3Put4byte( pBt.pPage1.aData, (u32)28, pBt.nPage );//put4byte(&pBt->pPage1->aData[28], pBt->nPage);
    }
  }
  sqlite3BtreeLeave( p );
  return rc;
}

/*
** This routine is called prior to sqlite3PagerCommit when a transaction
** is commited for an auto-vacuum database.
**
** If SQLITE_OK is returned, then pnTrunc is set to the number of pages
** the database file should be truncated to during the commit process.
** i.e. the database has been reorganized so that only the first pnTrunc
** pages are in use.
*/
static int autoVacuumCommit( BtShared pBt )
{
  int rc = SQLITE_OK;
  Pager pPager = pBt.pPager;
  // VVA_ONLY( int nRef = sqlite3PagerRefcount(pPager) );
#if !NDEBUG || DEBUG
  int nRef = sqlite3PagerRefcount( pPager );
#else
int nRef=0;
#endif


  Debug.Assert( sqlite3_mutex_held( pBt.mutex ) );
  invalidateAllOverflowCache( pBt );
  Debug.Assert( pBt.autoVacuum );
  if ( !pBt.incrVacuum )
  {
    Pgno nFin;         /* Number of pages in database after autovacuuming */
    Pgno nFree;        /* Number of pages on the freelist initially */
    Pgno nPtrmap;      /* Number of PtrMap pages to be freed */
    Pgno iFree;        /* The next page to be freed */
    int nEntry;        /* Number of entries on one ptrmap page */
    Pgno nOrig;        /* Database size before freeing */

    nOrig = btreePagecount( pBt );
    if ( PTRMAP_ISPAGE( pBt, nOrig ) || nOrig == PENDING_BYTE_PAGE( pBt ) )
    {
      /* It is not possible to create a database for which the final page
      ** is either a pointer-map page or the pending-byte page. If one
      ** is encountered, this indicates corruption.
      */
      return SQLITE_CORRUPT_BKPT();
    }

    nFree = sqlite3Get4byte( pBt.pPage1.aData, 36 );
    nEntry = (int)pBt.usableSize / 5;
    nPtrmap = (Pgno)( ( nFree - nOrig + PTRMAP_PAGENO( pBt, nOrig ) + (Pgno)nEntry ) / nEntry );
    nFin = nOrig - nFree - nPtrmap;
    if ( nOrig > PENDING_BYTE_PAGE( pBt ) && nFin < PENDING_BYTE_PAGE( pBt ) )
    {
      nFin--;
    }
    while ( PTRMAP_ISPAGE( pBt, nFin ) || nFin == PENDING_BYTE_PAGE( pBt ) )
    {
      nFin--;
    }
    if ( nFin > nOrig )
      return SQLITE_CORRUPT_BKPT();

    for ( iFree = nOrig; iFree > nFin && rc == SQLITE_OK; iFree-- )
    {
      rc = incrVacuumStep( pBt, nFin, iFree );
    }
    if ( ( rc == SQLITE_DONE || rc == SQLITE_OK ) && nFree > 0 )
    {
      rc = sqlite3PagerWrite( pBt.pPage1.pDbPage );
      sqlite3Put4byte( pBt.pPage1.aData, 32, 0 );
      sqlite3Put4byte( pBt.pPage1.aData, 36, 0 );
      sqlite3Put4byte( pBt.pPage1.aData, (u32)28, nFin );
      sqlite3PagerTruncateImage( pBt.pPager, nFin );
      pBt.nPage = nFin;
    }
    if ( rc != SQLITE_OK )
    {
      sqlite3PagerRollback( pPager );
    }
  }

  Debug.Assert( nRef == sqlite3PagerRefcount( pPager ) );
  return rc;
}

#else //* ifndef SQLITE_OMIT_AUTOVACUUM */
//# define setChildPtrmaps(x) SQLITE_OK
#endif

/*
** This routine does the first phase of a two-phase commit.  This routine
** causes a rollback journal to be created (if it does not already exist)
** and populated with enough information so that if a power loss occurs
** the database can be restored to its original state by playing back
** the journal.  Then the contents of the journal are flushed out to
** the disk.  After the journal is safely on oxide, the changes to the
** database are written into the database file and flushed to oxide.
** At the end of this call, the rollback journal still exists on the
** disk and we are still holding all locks, so the transaction has not
** committed.  See sqlite3BtreeCommitPhaseTwo() for the second phase of the
** commit process.
**
** This call is a no-op if no write-transaction is currently active on pBt.
**
** Otherwise, sync the database file for the btree pBt. zMaster points to
** the name of a master journal file that should be written into the
** individual journal file, or is NULL, indicating no master journal file
** (single database transaction).
**
** When this is called, the master journal should already have been
** created, populated with this journal pointer and synced to disk.
**
** Once this is routine has returned, the only thing required to commit
** the write-transaction for this database file is to delete the journal.
*/
static int sqlite3BtreeCommitPhaseOne( Btree p, string zMaster )
{
  int rc = SQLITE_OK;
  if ( p.inTrans == TRANS_WRITE )
  {
    BtShared pBt = p.pBt;
    sqlite3BtreeEnter( p );
#if !SQLITE_OMIT_AUTOVACUUM
    if ( pBt.autoVacuum )
    {
      rc = autoVacuumCommit( pBt );
      if ( rc != SQLITE_OK )
      {
        sqlite3BtreeLeave( p );
        return rc;
      }
    }
#endif
    rc = sqlite3PagerCommitPhaseOne( pBt.pPager, zMaster, false );
    sqlite3BtreeLeave( p );
  }
  return rc;
}

/*
** This function is called from both BtreeCommitPhaseTwo() and BtreeRollback()
** at the conclusion of a transaction.
*/
static void btreeEndTransaction( Btree p )
{
  BtShared pBt = p.pBt;
  Debug.Assert( sqlite3BtreeHoldsMutex( p ) );

  btreeClearHasContent( pBt );
  if ( p.inTrans > TRANS_NONE && p.db.activeVdbeCnt > 1 )
  {
    /* If there are other active statements that belong to this database
    ** handle, downgrade to a read-only transaction. The other statements
    ** may still be reading from the database.  */

    downgradeAllSharedCacheTableLocks( p );
    p.inTrans = TRANS_READ;
  }
  else
  {
    /* If the handle had any kind of transaction open, decrement the
    ** transaction count of the shared btree. If the transaction count
    ** reaches 0, set the shared state to TRANS_NONE. The unlockBtreeIfUnused()
    ** call below will unlock the pager.  */
    if ( p.inTrans != TRANS_NONE )
    {
      clearAllSharedCacheTableLocks( p );
      pBt.nTransaction--;
      if ( 0 == pBt.nTransaction )
      {
        pBt.inTransaction = TRANS_NONE;
      }
    }

    /* Set the current transaction state to TRANS_NONE and unlock the
    ** pager if this call closed the only read or write transaction.  */
    p.inTrans = TRANS_NONE;
    unlockBtreeIfUnused( pBt );
  }

  btreeIntegrity( p );
}

/*
** Commit the transaction currently in progress.
**
** This routine implements the second phase of a 2-phase commit.  The
** sqlite3BtreeCommitPhaseOne() routine does the first phase and should
** be invoked prior to calling this routine.  The sqlite3BtreeCommitPhaseOne()
** routine did all the work of writing information out to disk and flushing the
** contents so that they are written onto the disk platter.  All this
** routine has to do is delete or truncate or zero the header in the
** the rollback journal (which causes the transaction to commit) and
** drop locks.
**
** Normally, if an error occurs while the pager layer is attempting to 
** finalize the underlying journal file, this function returns an error and
** the upper layer will attempt a rollback. However, if the second argument
** is non-zero then this b-tree transaction is part of a multi-file 
** transaction. In this case, the transaction has already been committed 
** (by deleting a master journal file) and the caller will ignore this 
** functions return code. So, even if an error occurs in the pager layer,
** reset the b-tree objects internal state to indicate that the write
** transaction has been closed. This is quite safe, as the pager will have
** transitioned to the error state.
**
** This will release the write lock on the database file.  If there
** are no active cursors, it also releases the read lock.
*/
static int sqlite3BtreeCommitPhaseTwo( Btree p, int bCleanup)
{
  if ( p.inTrans == TRANS_NONE )
    return SQLITE_OK;
  sqlite3BtreeEnter( p );
  btreeIntegrity( p );

  /* If the handle has a write-transaction open, commit the shared-btrees
  ** transaction and set the shared state to TRANS_READ.
  */
  if ( p.inTrans == TRANS_WRITE )
  {
    int rc;
    BtShared pBt = p.pBt;
    Debug.Assert( pBt.inTransaction == TRANS_WRITE );
    Debug.Assert( pBt.nTransaction > 0 );
    rc = sqlite3PagerCommitPhaseTwo( pBt.pPager );
    if ( rc != SQLITE_OK && bCleanup == 0 )
    {
      sqlite3BtreeLeave( p );
      return rc;
    }
    pBt.inTransaction = TRANS_READ;
  }

  btreeEndTransaction( p );
  sqlite3BtreeLeave( p );
  return SQLITE_OK;
}

/*
** Do both phases of a commit.
*/
static int sqlite3BtreeCommit( Btree p )
{
  int rc;
  sqlite3BtreeEnter( p );
  rc = sqlite3BtreeCommitPhaseOne( p, null );
  if ( rc == SQLITE_OK )
  {
    rc = sqlite3BtreeCommitPhaseTwo( p, 0 );
  }
  sqlite3BtreeLeave( p );
  return rc;
}

#if !NDEBUG || DEBUG
/*
** Return the number of write-cursors open on this handle. This is for use
** in Debug.Assert() expressions, so it is only compiled if NDEBUG is not
** defined.
**
** For the purposes of this routine, a write-cursor is any cursor that
** is capable of writing to the databse.  That means the cursor was
** originally opened for writing and the cursor has not be disabled
** by having its state changed to CURSOR_FAULT.
*/
static int countWriteCursors( BtShared pBt )
{
  BtCursor pCur;
  int r = 0;
  for ( pCur = pBt.pCursor; pCur != null; pCur = pCur.pNext )
  {
    if ( pCur.wrFlag != 0 && pCur.eState != CURSOR_FAULT )
      r++;
  }
  return r;
}
#else
static int countWriteCursors(BtShared pBt) { return -1; }
#endif

/*
** This routine sets the state to CURSOR_FAULT and the error
** code to errCode for every cursor on BtShared that pBtree
** references.
**
** Every cursor is tripped, including cursors that belong
** to other database connections that happen to be sharing
** the cache with pBtree.
**
** This routine gets called when a rollback occurs.
** All cursors using the same cache must be tripped
** to prevent them from trying to use the btree after
** the rollback.  The rollback may have deleted tables
** or moved root pages, so it is not sufficient to
** save the state of the cursor.  The cursor must be
** invalidated.
*/
static void sqlite3BtreeTripAllCursors( Btree pBtree, int errCode )
{
  BtCursor p;
  sqlite3BtreeEnter( pBtree );
  for ( p = pBtree.pBt.pCursor; p != null; p = p.pNext )
  {
    int i;
    sqlite3BtreeClearCursor( p );
    p.eState = CURSOR_FAULT;
    p.skipNext = errCode;
    for ( i = 0; i <= p.iPage; i++ )
    {
      releasePage( p.apPage[i] );
      p.apPage[i] = null;
    }
  }
  sqlite3BtreeLeave( pBtree );
}

/*
** Rollback the transaction in progress.  All cursors will be
** invalided by this operation.  Any attempt to use a cursor
** that was open at the beginning of this operation will result
** in an error.
**
** This will release the write lock on the database file.  If there
** are no active cursors, it also releases the read lock.
*/
static int sqlite3BtreeRollback( Btree p )
{
  int rc;
  BtShared pBt = p.pBt;
  MemPage pPage1 = new MemPage();

  sqlite3BtreeEnter( p );
  rc = saveAllCursors( pBt, 0, null );
#if !SQLITE_OMIT_SHARED_CACHE
if( rc!=SQLITE_OK ){
/* This is a horrible situation. An IO or malloc() error occurred whilst
** trying to save cursor positions. If this is an automatic rollback (as
** the result of a constraint, malloc() failure or IO error) then
** the cache may be internally inconsistent (not contain valid trees) so
** we cannot simply return the error to the caller. Instead, abort
** all queries that may be using any of the cursors that failed to save.
*/
sqlite3BtreeTripAllCursors(p, rc);
}
#endif
  btreeIntegrity( p );

  if ( p.inTrans == TRANS_WRITE )
  {
    int rc2;

    Debug.Assert( TRANS_WRITE == pBt.inTransaction );
    rc2 = sqlite3PagerRollback( pBt.pPager );
    if ( rc2 != SQLITE_OK )
    {
      rc = rc2;
    }

    /* The rollback may have destroyed the pPage1.aData value.  So
    ** call btreeGetPage() on page 1 again to make
    ** sure pPage1.aData is set correctly. */
    if ( btreeGetPage( pBt, 1, ref pPage1, 0 ) == SQLITE_OK )
    {
      Pgno nPage = sqlite3Get4byte( pPage1.aData, 28 );
      testcase( nPage == 0 );
      if ( nPage == 0 )
        sqlite3PagerPagecount( pBt.pPager, out nPage );
      testcase( pBt.nPage != nPage );
      pBt.nPage = nPage;
      releasePage( pPage1 );
    }
    Debug.Assert( countWriteCursors( pBt ) == 0 );
    pBt.inTransaction = TRANS_READ;
  }

  btreeEndTransaction( p );
  sqlite3BtreeLeave( p );
  return rc;
}

/*
** Start a statement subtransaction. The subtransaction can can be rolled
** back independently of the main transaction. You must start a transaction
** before starting a subtransaction. The subtransaction is ended automatically
** if the main transaction commits or rolls back.
**
** Statement subtransactions are used around individual SQL statements
** that are contained within a BEGIN...COMMIT block.  If a constraint
** error occurs within the statement, the effect of that one statement
** can be rolled back without having to rollback the entire transaction.
**
** A statement sub-transaction is implemented as an anonymous savepoint. The
** value passed as the second parameter is the total number of savepoints,
** including the new anonymous savepoint, open on the B-Tree. i.e. if there
** are no active savepoints and no other statement-transactions open,
** iStatement is 1. This anonymous savepoint can be released or rolled back
** using the sqlite3BtreeSavepoint() function.
*/
static int sqlite3BtreeBeginStmt( Btree p, int iStatement )
{
  int rc;
  BtShared pBt = p.pBt;
  sqlite3BtreeEnter( p );
  Debug.Assert( p.inTrans == TRANS_WRITE );
  Debug.Assert( !pBt.readOnly );
  Debug.Assert( iStatement > 0 );
  Debug.Assert( iStatement > p.db.nSavepoint );
  Debug.Assert( pBt.inTransaction == TRANS_WRITE );
  /* At the pager level, a statement transaction is a savepoint with
  ** an index greater than all savepoints created explicitly using
  ** SQL statements. It is illegal to open, release or rollback any
  ** such savepoints while the statement transaction savepoint is active.
  */
  rc = sqlite3PagerOpenSavepoint( pBt.pPager, iStatement );
  sqlite3BtreeLeave( p );
  return rc;
}

/*
** The second argument to this function, op, is always SAVEPOINT_ROLLBACK
** or SAVEPOINT_RELEASE. This function either releases or rolls back the
** savepoint identified by parameter iSavepoint, depending on the value
** of op.
**
** Normally, iSavepoint is greater than or equal to zero. However, if op is
** SAVEPOINT_ROLLBACK, then iSavepoint may also be -1. In this case the
** contents of the entire transaction are rolled back. This is different
** from a normal transaction rollback, as no locks are released and the
** transaction remains open.
*/
static int sqlite3BtreeSavepoint( Btree p, int op, int iSavepoint )
{
  int rc = SQLITE_OK;
  if ( p != null && p.inTrans == TRANS_WRITE )
  {
    BtShared pBt = p.pBt;
    Debug.Assert( op == SAVEPOINT_RELEASE || op == SAVEPOINT_ROLLBACK );
    Debug.Assert( iSavepoint >= 0 || ( iSavepoint == -1 && op == SAVEPOINT_ROLLBACK ) );
    sqlite3BtreeEnter( p );
    rc = sqlite3PagerSavepoint( pBt.pPager, op, iSavepoint );
    if ( rc == SQLITE_OK )
    {
      if ( iSavepoint < 0 && pBt.initiallyEmpty )
        pBt.nPage = 0;
      rc = newDatabase( pBt );
      pBt.nPage = sqlite3Get4byte( pBt.pPage1.aData, 28 );
      /* The database size was written into the offset 28 of the header
      ** when the transaction started, so we know that the value at offset
      ** 28 is nonzero. */
      Debug.Assert( pBt.nPage > 0 );
    }
    sqlite3BtreeLeave( p );
  }
  return rc;
}

/*
** Create a new cursor for the BTree whose root is on the page
** iTable. If a read-only cursor is requested, it is assumed that
** the caller already has at least a read-only transaction open
** on the database already. If a write-cursor is requested, then
** the caller is assumed to have an open write transaction.
**
** If wrFlag==null, then the cursor can only be used for reading.
** If wrFlag==1, then the cursor can be used for reading or for
** writing if other conditions for writing are also met.  These
** are the conditions that must be met in order for writing to
** be allowed:
**
** 1:  The cursor must have been opened with wrFlag==1
**
** 2:  Other database connections that share the same pager cache
**     but which are not in the READ_UNCOMMITTED state may not have
**     cursors open with wrFlag==null on the same table.  Otherwise
**     the changes made by this write cursor would be visible to
**     the read cursors in the other database connection.
**
** 3:  The database must be writable (not on read-only media)
**
** 4:  There must be an active transaction.
**
** No checking is done to make sure that page iTable really is the
** root page of a b-tree.  If it is not, then the cursor acquired
** will not work correctly.
**
** It is assumed that the sqlite3BtreeCursorZero() has been called
** on pCur to initialize the memory space prior to invoking this routine.
*/
static int btreeCursor(
Btree p,                              /* The btree */
int iTable,                           /* Root page of table to open */
int wrFlag,                           /* 1 to write. 0 read-only */
KeyInfo pKeyInfo,                     /* First arg to comparison function */
BtCursor pCur                         /* Space for new cursor */
)
{
  BtShared pBt = p.pBt;                 /* Shared b-tree handle */

  Debug.Assert( sqlite3BtreeHoldsMutex( p ) );
  Debug.Assert( wrFlag == 0 || wrFlag == 1 );

  /* The following Debug.Assert statements verify that if this is a sharable
  ** b-tree database, the connection is holding the required table locks,
  ** and that no other connection has any open cursor that conflicts with
  ** this lock.  */
  Debug.Assert( hasSharedCacheTableLock( p, (u32)iTable, pKeyInfo != null ? 1 : 0, wrFlag + 1 ) );
  Debug.Assert( wrFlag == 0 || !hasReadConflicts( p, (u32)iTable ) );

  /* Assert that the caller has opened the required transaction. */
  Debug.Assert( p.inTrans > TRANS_NONE );
  Debug.Assert( wrFlag == 0 || p.inTrans == TRANS_WRITE );
  Debug.Assert( pBt.pPage1 != null && pBt.pPage1.aData != null );

  if ( NEVER( wrFlag != 0 && pBt.readOnly ) )
  {
    return SQLITE_READONLY;
  }
  if ( iTable == 1 && btreePagecount( pBt ) == 0 )
  {
    return SQLITE_EMPTY;
  }

  /* Now that no other errors can occur, finish filling in the BtCursor
  ** variables and link the cursor into the BtShared list.  */
  pCur.pgnoRoot = (Pgno)iTable;
  pCur.iPage = -1;
  pCur.pKeyInfo = pKeyInfo;
  pCur.pBtree = p;
  pCur.pBt = pBt;
  pCur.wrFlag = (u8)wrFlag;
  pCur.pNext = pBt.pCursor;
  if ( pCur.pNext != null )
  {
    pCur.pNext.pPrev = pCur;
  }
  pBt.pCursor = pCur;
  pCur.eState = CURSOR_INVALID;
  pCur.cachedRowid = 0;
  return SQLITE_OK;
}
static int sqlite3BtreeCursor(
Btree p,                                   /* The btree */
int iTable,                                /* Root page of table to open */
int wrFlag,                                /* 1 to write. 0 read-only */
KeyInfo pKeyInfo,                          /* First arg to xCompare() */
BtCursor pCur                              /* Write new cursor here */
)
{
  int rc;
  sqlite3BtreeEnter( p );
  rc = btreeCursor( p, iTable, wrFlag, pKeyInfo, pCur );
  sqlite3BtreeLeave( p );
  return rc;
}

/*
** Return the size of a BtCursor object in bytes.
**
** This interfaces is needed so that users of cursors can preallocate
** sufficient storage to hold a cursor.  The BtCursor object is opaque
** to users so they cannot do the sizeof() themselves - they must call
** this routine.
*/
static int sqlite3BtreeCursorSize()
{
  return -1; // Not Used --  return ROUND8(sizeof(BtCursor));
}

/*
** Initialize memory that will be converted into a BtCursor object.
**
** The simple approach here would be to memset() the entire object
** to zero.  But it turns out that the apPage[] and aiIdx[] arrays
** do not need to be zeroed and they are large, so we can save a lot
** of run-time by skipping the initialization of those elements.
*/
static void sqlite3BtreeCursorZero( BtCursor p )
{
  p.Clear(); // memset( p, 0, offsetof( BtCursor, iPage ) );
}

/*
** Set the cached rowid value of every cursor in the same database file
** as pCur and having the same root page number as pCur.  The value is
** set to iRowid.
**
** Only positive rowid values are considered valid for this cache.
** The cache is initialized to zero, indicating an invalid cache.
** A btree will work fine with zero or negative rowids.  We just cannot
** cache zero or negative rowids, which means tables that use zero or
** negative rowids might run a little slower.  But in practice, zero
** or negative rowids are very uncommon so this should not be a problem.
*/
static void sqlite3BtreeSetCachedRowid( BtCursor pCur, sqlite3_int64 iRowid )
{
  BtCursor p;
  for ( p = pCur.pBt.pCursor; p != null; p = p.pNext )
  {
    if ( p.pgnoRoot == pCur.pgnoRoot )
      p.cachedRowid = iRowid;
  }
  Debug.Assert( pCur.cachedRowid == iRowid );
}

/*
** Return the cached rowid for the given cursor.  A negative or zero
** return value indicates that the rowid cache is invalid and should be
** ignored.  If the rowid cache has never before been set, then a
** zero is returned.
*/
static sqlite3_int64 sqlite3BtreeGetCachedRowid( BtCursor pCur )
{
  return pCur.cachedRowid;
}

/*
** Close a cursor.  The read lock on the database file is released
** when the last cursor is closed.
*/
static int sqlite3BtreeCloseCursor( BtCursor pCur )
{
  Btree pBtree = pCur.pBtree;
  if ( pBtree != null )
  {
    int i;
    BtShared pBt = pCur.pBt;
    sqlite3BtreeEnter( pBtree );
    sqlite3BtreeClearCursor( pCur );
    if ( pCur.pPrev != null )
    {
      pCur.pPrev.pNext = pCur.pNext;
    }
    else
    {
      pBt.pCursor = pCur.pNext;
    }
    if ( pCur.pNext != null )
    {
      pCur.pNext.pPrev = pCur.pPrev;
    }
    for ( i = 0; i <= pCur.iPage; i++ )
    {
      releasePage( pCur.apPage[i] );
    }
    unlockBtreeIfUnused( pBt );
    invalidateOverflowCache( pCur );
    /* sqlite3_free(ref pCur); */
    sqlite3BtreeLeave( pBtree );
  }
  return SQLITE_OK;
}

/*
** Make sure the BtCursor* given in the argument has a valid
** BtCursor.info structure.  If it is not already valid, call
** btreeParseCell() to fill it in.
**
** BtCursor.info is a cache of the information in the current cell.
** Using this cache reduces the number of calls to btreeParseCell().
**
** 2007-06-25:  There is a bug in some versions of MSVC that cause the
** compiler to crash when getCellInfo() is implemented as a macro.
** But there is a measureable speed advantage to using the macro on gcc
** (when less compiler optimizations like -Os or -O0 are used and the
** compiler is not doing agressive inlining.)  So we use a real function
** for MSVC and a macro for everything else.  Ticket #2457.
*/
#if !NDEBUG
static void assertCellInfo( BtCursor pCur )
{
  CellInfo info;
  int iPage = pCur.iPage;
  info = new CellInfo();//memset(info, 0, sizeof(info));
  btreeParseCell( pCur.apPage[iPage], pCur.aiIdx[iPage], ref info );
  Debug.Assert( info.GetHashCode() == pCur.info.GetHashCode() || info.Equals( pCur.info ) );//memcmp(info, pCur.info, sizeof(info))==0 );
}
#else
//  #define assertCellInfo(x)
static void assertCellInfo(BtCursor pCur) { }
#endif
#if _MSC_VER
/* Use a real function in MSVC to work around bugs in that compiler. */
static void getCellInfo( BtCursor pCur )
{
  if ( pCur.info.nSize == 0 )
  {
    int iPage = pCur.iPage;
    btreeParseCell( pCur.apPage[iPage], pCur.aiIdx[iPage], ref pCur.info );
    pCur.validNKey = true;
  }
  else
  {
    assertCellInfo( pCur );
  }
}
#else //* if not _MSC_VER */
/* Use a macro in all other compilers so that the function is inlined */
//#define getCellInfo(pCur)                                                      \
//  if( pCur.info.nSize==null ){                                                   \
//    int iPage = pCur.iPage;                                                   \
//    btreeParseCell(pCur.apPage[iPage],pCur.aiIdx[iPage],&pCur.info); \
//    pCur.validNKey = true;                                                       \
//  }else{                                                                       \
//    assertCellInfo(pCur);                                                      \
//  }
#endif //* _MSC_VER */

#if !NDEBUG  //* The next routine used only within Debug.Assert() statements */
/*
** Return true if the given BtCursor is valid.  A valid cursor is one
** that is currently pointing to a row in a (non-empty) table.
** This is a verification routine is used only within Debug.Assert() statements.
*/
static bool sqlite3BtreeCursorIsValid( BtCursor pCur )
{
  return pCur != null && pCur.eState == CURSOR_VALID;
}
#else
static bool sqlite3BtreeCursorIsValid(BtCursor pCur) { return true; }
#endif //* NDEBUG */

/*
** Set pSize to the size of the buffer needed to hold the value of
** the key for the current entry.  If the cursor is not pointing
** to a valid entry, pSize is set to 0.
**
** For a table with the INTKEY flag set, this routine returns the key
** itself, not the number of bytes in the key.
**
** The caller must position the cursor prior to invoking this routine.
**
** This routine cannot fail.  It always returns SQLITE_OK.
*/
static int sqlite3BtreeKeySize( BtCursor pCur, ref i64 pSize )
{
  Debug.Assert( cursorHoldsMutex( pCur ) );
  Debug.Assert( pCur.eState == CURSOR_INVALID || pCur.eState == CURSOR_VALID );
  if ( pCur.eState != CURSOR_VALID )
  {
    pSize = 0;
  }
  else
  {
    getCellInfo( pCur );
    pSize = pCur.info.nKey;
  }
  return SQLITE_OK;
}

/*
** Set pSize to the number of bytes of data in the entry the
** cursor currently points to.
**
** The caller must guarantee that the cursor is pointing to a non-NULL
** valid entry.  In other words, the calling procedure must guarantee
** that the cursor has Cursor.eState==CURSOR_VALID.
**
** Failure is not possible.  This function always returns SQLITE_OK.
** It might just as well be a procedure (returning void) but we continue
** to return an integer result code for historical reasons.
*/
static int sqlite3BtreeDataSize( BtCursor pCur, ref u32 pSize )
{
  Debug.Assert( cursorHoldsMutex( pCur ) );
  Debug.Assert( pCur.eState == CURSOR_VALID );
  getCellInfo( pCur );
  pSize = pCur.info.nData;
  return SQLITE_OK;
}

/*
** Given the page number of an overflow page in the database (parameter
** ovfl), this function finds the page number of the next page in the
** linked list of overflow pages. If possible, it uses the auto-vacuum
** pointer-map data instead of reading the content of page ovfl to do so.
**
** If an error occurs an SQLite error code is returned. Otherwise:
**
** The page number of the next overflow page in the linked list is
** written to pPgnoNext. If page ovfl is the last page in its linked
** list, pPgnoNext is set to zero.
**
** If ppPage is not NULL, and a reference to the MemPage object corresponding
** to page number pOvfl was obtained, then ppPage is set to point to that
** reference. It is the responsibility of the caller to call releasePage()
** on ppPage to free the reference. In no reference was obtained (because
** the pointer-map was used to obtain the value for pPgnoNext), then
** ppPage is set to zero.
*/
static int getOverflowPage(
BtShared pBt,               /* The database file */
Pgno ovfl,                  /* Current overflow page number */
out MemPage ppPage,         /* OUT: MemPage handle (may be NULL) */
out Pgno pPgnoNext          /* OUT: Next overflow page number */
)
{
  Pgno next = 0;
  MemPage pPage = null;
  ppPage = null;
  int rc = SQLITE_OK;

  Debug.Assert( sqlite3_mutex_held( pBt.mutex ) );
  // Debug.Assert( pPgnoNext);

#if !SQLITE_OMIT_AUTOVACUUM
  /* Try to find the next page in the overflow list using the
** autovacuum pointer-map pages. Guess that the next page in
** the overflow list is page number (ovfl+1). If that guess turns
** out to be wrong, fall back to loading the data of page
** number ovfl to determine the next page number.
*/
  if ( pBt.autoVacuum )
  {
    Pgno pgno = 0;
    Pgno iGuess = ovfl + 1;
    u8 eType = 0;

    while ( PTRMAP_ISPAGE( pBt, iGuess ) || iGuess == PENDING_BYTE_PAGE( pBt ) )
    {
      iGuess++;
    }

    if ( iGuess <= btreePagecount( pBt ) )
    {
      rc = ptrmapGet( pBt, iGuess, ref eType, ref pgno );
      if ( rc == SQLITE_OK && eType == PTRMAP_OVERFLOW2 && pgno == ovfl )
      {
        next = iGuess;
        rc = SQLITE_DONE;
      }
    }
  }
#endif

  Debug.Assert( next == 0 || rc == SQLITE_DONE );
  if ( rc == SQLITE_OK )
  {
    rc = btreeGetPage( pBt, ovfl, ref pPage, 0 );
    Debug.Assert( rc == SQLITE_OK || pPage == null );
    if ( rc == SQLITE_OK )
    {
      next = sqlite3Get4byte( pPage.aData );
    }
  }

  pPgnoNext = next;
  if ( ppPage != null )
  {
    ppPage = pPage;
  }
  else
  {
    releasePage( pPage );
  }
  return ( rc == SQLITE_DONE ? SQLITE_OK : rc );
}

/*
** Copy data from a buffer to a page, or from a page to a buffer.
**
** pPayload is a pointer to data stored on database page pDbPage.
** If argument eOp is false, then nByte bytes of data are copied
** from pPayload to the buffer pointed at by pBuf. If eOp is true,
** then sqlite3PagerWrite() is called on pDbPage and nByte bytes
** of data are copied from the buffer pBuf to pPayload.
**
** SQLITE_OK is returned on success, otherwise an error code.
*/
static int copyPayload(
byte[] pPayload,           /* Pointer to page data */
u32 payloadOffset,         /* Offset into page data */
byte[] pBuf,               /* Pointer to buffer */
u32 pBufOffset,            /* Offset into buffer */
u32 nByte,                 /* Number of bytes to copy */
int eOp,                   /* 0 . copy from page, 1 . copy to page */
DbPage pDbPage             /* Page containing pPayload */
)
{
  if ( eOp != 0 )
  {
    /* Copy data from buffer to page (a write operation) */
    int rc = sqlite3PagerWrite( pDbPage );
    if ( rc != SQLITE_OK )
    {
      return rc;
    }
    Buffer.BlockCopy( pBuf, (int)pBufOffset, pPayload, (int)payloadOffset, (int)nByte );// memcpy( pPayload, pBuf, nByte );
  }
  else
  {
    /* Copy data from page to buffer (a read operation) */
    Buffer.BlockCopy( pPayload, (int)payloadOffset, pBuf, (int)pBufOffset, (int)nByte );//memcpy(pBuf, pPayload, nByte);
  }
  return SQLITE_OK;
}
//static int copyPayload(
//  byte[] pPayload,           /* Pointer to page data */
//  byte[] pBuf,               /* Pointer to buffer */
//  int nByte,                 /* Number of bytes to copy */
//  int eOp,                   /* 0 -> copy from page, 1 -> copy to page */
//  DbPage pDbPage             /* Page containing pPayload */
//){
//  if( eOp!=0 ){
//    /* Copy data from buffer to page (a write operation) */
//    int rc = sqlite3PagerWrite(pDbPage);
//    if( rc!=SQLITE_OK ){
//      return rc;
//    }
//    memcpy(pPayload, pBuf, nByte);
//  }else{
//    /* Copy data from page to buffer (a read operation) */
//    memcpy(pBuf, pPayload, nByte);
//  }
//  return SQLITE_OK;
//}

/*
** This function is used to read or overwrite payload information
** for the entry that the pCur cursor is pointing to. If the eOp
** parameter is 0, this is a read operation (data copied into
** buffer pBuf). If it is non-zero, a write (data copied from
** buffer pBuf).
**
** A total of "amt" bytes are read or written beginning at "offset".
** Data is read to or from the buffer pBuf.
**
** The content being read or written might appear on the main page
** or be scattered out on multiple overflow pages.
**
** If the BtCursor.isIncrblobHandle flag is set, and the current
** cursor entry uses one or more overflow pages, this function
** allocates space for and lazily popluates the overflow page-list
** cache array (BtCursor.aOverflow). Subsequent calls use this
** cache to make seeking to the supplied offset more efficient.
**
** Once an overflow page-list cache has been allocated, it may be
** invalidated if some other cursor writes to the same table, or if
** the cursor is moved to a different row. Additionally, in auto-vacuum
** mode, the following events may invalidate an overflow page-list cache.
**
**   * An incremental vacuum,
**   * A commit in auto_vacuum="full" mode,
**   * Creating a table (may require moving an overflow page).
*/
static int accessPayload(
BtCursor pCur,      /* Cursor pointing to entry to read from */
u32 offset,         /* Begin reading this far into payload */
u32 amt,            /* Read this many bytes */
byte[] pBuf,        /* Write the bytes into this buffer */
int eOp             /* zero to read. non-zero to write. */
)
{
  u32 pBufOffset = 0;
  byte[] aPayload;
  int rc = SQLITE_OK;
  u32 nKey;
  int iIdx = 0;
  MemPage pPage = pCur.apPage[pCur.iPage]; /* Btree page of current entry */
  BtShared pBt = pCur.pBt;                  /* Btree this cursor belongs to */

  Debug.Assert( pPage != null );
  Debug.Assert( pCur.eState == CURSOR_VALID );
  Debug.Assert( pCur.aiIdx[pCur.iPage] < pPage.nCell );
  Debug.Assert( cursorHoldsMutex( pCur ) );

  getCellInfo( pCur );
  aPayload = pCur.info.pCell; //pCur.info.pCell + pCur.info.nHeader;
  nKey = (u32)( pPage.intKey != 0 ? 0 : (int)pCur.info.nKey );

  if ( NEVER( offset + amt > nKey + pCur.info.nData )
  || pCur.info.nLocal > pBt.usableSize//&aPayload[pCur.info.nLocal] > &pPage.aData[pBt.usableSize]
  )
  {
    /* Trying to read or write past the end of the data is an error */
    return SQLITE_CORRUPT_BKPT();
  }

  /* Check if data must be read/written to/from the btree page itself. */
  if ( offset < pCur.info.nLocal )
  {
    int a = (int)amt;
    if ( a + offset > pCur.info.nLocal )
    {
      a = (int)( pCur.info.nLocal - offset );
    }
    rc = copyPayload( aPayload, (u32)( offset + pCur.info.iCell + pCur.info.nHeader ), pBuf, pBufOffset, (u32)a, eOp, pPage.pDbPage );
    offset = 0;
    pBufOffset += (u32)a; //pBuf += a;
    amt -= (u32)a;
  }
  else
  {
    offset -= pCur.info.nLocal;
  }

  if ( rc == SQLITE_OK && amt > 0 )
  {
    u32 ovflSize = (u32)( pBt.usableSize - 4 );  /* Bytes content per ovfl page */
    Pgno nextPage;

    nextPage = sqlite3Get4byte( aPayload, pCur.info.nLocal + pCur.info.iCell + pCur.info.nHeader );

#if !SQLITE_OMIT_INCRBLOB
/* If the isIncrblobHandle flag is set and the BtCursor.aOverflow[]
** has not been allocated, allocate it now. The array is sized at
** one entry for each overflow page in the overflow chain. The
** page number of the first overflow page is stored in aOverflow[0],
** etc. A value of 0 in the aOverflow[] array means "not yet known"
** (the cache is lazily populated).
*/
if( pCur.isIncrblobHandle && !pCur.aOverflow ){
int nOvfl = (pCur.info.nPayload-pCur.info.nLocal+ovflSize-1)/ovflSize;
pCur.aOverflow = (Pgno *)sqlite3MallocZero(sizeof(Pgno)*nOvfl);
/* nOvfl is always positive.  If it were zero, fetchPayload would have
** been used instead of this routine. */
if( ALWAYS(nOvfl) && !pCur.aOverflow ){
rc = SQLITE_NOMEM;
}
}

/* If the overflow page-list cache has been allocated and the
** entry for the first required overflow page is valid, skip
** directly to it.
*/
if( pCur.aOverflow && pCur.aOverflow[offset/ovflSize] ){
iIdx = (offset/ovflSize);
nextPage = pCur.aOverflow[iIdx];
offset = (offset%ovflSize);
}
#endif

    for ( ; rc == SQLITE_OK && amt > 0 && nextPage != 0; iIdx++ )
    {

#if !SQLITE_OMIT_INCRBLOB
/* If required, populate the overflow page-list cache. */
if( pCur.aOverflow ){
Debug.Assert(!pCur.aOverflow[iIdx] || pCur.aOverflow[iIdx]==nextPage);
pCur.aOverflow[iIdx] = nextPage;
}
#endif

      MemPage MemPageDummy = null;
      if ( offset >= ovflSize )
      {
        /* The only reason to read this page is to obtain the page
        ** number for the next page in the overflow chain. The page
        ** data is not required. So first try to lookup the overflow
        ** page-list cache, if any, then fall back to the getOverflowPage()
        ** function.
        */
#if !SQLITE_OMIT_INCRBLOB
if( pCur.aOverflow && pCur.aOverflow[iIdx+1] ){
nextPage = pCur.aOverflow[iIdx+1];
} else
#endif
        rc = getOverflowPage( pBt, nextPage, out MemPageDummy, out nextPage );
        offset -= ovflSize;
      }
      else
      {
        /* Need to read this page properly. It contains some of the
        ** range of data that is being read (eOp==null) or written (eOp!=null).
        */
        PgHdr pDbPage = new PgHdr();
        int a = (int)amt;
        rc = sqlite3PagerGet( pBt.pPager, nextPage, ref pDbPage );
        if ( rc == SQLITE_OK )
        {
          aPayload = sqlite3PagerGetData( pDbPage );
          nextPage = sqlite3Get4byte( aPayload );
          if ( a + offset > ovflSize )
          {
            a = (int)( ovflSize - offset );
          }
          rc = copyPayload( aPayload, offset + 4, pBuf, pBufOffset, (u32)a, eOp, pDbPage );
          sqlite3PagerUnref( pDbPage );
          offset = 0;
          amt -= (u32)a;
          pBufOffset += (u32)a;//pBuf += a;
        }
      }
    }
  }

  if ( rc == SQLITE_OK && amt > 0 )
  {
    return SQLITE_CORRUPT_BKPT();
  }
  return rc;
}

/*
** Read part of the key associated with cursor pCur.  Exactly
** "amt" bytes will be transfered into pBuf[].  The transfer
** begins at "offset".
**
** The caller must ensure that pCur is pointing to a valid row
** in the table.
**
** Return SQLITE_OK on success or an error code if anything goes
** wrong.  An error is returned if "offset+amt" is larger than
** the available payload.
*/
static int sqlite3BtreeKey( BtCursor pCur, u32 offset, u32 amt, byte[] pBuf )
{
  Debug.Assert( cursorHoldsMutex( pCur ) );
  Debug.Assert( pCur.eState == CURSOR_VALID );
  Debug.Assert( pCur.iPage >= 0 && pCur.apPage[pCur.iPage] != null );
  Debug.Assert( pCur.aiIdx[pCur.iPage] < pCur.apPage[pCur.iPage].nCell );
  return accessPayload( pCur, offset, amt, pBuf, 0 );
}

/*
** Read part of the data associated with cursor pCur.  Exactly
** "amt" bytes will be transfered into pBuf[].  The transfer
** begins at "offset".
**
** Return SQLITE_OK on success or an error code if anything goes
** wrong.  An error is returned if "offset+amt" is larger than
** the available payload.
*/
static int sqlite3BtreeData( BtCursor pCur, u32 offset, u32 amt, byte[] pBuf )
{
  int rc;

#if !SQLITE_OMIT_INCRBLOB
if ( pCur.eState==CURSOR_INVALID ){
return SQLITE_ABORT;
}
#endif

  Debug.Assert( cursorHoldsMutex( pCur ) );
  rc = restoreCursorPosition( pCur );
  if ( rc == SQLITE_OK )
  {
    Debug.Assert( pCur.eState == CURSOR_VALID );
    Debug.Assert( pCur.iPage >= 0 && pCur.apPage[pCur.iPage] != null );
    Debug.Assert( pCur.aiIdx[pCur.iPage] < pCur.apPage[pCur.iPage].nCell );
    rc = accessPayload( pCur, offset, amt, pBuf, 0 );
  }
  return rc;
}

/*
** Return a pointer to payload information from the entry that the
** pCur cursor is pointing to.  The pointer is to the beginning of
** the key if skipKey==null and it points to the beginning of data if
** skipKey==1.  The number of bytes of available key/data is written
** into pAmt.  If pAmt==null, then the value returned will not be
** a valid pointer.
**
** This routine is an optimization.  It is common for the entire key
** and data to fit on the local page and for there to be no overflow
** pages.  When that is so, this routine can be used to access the
** key and data without making a copy.  If the key and/or data spills
** onto overflow pages, then accessPayload() must be used to reassemble
** the key/data and copy it into a preallocated buffer.
**
** The pointer returned by this routine looks directly into the cached
** page of the database.  The data might change or move the next time
** any btree routine is called.
*/
static byte[] fetchPayload(
BtCursor pCur,   /* Cursor pointing to entry to read from */
ref int pAmt,    /* Write the number of available bytes here */
ref int outOffset, /* Offset into Buffer */
bool skipKey    /* read beginning at data if this is true */
)
{
  byte[] aPayload;
  MemPage pPage;
  u32 nKey;
  u32 nLocal;

  Debug.Assert( pCur != null && pCur.iPage >= 0 && pCur.apPage[pCur.iPage] != null );
  Debug.Assert( pCur.eState == CURSOR_VALID );
  Debug.Assert( cursorHoldsMutex( pCur ) );
  outOffset = -1;
  pPage = pCur.apPage[pCur.iPage];
  Debug.Assert( pCur.aiIdx[pCur.iPage] < pPage.nCell );
  if ( NEVER( pCur.info.nSize == 0 ) )
  {
    btreeParseCell( pCur.apPage[pCur.iPage], pCur.aiIdx[pCur.iPage],
    ref pCur.info );
  }
  //aPayload = pCur.info.pCell;
  //aPayload += pCur.info.nHeader;
  aPayload = sqlite3Malloc( pCur.info.nSize - pCur.info.nHeader );
  if ( pPage.intKey != 0 )
  {
    nKey = 0;
  }
  else
  {
    nKey = (u32)pCur.info.nKey;
  }
  if ( skipKey )
  {
    //aPayload += nKey;
    outOffset = (int)( pCur.info.iCell + pCur.info.nHeader + nKey );
    Buffer.BlockCopy( pCur.info.pCell, outOffset, aPayload, 0, (int)( pCur.info.nSize - pCur.info.nHeader - nKey ) );
    nLocal = pCur.info.nLocal - nKey;
  }
  else
  {
    outOffset = (int)( pCur.info.iCell + pCur.info.nHeader );
    Buffer.BlockCopy( pCur.info.pCell, outOffset, aPayload, 0, pCur.info.nSize - pCur.info.nHeader );
    nLocal = pCur.info.nLocal;
    Debug.Assert( nLocal <= nKey );
  }
  pAmt = (int)nLocal;
  return aPayload;
}

/*
** For the entry that cursor pCur is point to, return as
** many bytes of the key or data as are available on the local
** b-tree page.  Write the number of available bytes into pAmt.
**
** The pointer returned is ephemeral.  The key/data may move
** or be destroyed on the next call to any Btree routine,
** including calls from other threads against the same cache.
** Hence, a mutex on the BtShared should be held prior to calling
** this routine.
**
** These routines is used to get quick access to key and data
** in the common case where no overflow pages are used.
*/
static byte[] sqlite3BtreeKeyFetch( BtCursor pCur, ref int pAmt, ref int outOffset )
{
  byte[] p = null;
  Debug.Assert( sqlite3_mutex_held( pCur.pBtree.db.mutex ) );
  Debug.Assert( cursorHoldsMutex( pCur ) );
  if ( ALWAYS( pCur.eState == CURSOR_VALID ) )
  {
    p = fetchPayload( pCur, ref pAmt, ref outOffset, false );
  }
  return p;
}
static byte[] sqlite3BtreeDataFetch( BtCursor pCur, ref int pAmt, ref int outOffset )
{
  byte[] p = null;
  Debug.Assert( sqlite3_mutex_held( pCur.pBtree.db.mutex ) );
  Debug.Assert( cursorHoldsMutex( pCur ) );
  if ( ALWAYS( pCur.eState == CURSOR_VALID ) )
  {
    p = fetchPayload( pCur, ref pAmt, ref outOffset, true );
  }
  return p;
}

/*
** Move the cursor down to a new child page.  The newPgno argument is the
** page number of the child page to move to.
**
** This function returns SQLITE_CORRUPT if the page-header flags field of
** the new child page does not match the flags field of the parent (i.e.
** if an intkey page appears to be the parent of a non-intkey page, or
** vice-versa).
*/
static int moveToChild( BtCursor pCur, u32 newPgno )
{
  int rc;
  int i = pCur.iPage;
  MemPage pNewPage = new MemPage();
  BtShared pBt = pCur.pBt;

  Debug.Assert( cursorHoldsMutex( pCur ) );
  Debug.Assert( pCur.eState == CURSOR_VALID );
  Debug.Assert( pCur.iPage < BTCURSOR_MAX_DEPTH );
  if ( pCur.iPage >= ( BTCURSOR_MAX_DEPTH - 1 ) )
  {
    return SQLITE_CORRUPT_BKPT();
  }
  rc = getAndInitPage( pBt, newPgno, ref pNewPage );
  if ( rc != 0 )
    return rc;
  pCur.apPage[i + 1] = pNewPage;
  pCur.aiIdx[i + 1] = 0;
  pCur.iPage++;

  pCur.info.nSize = 0;
  pCur.validNKey = false;
  if ( pNewPage.nCell < 1 || pNewPage.intKey != pCur.apPage[i].intKey )
  {
    return SQLITE_CORRUPT_BKPT();
  }
  return SQLITE_OK;
}

#if !NDEBUG
/*
** Page pParent is an internal (non-leaf) tree page. This function
** asserts that page number iChild is the left-child if the iIdx'th
** cell in page pParent. Or, if iIdx is equal to the total number of
** cells in pParent, that page number iChild is the right-child of
** the page.
*/
static void assertParentIndex( MemPage pParent, int iIdx, Pgno iChild )
{
  Debug.Assert( iIdx <= pParent.nCell );
  if ( iIdx == pParent.nCell )
  {
    Debug.Assert( sqlite3Get4byte( pParent.aData, pParent.hdrOffset + 8 ) == iChild );
  }
  else
  {
    Debug.Assert( sqlite3Get4byte( pParent.aData, findCell( pParent, iIdx ) ) == iChild );
  }
}
#else
//#  define assertParentIndex(x,y,z)
static void assertParentIndex(MemPage pParent, int iIdx, Pgno iChild) { }
#endif

/*
** Move the cursor up to the parent page.
**
** pCur.idx is set to the cell index that contains the pointer
** to the page we are coming from.  If we are coming from the
** right-most child page then pCur.idx is set to one more than
** the largest cell index.
*/
static void moveToParent( BtCursor pCur )
{
  Debug.Assert( cursorHoldsMutex( pCur ) );
  Debug.Assert( pCur.eState == CURSOR_VALID );
  Debug.Assert( pCur.iPage > 0 );
  Debug.Assert( pCur.apPage[pCur.iPage] != null );
  assertParentIndex(
  pCur.apPage[pCur.iPage - 1],
  pCur.aiIdx[pCur.iPage - 1],
  pCur.apPage[pCur.iPage].pgno
  );
  releasePage( pCur.apPage[pCur.iPage] );
  pCur.iPage--;
  pCur.info.nSize = 0;
  pCur.validNKey = false;
}

/*
** Move the cursor to point to the root page of its b-tree structure.
**
** If the table has a virtual root page, then the cursor is moved to point
** to the virtual root page instead of the actual root page. A table has a
** virtual root page when the actual root page contains no cells and a
** single child page. This can only happen with the table rooted at page 1.
**
** If the b-tree structure is empty, the cursor state is set to
** CURSOR_INVALID. Otherwise, the cursor is set to point to the first
** cell located on the root (or virtual root) page and the cursor state
** is set to CURSOR_VALID.
**
** If this function returns successfully, it may be assumed that the
** page-header flags indicate that the [virtual] root-page is the expected
** kind of b-tree page (i.e. if when opening the cursor the caller did not
** specify a KeyInfo structure the flags byte is set to 0x05 or 0x0D,
** indicating a table b-tree, or if the caller did specify a KeyInfo
** structure the flags byte is set to 0x02 or 0x0A, indicating an index
** b-tree).
*/
static int moveToRoot( BtCursor pCur )
{
  MemPage pRoot;
  int rc = SQLITE_OK;
  Btree p = pCur.pBtree;
  BtShared pBt = p.pBt;

  Debug.Assert( cursorHoldsMutex( pCur ) );
  Debug.Assert( CURSOR_INVALID < CURSOR_REQUIRESEEK );
  Debug.Assert( CURSOR_VALID < CURSOR_REQUIRESEEK );
  Debug.Assert( CURSOR_FAULT > CURSOR_REQUIRESEEK );
  if ( pCur.eState >= CURSOR_REQUIRESEEK )
  {
    if ( pCur.eState == CURSOR_FAULT )
    {
      Debug.Assert( pCur.skipNext != SQLITE_OK );
      return pCur.skipNext;
    }
    sqlite3BtreeClearCursor( pCur );
  }

  if ( pCur.iPage >= 0 )
  {
    int i;
    for ( i = 1; i <= pCur.iPage; i++ )
    {
      releasePage( pCur.apPage[i] );
    }
    pCur.iPage = 0;
  }
  else
  {
    rc = getAndInitPage( pBt, pCur.pgnoRoot, ref pCur.apPage[0] );
    if ( rc != SQLITE_OK )
    {
      pCur.eState = CURSOR_INVALID;
      return rc;
    }
    pCur.iPage = 0;

    /* If pCur.pKeyInfo is not NULL, then the caller that opened this cursor
    ** expected to open it on an index b-tree. Otherwise, if pKeyInfo is
    ** NULL, the caller expects a table b-tree. If this is not the case,
    ** return an SQLITE_CORRUPT error.  */
    Debug.Assert( pCur.apPage[0].intKey == 1 || pCur.apPage[0].intKey == 0 );
    if ( ( pCur.pKeyInfo == null ) != ( pCur.apPage[0].intKey != 0 ) )
    {
      return SQLITE_CORRUPT_BKPT();
    }
  }

  /* Assert that the root page is of the correct type. This must be the
  ** case as the call to this function that loaded the root-page (either
  ** this call or a previous invocation) would have detected corruption
  ** if the assumption were not true, and it is not possible for the flags
  ** byte to have been modified while this cursor is holding a reference
  ** to the page.  */
  pRoot = pCur.apPage[0];
  Debug.Assert( pRoot.pgno == pCur.pgnoRoot );
  Debug.Assert( pRoot.isInit != 0 && ( pCur.pKeyInfo == null ) == ( pRoot.intKey != 0 ) );

  pCur.aiIdx[0] = 0;
  pCur.info.nSize = 0;
  pCur.atLast = 0;
  pCur.validNKey = false;

  if ( pRoot.nCell == 0 && 0 == pRoot.leaf )
  {
    Pgno subpage;
    if ( pRoot.pgno != 1 )
      return SQLITE_CORRUPT_BKPT();
    subpage = sqlite3Get4byte( pRoot.aData, pRoot.hdrOffset + 8 );
    pCur.eState = CURSOR_VALID;
    rc = moveToChild( pCur, subpage );
  }
  else
  {
    pCur.eState = ( ( pRoot.nCell > 0 ) ? CURSOR_VALID : CURSOR_INVALID );
  }
  return rc;
}

/*
** Move the cursor down to the left-most leaf entry beneath the
** entry to which it is currently pointing.
**
** The left-most leaf is the one with the smallest key - the first
** in ascending order.
*/
static int moveToLeftmost( BtCursor pCur )
{
  Pgno pgno;
  int rc = SQLITE_OK;
  MemPage pPage;

  Debug.Assert( cursorHoldsMutex( pCur ) );
  Debug.Assert( pCur.eState == CURSOR_VALID );
  while ( rc == SQLITE_OK && 0 == ( pPage = pCur.apPage[pCur.iPage] ).leaf )
  {
    Debug.Assert( pCur.aiIdx[pCur.iPage] < pPage.nCell );
    pgno = sqlite3Get4byte( pPage.aData, findCell( pPage, pCur.aiIdx[pCur.iPage] ) );
    rc = moveToChild( pCur, pgno );
  }
  return rc;
}

/*
** Move the cursor down to the right-most leaf entry beneath the
** page to which it is currently pointing.  Notice the difference
** between moveToLeftmost() and moveToRightmost().  moveToLeftmost()
** finds the left-most entry beneath the *entry* whereas moveToRightmost()
** finds the right-most entry beneath the page*.
**
** The right-most entry is the one with the largest key - the last
** key in ascending order.
*/
static int moveToRightmost( BtCursor pCur )
{
  Pgno pgno;
  int rc = SQLITE_OK;
  MemPage pPage = null;

  Debug.Assert( cursorHoldsMutex( pCur ) );
  Debug.Assert( pCur.eState == CURSOR_VALID );
  while ( rc == SQLITE_OK && 0 == ( pPage = pCur.apPage[pCur.iPage] ).leaf )
  {
    pgno = sqlite3Get4byte( pPage.aData, pPage.hdrOffset + 8 );
    pCur.aiIdx[pCur.iPage] = pPage.nCell;
    rc = moveToChild( pCur, pgno );
  }
  if ( rc == SQLITE_OK )
  {
    pCur.aiIdx[pCur.iPage] = (u16)( pPage.nCell - 1 );
    pCur.info.nSize = 0;
    pCur.validNKey = false;
  }
  return rc;
}

/* Move the cursor to the first entry in the table.  Return SQLITE_OK
** on success.  Set pRes to 0 if the cursor actually points to something
** or set pRes to 1 if the table is empty.
*/
static int sqlite3BtreeFirst( BtCursor pCur, ref int pRes )
{
  int rc;

  Debug.Assert( cursorHoldsMutex( pCur ) );
  Debug.Assert( sqlite3_mutex_held( pCur.pBtree.db.mutex ) );
  rc = moveToRoot( pCur );
  if ( rc == SQLITE_OK )
  {
    if ( pCur.eState == CURSOR_INVALID )
    {
      Debug.Assert( pCur.apPage[pCur.iPage].nCell == 0 );
      pRes = 1;
    }
    else
    {
      Debug.Assert( pCur.apPage[pCur.iPage].nCell > 0 );
      pRes = 0;
      rc = moveToLeftmost( pCur );
    }
  }
  return rc;
}

/* Move the cursor to the last entry in the table.  Return SQLITE_OK
** on success.  Set pRes to 0 if the cursor actually points to something
** or set pRes to 1 if the table is empty.
*/
static int sqlite3BtreeLast( BtCursor pCur, ref int pRes )
{
  int rc;

  Debug.Assert( cursorHoldsMutex( pCur ) );
  Debug.Assert( sqlite3_mutex_held( pCur.pBtree.db.mutex ) );

  /* If the cursor already points to the last entry, this is a no-op. */
  if ( CURSOR_VALID == pCur.eState && pCur.atLast != 0 )
  {
#if SQLITE_DEBUG
    /* This block serves to Debug.Assert() that the cursor really does point
** to the last entry in the b-tree. */
    int ii;
    for ( ii = 0; ii < pCur.iPage; ii++ )
    {
      Debug.Assert( pCur.aiIdx[ii] == pCur.apPage[ii].nCell );
    }
    Debug.Assert( pCur.aiIdx[pCur.iPage] == pCur.apPage[pCur.iPage].nCell - 1 );
    Debug.Assert( pCur.apPage[pCur.iPage].leaf != 0 );
#endif
    return SQLITE_OK;
  }

  rc = moveToRoot( pCur );
  if ( rc == SQLITE_OK )
  {
    if ( CURSOR_INVALID == pCur.eState )
    {
      Debug.Assert( pCur.apPage[pCur.iPage].nCell == 0 );
      pRes = 1;
    }
    else
    {
      Debug.Assert( pCur.eState == CURSOR_VALID );
      pRes = 0;
      rc = moveToRightmost( pCur );
      pCur.atLast = (u8)( rc == SQLITE_OK ? 1 : 0 );
    }
  }
  return rc;
}

/* Move the cursor so that it points to an entry near the key
** specified by pIdxKey or intKey.   Return a success code.
**
** For INTKEY tables, the intKey parameter is used.  pIdxKey
** must be NULL.  For index tables, pIdxKey is used and intKey
** is ignored.
**
** If an exact match is not found, then the cursor is always
** left pointing at a leaf page which would hold the entry if it
** were present.  The cursor might point to an entry that comes
** before or after the key.
**
** An integer is written into pRes which is the result of
** comparing the key with the entry to which the cursor is
** pointing.  The meaning of the integer written into
** pRes is as follows:
**
**     pRes<0      The cursor is left pointing at an entry that
**                  is smaller than intKey/pIdxKey or if the table is empty
**                  and the cursor is therefore left point to nothing.
**
**     pRes==null     The cursor is left pointing at an entry that
**                  exactly matches intKey/pIdxKey.
**
**     pRes>0      The cursor is left pointing at an entry that
**                  is larger than intKey/pIdxKey.
**
*/
static int sqlite3BtreeMovetoUnpacked(
BtCursor pCur,           /* The cursor to be moved */
UnpackedRecord pIdxKey,  /* Unpacked index key */
i64 intKey,              /* The table key */
int biasRight,           /* If true, bias the search to the high end */
ref int pRes             /* Write search results here */
)
{
  int rc;

  Debug.Assert( cursorHoldsMutex( pCur ) );
  Debug.Assert( sqlite3_mutex_held( pCur.pBtree.db.mutex ) );
  // Not needed in C# // Debug.Assert( pRes != 0 );
  Debug.Assert( ( pIdxKey == null ) == ( pCur.pKeyInfo == null ) );

  /* If the cursor is already positioned at the point we are trying
  ** to move to, then just return without doing any work */
  if ( pCur.eState == CURSOR_VALID && pCur.validNKey
  && pCur.apPage[0].intKey != 0
  )
  {
    if ( pCur.info.nKey == intKey )
    {
      pRes = 0;
      return SQLITE_OK;
    }
    if ( pCur.atLast != 0 && pCur.info.nKey < intKey )
    {
      pRes = -1;
      return SQLITE_OK;
    }
  }

  rc = moveToRoot( pCur );
  if ( rc != 0 )
  {
    return rc;
  }
  Debug.Assert( pCur.apPage[pCur.iPage] != null );
  Debug.Assert( pCur.apPage[pCur.iPage].isInit != 0 );
  Debug.Assert( pCur.apPage[pCur.iPage].nCell > 0 || pCur.eState == CURSOR_INVALID );
  if ( pCur.eState == CURSOR_INVALID )
  {
    pRes = -1;
    Debug.Assert( pCur.apPage[pCur.iPage].nCell == 0 );
    return SQLITE_OK;
  }
  Debug.Assert( pCur.apPage[0].intKey != 0 || pIdxKey != null );
  for ( ; ; )
  {
    int lwr, upr, idx;
    Pgno chldPg;
    MemPage pPage = pCur.apPage[pCur.iPage];
    int c;

    /* pPage.nCell must be greater than zero. If this is the root-page
    ** the cursor would have been INVALID above and this for(;;) loop
    ** not run. If this is not the root-page, then the moveToChild() routine
    ** would have already detected db corruption. Similarly, pPage must
    ** be the right kind (index or table) of b-tree page. Otherwise
    ** a moveToChild() or moveToRoot() call would have detected corruption.  */
    Debug.Assert( pPage.nCell > 0 );
    Debug.Assert( pPage.intKey == ( ( pIdxKey == null ) ? 1 : 0 ) );
    lwr = 0;
    upr = pPage.nCell - 1;
    if ( biasRight != 0 )
    {
      pCur.aiIdx[pCur.iPage] = (u16)( idx = upr );
    }
    else
    {
      pCur.aiIdx[pCur.iPage] = (u16)( idx = ( upr + lwr ) / 2 );
    }
    for ( ; ; )
    {
      int pCell;                        /* Pointer to current cell in pPage */

      Debug.Assert( idx == pCur.aiIdx[pCur.iPage] );
      pCur.info.nSize = 0;
      pCell = findCell( pPage, idx ) + pPage.childPtrSize;
      if ( pPage.intKey != 0 )
      {
        i64 nCellKey = 0;
        if ( pPage.hasData != 0 )
        {
          u32 Dummy0 = 0;
          pCell += getVarint32( pPage.aData, pCell, out Dummy0 );
        }
        getVarint( pPage.aData, pCell, out nCellKey );
        if ( nCellKey == intKey )
        {
          c = 0;
        }
        else if ( nCellKey < intKey )
        {
          c = -1;
        }
        else
        {
          Debug.Assert( nCellKey > intKey );
          c = +1;
        }
        pCur.validNKey = true;
        pCur.info.nKey = nCellKey;
      }
      else
      {
        /* The maximum supported page-size is 65536 bytes. This means that
        ** the maximum number of record bytes stored on an index B-Tree
        ** page is less than 16384 bytes and may be stored as a 2-byte
        ** varint. This information is used to attempt to avoid parsing
        ** the entire cell by checking for the cases where the record is
        ** stored entirely within the b-tree page by inspecting the first
        ** 2 bytes of the cell.
        */
        int nCell = pPage.aData[pCell + 0]; //pCell[0];
        if ( 0 == ( nCell & 0x80 ) && nCell <= pPage.maxLocal )
        {
          /* This branch runs if the record-size field of the cell is a
          ** single byte varint and the record fits entirely on the main
          ** b-tree page.  */
          c = sqlite3VdbeRecordCompare( nCell, pPage.aData, pCell + 1, pIdxKey ); //c = sqlite3VdbeRecordCompare( nCell, (void*)&pCell[1], pIdxKey );
        }
        else if ( 0 == ( pPage.aData[pCell + 1] & 0x80 )//!(pCell[1] & 0x80)
        && ( nCell = ( ( nCell & 0x7f ) << 7 ) + pPage.aData[pCell + 1] ) <= pPage.maxLocal//pCell[1])<=pPage.maxLocal
        )
        {
          /* The record-size field is a 2 byte varint and the record
          ** fits entirely on the main b-tree page.  */
          c = sqlite3VdbeRecordCompare( nCell, pPage.aData, pCell + 2, pIdxKey ); //c = sqlite3VdbeRecordCompare( nCell, (void*)&pCell[2], pIdxKey );
        }
        else
        {
          /* The record flows over onto one or more overflow pages. In
          ** this case the whole cell needs to be parsed, a buffer allocated
          ** and accessPayload() used to retrieve the record into the
          ** buffer before VdbeRecordCompare() can be called. */
          u8[] pCellKey;
          u8[] pCellBody = new u8[pPage.aData.Length - pCell + pPage.childPtrSize];
          Buffer.BlockCopy( pPage.aData, pCell - pPage.childPtrSize, pCellBody, 0, pCellBody.Length );//          u8 * const pCellBody = pCell - pPage->childPtrSize;
          btreeParseCellPtr( pPage, pCellBody, ref pCur.info );
          nCell = (int)pCur.info.nKey;
          pCellKey = sqlite3Malloc( nCell );
          //if ( pCellKey == null )
          //{
          //  rc = SQLITE_NOMEM;
          //  goto moveto_finish;
          //}
          rc = accessPayload( pCur, 0, (u32)nCell, pCellKey, 0 );
          if ( rc != 0 )
          {
            pCellKey = null;// sqlite3_free(ref pCellKey );
            goto moveto_finish;
          }
          c = sqlite3VdbeRecordCompare( nCell, pCellKey, pIdxKey );
          pCellKey = null;// sqlite3_free(ref pCellKey );
        }
      }
      if ( c == 0 )
      {
        if ( pPage.intKey != 0 && 0 == pPage.leaf )
        {
          lwr = idx;
          upr = lwr - 1;
          break;
        }
        else
        {
          pRes = 0;
          rc = SQLITE_OK;
          goto moveto_finish;
        }
      }
      if ( c < 0 )
      {
        lwr = idx + 1;
      }
      else
      {
        upr = idx - 1;
      }
      if ( lwr > upr )
      {
        break;
      }
      pCur.aiIdx[pCur.iPage] = (u16)( idx = ( lwr + upr ) / 2 );
    }
    Debug.Assert( lwr == upr + 1 );
    Debug.Assert( pPage.isInit != 0 );
    if ( pPage.leaf != 0 )
    {
      chldPg = 0;
    }
    else if ( lwr >= pPage.nCell )
    {
      chldPg = sqlite3Get4byte( pPage.aData, pPage.hdrOffset + 8 );
    }
    else
    {
      chldPg = sqlite3Get4byte( pPage.aData, findCell( pPage, lwr ) );
    }
    if ( chldPg == 0 )
    {
      Debug.Assert( pCur.aiIdx[pCur.iPage] < pCur.apPage[pCur.iPage].nCell );
      pRes = c;
      rc = SQLITE_OK;
      goto moveto_finish;
    }
    pCur.aiIdx[pCur.iPage] = (u16)lwr;
    pCur.info.nSize = 0;
    pCur.validNKey = false;
    rc = moveToChild( pCur, chldPg );
    if ( rc != 0 )
      goto moveto_finish;
  }
moveto_finish:
  return rc;
}


/*
** Return TRUE if the cursor is not pointing at an entry of the table.
**
** TRUE will be returned after a call to sqlite3BtreeNext() moves
** past the last entry in the table or sqlite3BtreePrev() moves past
** the first entry.  TRUE is also returned if the table is empty.
*/
static bool sqlite3BtreeEof( BtCursor pCur )
{
  /* TODO: What if the cursor is in CURSOR_REQUIRESEEK but all table entries
  ** have been deleted? This API will need to change to return an error code
  ** as well as the boolean result value.
  */
  return ( CURSOR_VALID != pCur.eState );
}

/*
** Advance the cursor to the next entry in the database.  If
** successful then set pRes=0.  If the cursor
** was already pointing to the last entry in the database before
** this routine was called, then set pRes=1.
*/
static int sqlite3BtreeNext( BtCursor pCur, ref int pRes )
{
  int rc;
  int idx;
  MemPage pPage;

  Debug.Assert( cursorHoldsMutex( pCur ) );
  rc = restoreCursorPosition( pCur );
  if ( rc != SQLITE_OK )
  {
    return rc;
  }
  // Not needed in C# // Debug.Assert( pRes != 0 );
  if ( CURSOR_INVALID == pCur.eState )
  {
    pRes = 1;
    return SQLITE_OK;
  }
  if ( pCur.skipNext > 0 )
  {
    pCur.skipNext = 0;
    pRes = 0;
    return SQLITE_OK;
  }
  pCur.skipNext = 0;

  pPage = pCur.apPage[pCur.iPage];
  idx = ++pCur.aiIdx[pCur.iPage];
  Debug.Assert( pPage.isInit != 0 );
  Debug.Assert( idx <= pPage.nCell );

  pCur.info.nSize = 0;
  pCur.validNKey = false;
  if ( idx >= pPage.nCell )
  {
    if ( 0 == pPage.leaf )
    {
      rc = moveToChild( pCur, sqlite3Get4byte( pPage.aData, pPage.hdrOffset + 8 ) );
      if ( rc != 0 )
        return rc;
      rc = moveToLeftmost( pCur );
      pRes = 0;
      return rc;
    }
    do
    {
      if ( pCur.iPage == 0 )
      {
        pRes = 1;
        pCur.eState = CURSOR_INVALID;
        return SQLITE_OK;
      }
      moveToParent( pCur );
      pPage = pCur.apPage[pCur.iPage];
    } while ( pCur.aiIdx[pCur.iPage] >= pPage.nCell );
    pRes = 0;
    if ( pPage.intKey != 0 )
    {
      rc = sqlite3BtreeNext( pCur, ref pRes );
    }
    else
    {
      rc = SQLITE_OK;
    }
    return rc;
  }
  pRes = 0;
  if ( pPage.leaf != 0 )
  {
    return SQLITE_OK;
  }
  rc = moveToLeftmost( pCur );
  return rc;
}


/*
** Step the cursor to the back to the previous entry in the database.  If
** successful then set pRes=0.  If the cursor
** was already pointing to the first entry in the database before
** this routine was called, then set pRes=1.
*/
static int sqlite3BtreePrevious( BtCursor pCur, ref int pRes )
{
  int rc;
  MemPage pPage;

  Debug.Assert( cursorHoldsMutex( pCur ) );
  rc = restoreCursorPosition( pCur );
  if ( rc != SQLITE_OK )
  {
    return rc;
  }
  pCur.atLast = 0;
  if ( CURSOR_INVALID == pCur.eState )
  {
    pRes = 1;
    return SQLITE_OK;
  }
  if ( pCur.skipNext < 0 )
  {
    pCur.skipNext = 0;
    pRes = 0;
    return SQLITE_OK;
  }
  pCur.skipNext = 0;

  pPage = pCur.apPage[pCur.iPage];
  Debug.Assert( pPage.isInit != 0 );
  if ( 0 == pPage.leaf )
  {
    int idx = pCur.aiIdx[pCur.iPage];
    rc = moveToChild( pCur, sqlite3Get4byte( pPage.aData, findCell( pPage, idx ) ) );
    if ( rc != 0 )
    {
      return rc;
    }
    rc = moveToRightmost( pCur );
  }
  else
  {
    while ( pCur.aiIdx[pCur.iPage] == 0 )
    {
      if ( pCur.iPage == 0 )
      {
        pCur.eState = CURSOR_INVALID;
        pRes = 1;
        return SQLITE_OK;
      }
      moveToParent( pCur );
    }
    pCur.info.nSize = 0;
    pCur.validNKey = false;

    pCur.aiIdx[pCur.iPage]--;
    pPage = pCur.apPage[pCur.iPage];
    if ( pPage.intKey != 0 && 0 == pPage.leaf )
    {
      rc = sqlite3BtreePrevious( pCur, ref pRes );
    }
    else
    {
      rc = SQLITE_OK;
    }
  }
  pRes = 0;
  return rc;
}

/*
** Allocate a new page from the database file.
**
** The new page is marked as dirty.  (In other words, sqlite3PagerWrite()
** has already been called on the new page.)  The new page has also
** been referenced and the calling routine is responsible for calling
** sqlite3PagerUnref() on the new page when it is done.
**
** SQLITE_OK is returned on success.  Any other return value indicates
** an error.  ppPage and pPgno are undefined in the event of an error.
** Do not invoke sqlite3PagerUnref() on ppPage if an error is returned.
**
** If the "nearby" parameter is not 0, then a (feeble) effort is made to
** locate a page close to the page number "nearby".  This can be used in an
** attempt to keep related pages close to each other in the database file,
** which in turn can make database access faster.
**
** If the "exact" parameter is not 0, and the page-number nearby exists
** anywhere on the free-list, then it is guarenteed to be returned. This
** is only used by auto-vacuum databases when allocating a new table.
*/
static int allocateBtreePage(
BtShared pBt,
ref MemPage ppPage,
ref Pgno pPgno,
Pgno nearby,
u8 exact
)
{
  MemPage pPage1;
  int rc;
  u32 n;     /* Number of pages on the freelist */
  u32 k;     /* Number of leaves on the trunk of the freelist */
  MemPage pTrunk = null;
  MemPage pPrevTrunk = null;
  Pgno mxPage;     /* Total size of the database file */

  Debug.Assert( sqlite3_mutex_held( pBt.mutex ) );
  pPage1 = pBt.pPage1;
  mxPage = btreePagecount( pBt );
  n = sqlite3Get4byte( pPage1.aData, 36 );
  testcase( n == mxPage - 1 );
  if ( n >= mxPage )
  {
    return SQLITE_CORRUPT_BKPT();
  }
  if ( n > 0 )
  {
    /* There are pages on the freelist.  Reuse one of those pages. */
    Pgno iTrunk;
    u8 searchList = 0; /* If the free-list must be searched for 'nearby' */

    /* If the 'exact' parameter was true and a query of the pointer-map
    ** shows that the page 'nearby' is somewhere on the free-list, then
    ** the entire-list will be searched for that page.
    */
#if !SQLITE_OMIT_AUTOVACUUM
    if ( exact != 0 && nearby <= mxPage )
    {
      u8 eType = 0;
      Debug.Assert( nearby > 0 );
      Debug.Assert( pBt.autoVacuum );
      u32 Dummy0 = 0;
      rc = ptrmapGet( pBt, nearby, ref eType, ref Dummy0 );
      if ( rc != 0 )
        return rc;
      if ( eType == PTRMAP_FREEPAGE )
      {
        searchList = 1;
      }
      pPgno = nearby;
    }
#endif

    /* Decrement the free-list count by 1. Set iTrunk to the index of the
** first free-list trunk page. iPrevTrunk is initially 1.
*/
    rc = sqlite3PagerWrite( pPage1.pDbPage );
    if ( rc != 0 )
      return rc;
    sqlite3Put4byte( pPage1.aData, (u32)36, n - 1 );

    /* The code within this loop is run only once if the 'searchList' variable
    ** is not true. Otherwise, it runs once for each trunk-page on the
    ** free-list until the page 'nearby' is located.
    */
    do
    {
      pPrevTrunk = pTrunk;
      if ( pPrevTrunk != null )
      {
        iTrunk = sqlite3Get4byte( pPrevTrunk.aData, 0 );
      }
      else
      {
        iTrunk = sqlite3Get4byte( pPage1.aData, 32 );
      }
      testcase( iTrunk == mxPage );
      if ( iTrunk > mxPage )
      {
        rc = SQLITE_CORRUPT_BKPT();
      }
      else
      {
        rc = btreeGetPage( pBt, iTrunk, ref pTrunk, 0 );
      }
      if ( rc != 0 )
      {
        pTrunk = null;
        goto end_allocate_page;
      }

      k = sqlite3Get4byte( pTrunk.aData, 4 ); /* # of leaves on this trunk page */
      if ( k == 0 && 0 == searchList )
      {
        /* The trunk has no leaves and the list is not being searched.
        ** So extract the trunk page itself and use it as the newly
        ** allocated page */
        Debug.Assert( pPrevTrunk == null );
        rc = sqlite3PagerWrite( pTrunk.pDbPage );
        if ( rc != 0 )
        {
          goto end_allocate_page;
        }
        pPgno = iTrunk;
        Buffer.BlockCopy( pTrunk.aData, 0, pPage1.aData, 32, 4 );//memcpy( pPage1.aData[32], ref pTrunk.aData[0], 4 );
        ppPage = pTrunk;
        pTrunk = null;
        TRACE( "ALLOCATE: %d trunk - %d free pages left\n", pPgno, n - 1 );
      }
      else if ( k > (u32)( pBt.usableSize / 4 - 2 ) )
      {
        /* Value of k is out of range.  Database corruption */
        rc = SQLITE_CORRUPT_BKPT();
        goto end_allocate_page;
#if !SQLITE_OMIT_AUTOVACUUM
      }
      else if ( searchList != 0 && nearby == iTrunk )
      {
        /* The list is being searched and this trunk page is the page
        ** to allocate, regardless of whether it has leaves.
        */
        Debug.Assert( pPgno == iTrunk );
        ppPage = pTrunk;
        searchList = 0;
        rc = sqlite3PagerWrite( pTrunk.pDbPage );
        if ( rc != 0 )
        {
          goto end_allocate_page;
        }
        if ( k == 0 )
        {
          if ( null == pPrevTrunk )
          {
            //memcpy(pPage1.aData[32], pTrunk.aData[0], 4);
            pPage1.aData[32 + 0] = pTrunk.aData[0 + 0];
            pPage1.aData[32 + 1] = pTrunk.aData[0 + 1];
            pPage1.aData[32 + 2] = pTrunk.aData[0 + 2];
            pPage1.aData[32 + 3] = pTrunk.aData[0 + 3];
          }
          else
          {
            rc = sqlite3PagerWrite( pPrevTrunk.pDbPage );
            if ( rc != SQLITE_OK )
            {
              goto end_allocate_page;
            }
            //memcpy(pPrevTrunk.aData[0], pTrunk.aData[0], 4);
            pPrevTrunk.aData[0 + 0] = pTrunk.aData[0 + 0];
            pPrevTrunk.aData[0 + 1] = pTrunk.aData[0 + 1];
            pPrevTrunk.aData[0 + 2] = pTrunk.aData[0 + 2];
            pPrevTrunk.aData[0 + 3] = pTrunk.aData[0 + 3];
          }
        }
        else
        {
          /* The trunk page is required by the caller but it contains
          ** pointers to free-list leaves. The first leaf becomes a trunk
          ** page in this case.
          */
          MemPage pNewTrunk = new MemPage();
          Pgno iNewTrunk = sqlite3Get4byte( pTrunk.aData, 8 );
          if ( iNewTrunk > mxPage )
          {
            rc = SQLITE_CORRUPT_BKPT();
            goto end_allocate_page;
          }
          testcase( iNewTrunk == mxPage );
          rc = btreeGetPage( pBt, iNewTrunk, ref pNewTrunk, 0 );
          if ( rc != SQLITE_OK )
          {
            goto end_allocate_page;
          }
          rc = sqlite3PagerWrite( pNewTrunk.pDbPage );
          if ( rc != SQLITE_OK )
          {
            releasePage( pNewTrunk );
            goto end_allocate_page;
          }
          //memcpy(pNewTrunk.aData[0], pTrunk.aData[0], 4);
          pNewTrunk.aData[0 + 0] = pTrunk.aData[0 + 0];
          pNewTrunk.aData[0 + 1] = pTrunk.aData[0 + 1];
          pNewTrunk.aData[0 + 2] = pTrunk.aData[0 + 2];
          pNewTrunk.aData[0 + 3] = pTrunk.aData[0 + 3];
          sqlite3Put4byte( pNewTrunk.aData, (u32)4, (u32)( k - 1 ) );
          Buffer.BlockCopy( pTrunk.aData, 12, pNewTrunk.aData, 8, (int)( k - 1 ) * 4 );//memcpy( pNewTrunk.aData[8], ref pTrunk.aData[12], ( k - 1 ) * 4 );
          releasePage( pNewTrunk );
          if ( null == pPrevTrunk )
          {
            Debug.Assert( sqlite3PagerIswriteable( pPage1.pDbPage ) );
            sqlite3Put4byte( pPage1.aData, (u32)32, iNewTrunk );
          }
          else
          {
            rc = sqlite3PagerWrite( pPrevTrunk.pDbPage );
            if ( rc != 0 )
            {
              goto end_allocate_page;
            }
            sqlite3Put4byte( pPrevTrunk.aData, (u32)0, iNewTrunk );
          }
        }
        pTrunk = null;
        TRACE( "ALLOCATE: %d trunk - %d free pages left\n", pPgno, n - 1 );
#endif
      }
      else if ( k > 0 )
      {
        /* Extract a leaf from the trunk */
        u32 closest;
        Pgno iPage;
        byte[] aData = pTrunk.aData;
        if ( nearby > 0 )
        {
          u32 i;
          int dist;
          closest = 0;
          dist = sqlite3AbsInt32( (int)(sqlite3Get4byte( aData, 8 ) - nearby ));
          for ( i = 1; i < k; i++ )
          {
            int d2 = sqlite3AbsInt32( (int)(sqlite3Get4byte( aData, 8 + i * 4 ) - nearby ));
            if ( d2 < dist )
            {
              closest = i;
              dist = d2;
            }
          }
        }
        else
        {
          closest = 0;
        }

        iPage = sqlite3Get4byte( aData, 8 + closest * 4 );
        testcase( iPage == mxPage );
        if ( iPage > mxPage )
        {
          rc = SQLITE_CORRUPT_BKPT();
          goto end_allocate_page;
        }
        testcase( iPage == mxPage );
        if ( 0 == searchList || iPage == nearby )
        {
          int noContent;
          pPgno = iPage;
          TRACE( "ALLOCATE: %d was leaf %d of %d on trunk %d" +
          ": %d more free pages\n",
          pPgno, closest + 1, k, pTrunk.pgno, n - 1 );
          rc = sqlite3PagerWrite( pTrunk.pDbPage );
          if ( rc != 0)
            goto end_allocate_page;
          if ( closest < k - 1 )
          {
            Buffer.BlockCopy( aData, (int)( 4 + k * 4 ), aData, 8 + (int)closest * 4, 4 );//memcpy( aData[8 + closest * 4], ref aData[4 + k * 4], 4 );
          }
          sqlite3Put4byte( aData, (u32)4, ( k - 1 ) );// sqlite3Put4byte( aData, 4, k - 1 );
          noContent = !btreeGetHasContent( pBt, pPgno ) ? 1 : 0;
          rc = btreeGetPage( pBt, pPgno, ref ppPage, noContent );
          if ( rc == SQLITE_OK )
          {
            rc = sqlite3PagerWrite( ( ppPage ).pDbPage );
            if ( rc != SQLITE_OK )
            {
              releasePage( ppPage );
            }
          }
          searchList = 0;
        }
      }
      releasePage( pPrevTrunk );
      pPrevTrunk = null;
    } while ( searchList != 0 );
  }
  else
  {
    /* There are no pages on the freelist, so create a new page at the
    ** end of the file */
    rc = sqlite3PagerWrite( pBt.pPage1.pDbPage );
    if ( rc != 0 )
      return rc;
    pBt.nPage++;
    if ( pBt.nPage == PENDING_BYTE_PAGE( pBt ) )
      pBt.nPage++;

#if !SQLITE_OMIT_AUTOVACUUM
    if ( pBt.autoVacuum && PTRMAP_ISPAGE( pBt, pBt.nPage ) )
    {
      /* If pPgno refers to a pointer-map page, allocate two new pages
      ** at the end of the file instead of one. The first allocated page
      ** becomes a new pointer-map page, the second is used by the caller.
      */
      MemPage pPg = null;
      TRACE( "ALLOCATE: %d from end of file (pointer-map page)\n", pPgno );
      Debug.Assert( pBt.nPage != PENDING_BYTE_PAGE( pBt ) );
      rc = btreeGetPage( pBt, pBt.nPage, ref pPg, 1 );
      if ( rc == SQLITE_OK )
      {
        rc = sqlite3PagerWrite( pPg.pDbPage );
        releasePage( pPg );
      }
      if ( rc != 0 )
        return rc;
      pBt.nPage++;
      if ( pBt.nPage == PENDING_BYTE_PAGE( pBt ) )
      {
        pBt.nPage++;
      }
    }
#endif
    sqlite3Put4byte( pBt.pPage1.aData, (u32)28, pBt.nPage );
    pPgno = pBt.nPage;

    Debug.Assert( pPgno != PENDING_BYTE_PAGE( pBt ) );
    rc = btreeGetPage( pBt, pPgno, ref ppPage, 1 );
    if ( rc != 0 )
      return rc;
    rc = sqlite3PagerWrite( ( ppPage ).pDbPage );
    if ( rc != SQLITE_OK )
    {
      releasePage( ppPage );
    }
    TRACE( "ALLOCATE: %d from end of file\n", pPgno );
  }

  Debug.Assert( pPgno != PENDING_BYTE_PAGE( pBt ) );

end_allocate_page:
  releasePage( pTrunk );
  releasePage( pPrevTrunk );
  if ( rc == SQLITE_OK )
  {
    if ( sqlite3PagerPageRefcount( ( ppPage ).pDbPage ) > 1 )
    {
      releasePage( ppPage );
      return SQLITE_CORRUPT_BKPT();
    }
    ( ppPage ).isInit = 0;
  }
  else
  {
    ppPage = null;
  }
  Debug.Assert( rc != SQLITE_OK || sqlite3PagerIswriteable( ( ppPage ).pDbPage ) );
  return rc;
}

/*
** This function is used to add page iPage to the database file free-list.
** It is assumed that the page is not already a part of the free-list.
**
** The value passed as the second argument to this function is optional.
** If the caller happens to have a pointer to the MemPage object
** corresponding to page iPage handy, it may pass it as the second value.
** Otherwise, it may pass NULL.
**
** If a pointer to a MemPage object is passed as the second argument,
** its reference count is not altered by this function.
*/
static int freePage2( BtShared pBt, MemPage pMemPage, Pgno iPage )
{
  MemPage pTrunk = null;                /* Free-list trunk page */
  Pgno iTrunk = 0;                      /* Page number of free-list trunk page */
  MemPage pPage1 = pBt.pPage1;          /* Local reference to page 1 */
  MemPage pPage;                        /* Page being freed. May be NULL. */
  int rc;                               /* Return Code */
  int nFree;                           /* Initial number of pages on free-list */

  Debug.Assert( sqlite3_mutex_held( pBt.mutex ) );
  Debug.Assert( iPage > 1 );
  Debug.Assert( null == pMemPage || pMemPage.pgno == iPage );

  if ( pMemPage != null )
  {
    pPage = pMemPage;
    sqlite3PagerRef( pPage.pDbPage );
  }
  else
  {
    pPage = btreePageLookup( pBt, iPage );
  }

  /* Increment the free page count on pPage1 */
  rc = sqlite3PagerWrite( pPage1.pDbPage );
  if ( rc != 0 )
    goto freepage_out;
  nFree = (int)sqlite3Get4byte( pPage1.aData, 36 );
  sqlite3Put4byte( pPage1.aData, 36, nFree + 1 );

  if ( pBt.secureDelete )
  {
    /* If the secure_delete option is enabled, then
    ** always fully overwrite deleted information with zeros.
    */
    if ( ( null == pPage && ( ( rc = btreeGetPage( pBt, iPage, ref pPage, 0 ) ) != 0 ) )
    || ( ( rc = sqlite3PagerWrite( pPage.pDbPage ) ) != 0 )
    )
    {
      goto freepage_out;
    }
    Array.Clear( pPage.aData, 0, (int)pPage.pBt.pageSize );//memset(pPage->aData, 0, pPage->pBt->pageSize);
  }

  /* If the database supports auto-vacuum, write an entry in the pointer-map
  ** to indicate that the page is free.
  */
#if !SQLITE_OMIT_AUTOVACUUM //   if ( ISAUTOVACUUM )
  if ( pBt.autoVacuum )
#else
if (false)
#endif
  {
    ptrmapPut( pBt, iPage, PTRMAP_FREEPAGE, 0, ref rc );
    if ( rc != 0 )
      goto freepage_out;
  }

  /* Now manipulate the actual database free-list structure. There are two
  ** possibilities. If the free-list is currently empty, or if the first
  ** trunk page in the free-list is full, then this page will become a
  ** new free-list trunk page. Otherwise, it will become a leaf of the
  ** first trunk page in the current free-list. This block tests if it
  ** is possible to add the page as a new free-list leaf.
  */
  if ( nFree != 0 )
  {
    u32 nLeaf;                /* Initial number of leaf cells on trunk page */

    iTrunk = sqlite3Get4byte( pPage1.aData, 32 );
    rc = btreeGetPage( pBt, iTrunk, ref pTrunk, 0 );
    if ( rc != SQLITE_OK )
    {
      goto freepage_out;
    }

    nLeaf = sqlite3Get4byte( pTrunk.aData, 4 );
    Debug.Assert( pBt.usableSize > 32 );
    if ( nLeaf > (u32)pBt.usableSize / 4 - 2 )
    {
      rc = SQLITE_CORRUPT_BKPT();
      goto freepage_out;
    }
    if ( nLeaf < (u32)pBt.usableSize / 4 - 8 )
    {
      /* In this case there is room on the trunk page to insert the page
      ** being freed as a new leaf.
      **
      ** Note that the trunk page is not really full until it contains
      ** usableSize/4 - 2 entries, not usableSize/4 - 8 entries as we have
      ** coded.  But due to a coding error in versions of SQLite prior to
      ** 3.6.0, databases with freelist trunk pages holding more than
      ** usableSize/4 - 8 entries will be reported as corrupt.  In order
      ** to maintain backwards compatibility with older versions of SQLite,
      ** we will continue to restrict the number of entries to usableSize/4 - 8
      ** for now.  At some point in the future (once everyone has upgraded
      ** to 3.6.0 or later) we should consider fixing the conditional above
      ** to read "usableSize/4-2" instead of "usableSize/4-8".
      */
      rc = sqlite3PagerWrite( pTrunk.pDbPage );
      if ( rc == SQLITE_OK )
      {
        sqlite3Put4byte( pTrunk.aData, (u32)4, nLeaf + 1 );
        sqlite3Put4byte( pTrunk.aData, (u32)8 + nLeaf * 4, iPage );
        if ( pPage != null && !pBt.secureDelete )
        {
          sqlite3PagerDontWrite( pPage.pDbPage );
        }
        rc = btreeSetHasContent( pBt, iPage );
      }
      TRACE( "FREE-PAGE: %d leaf on trunk page %d\n", iPage, pTrunk.pgno );
      goto freepage_out;
    }
  }

  /* If control flows to this point, then it was not possible to add the
  ** the page being freed as a leaf page of the first trunk in the free-list.
  ** Possibly because the free-list is empty, or possibly because the
  ** first trunk in the free-list is full. Either way, the page being freed
  ** will become the new first trunk page in the free-list.
  */
  if ( pPage == null && SQLITE_OK != ( rc = btreeGetPage( pBt, iPage, ref pPage, 0 ) ) )
  {
    goto freepage_out;
  }
  rc = sqlite3PagerWrite( pPage.pDbPage );
  if ( rc != SQLITE_OK )
  {
    goto freepage_out;
  }
  sqlite3Put4byte( pPage.aData, iTrunk );
  sqlite3Put4byte( pPage.aData, 4, 0 );
  sqlite3Put4byte( pPage1.aData, (u32)32, iPage );
  TRACE( "FREE-PAGE: %d new trunk page replacing %d\n", pPage.pgno, iTrunk );

freepage_out:
  if ( pPage != null )
  {
    pPage.isInit = 0;
  }
  releasePage( pPage );
  releasePage( pTrunk );
  return rc;
}
static void freePage( MemPage pPage, ref int pRC )
{
  if ( ( pRC ) == SQLITE_OK )
  {
    pRC = freePage2( pPage.pBt, pPage, pPage.pgno );
  }
}

/*
** Free any overflow pages associated with the given Cell.
*/
static int clearCell( MemPage pPage, int pCell )
{
  BtShared pBt = pPage.pBt;
  CellInfo info = new CellInfo();
  Pgno ovflPgno;
  int rc;
  int nOvfl;
  u32 ovflPageSize;

  Debug.Assert( sqlite3_mutex_held( pPage.pBt.mutex ) );
  btreeParseCellPtr( pPage, pCell, ref info );
  if ( info.Overflow == 0 )
  {
    return SQLITE_OK;  /* No overflow pages. Return without doing anything */
  }
  ovflPgno = sqlite3Get4byte( pPage.aData, pCell, info.Overflow );
  Debug.Assert( pBt.usableSize > 4 );
  ovflPageSize = (u16)( pBt.usableSize - 4 );
  nOvfl = (int)( ( info.Payload - info.Local + ovflPageSize - 1 ) / ovflPageSize );
  Debug.Assert( ovflPgno == 0 || nOvfl > 0 );
  while ( nOvfl-- != 0 )
  {
    Pgno iNext = 0;
    MemPage pOvfl = null;
    if ( ovflPgno < 2 || ovflPgno > btreePagecount( pBt ) )
    {
      /* 0 is not a legal page number and page 1 cannot be an
      ** overflow page. Therefore if ovflPgno<2 or past the end of the
      ** file the database must be corrupt. */
      return SQLITE_CORRUPT_BKPT();
    }
    if ( nOvfl != 0 )
    {
      rc = getOverflowPage( pBt, ovflPgno, out pOvfl, out iNext );
      if ( rc != 0 )
        return rc;
    }

    if ( ( pOvfl != null || ( ( pOvfl = btreePageLookup( pBt, ovflPgno ) ) != null ) )
    && sqlite3PagerPageRefcount( pOvfl.pDbPage ) != 1
    )
    {
      /* There is no reason any cursor should have an outstanding reference 
      ** to an overflow page belonging to a cell that is being deleted/updated.
      ** So if there exists more than one reference to this page, then it 
      ** must not really be an overflow page and the database must be corrupt. 
      ** It is helpful to detect this before calling freePage2(), as 
      ** freePage2() may zero the page contents if secure-delete mode is
      ** enabled. If this 'overflow' page happens to be a page that the
      ** caller is iterating through or using in some other way, this
      ** can be problematic.
      */
      rc = SQLITE_CORRUPT_BKPT();
    }
    else
    {
      rc = freePage2( pBt, pOvfl, ovflPgno );
    }
    if ( pOvfl != null )
    {
      sqlite3PagerUnref( pOvfl.pDbPage );
    }
    if ( rc != 0 )
      return rc;
    ovflPgno = iNext;
  }
  return SQLITE_OK;
}

/*
** Create the byte sequence used to represent a cell on page pPage
** and write that byte sequence into pCell[].  Overflow pages are
** allocated and filled in as necessary.  The calling procedure
** is responsible for making sure sufficient space has been allocated
** for pCell[].
**
** Note that pCell does not necessary need to point to the pPage.aData
** area.  pCell might point to some temporary storage.  The cell will
** be constructed in this temporary area then copied into pPage.aData
** later.
*/
static int fillInCell(
MemPage pPage,            /* The page that contains the cell */
byte[] pCell,             /* Complete text of the cell */
byte[] pKey, i64 nKey,    /* The key */
byte[] pData, int nData,  /* The data */
int nZero,                /* Extra zero bytes to append to pData */
ref int pnSize            /* Write cell size here */
)
{
  int nPayload;
  u8[] pSrc;
  int pSrcIndex = 0;
  int nSrc, n, rc;
  int spaceLeft;
  MemPage pOvfl = null;
  MemPage pToRelease = null;
  byte[] pPrior;
  int pPriorIndex = 0;
  byte[] pPayload;
  int pPayloadIndex = 0;
  BtShared pBt = pPage.pBt;
  Pgno pgnoOvfl = 0;
  int nHeader;
  CellInfo info = new CellInfo();

  Debug.Assert( sqlite3_mutex_held( pPage.pBt.mutex ) );

  /* pPage is not necessarily writeable since pCell might be auxiliary
  ** buffer space that is separate from the pPage buffer area */
  // TODO -- Determine if the following Assert is needed under c#
  //Debug.Assert( pCell < pPage.aData || pCell >= &pPage.aData[pBt.pageSize]
  //          || sqlite3PagerIswriteable(pPage.pDbPage) );

  /* Fill in the header. */
  nHeader = 0;
  if ( 0 == pPage.leaf )
  {
    nHeader += 4;
  }
  if ( pPage.hasData != 0 )
  {
    nHeader += (int)putVarint( pCell, nHeader, (int)( nData + nZero ) ); //putVarint( pCell[nHeader], nData + nZero );
  }
  else
  {
    nData = nZero = 0;
  }
  nHeader += putVarint( pCell, nHeader, (u64)nKey ); //putVarint( pCell[nHeader], *(u64*)&nKey );
  btreeParseCellPtr( pPage, pCell, ref info );
  Debug.Assert( info.Header == nHeader );
  Debug.Assert( info.nKey == nKey );
  Debug.Assert( info.Data == (u32)( nData + nZero ) );

  /* Fill in the payload */
  nPayload = nData + nZero;
  if ( pPage.intKey != 0 )
  {
    pSrc = pData;
    nSrc = nData;
    nData = 0;
  }
  else
  {
    if ( NEVER( nKey > 0x7fffffff || pKey == null ) )
    {
      return SQLITE_CORRUPT_BKPT();
    }
    nPayload += (int)nKey;
    pSrc = pKey;
    nSrc = (int)nKey;
  }
  pnSize = info.Size;
  spaceLeft = info.Local;
  //  pPayload = &pCell[nHeader];
  pPayload = pCell;
  pPayloadIndex = nHeader;
  //  pPrior = &pCell[info.iOverflow];
  pPrior = pCell;
  pPriorIndex = info.Overflow;

  while ( nPayload > 0 )
  {
    if ( spaceLeft == 0 )
    {
#if !SQLITE_OMIT_AUTOVACUUM
      Pgno pgnoPtrmap = pgnoOvfl; /* Overflow page pointer-map entry page */
      if ( pBt.autoVacuum )
      {
        do
        {
          pgnoOvfl++;
        } while (
        PTRMAP_ISPAGE( pBt, pgnoOvfl ) || pgnoOvfl == PENDING_BYTE_PAGE( pBt )
        );
      }
#endif
      rc = allocateBtreePage( pBt, ref pOvfl, ref pgnoOvfl, pgnoOvfl, 0 );
#if !SQLITE_OMIT_AUTOVACUUM
      /* If the database supports auto-vacuum, and the second or subsequent
** overflow page is being allocated, add an entry to the pointer-map
** for that page now.
**
** If this is the first overflow page, then write a partial entry
** to the pointer-map. If we write nothing to this pointer-map slot,
** then the optimistic overflow chain processing in clearCell()
** may misinterpret the uninitialised values and delete the
** wrong pages from the database.
*/
      if ( pBt.autoVacuum && rc == SQLITE_OK )
      {
        u8 eType = (u8)( pgnoPtrmap != 0 ? PTRMAP_OVERFLOW2 : PTRMAP_OVERFLOW1 );
        ptrmapPut( pBt, pgnoOvfl, eType, pgnoPtrmap, ref rc );
        if ( rc != 0 )
        {
          releasePage( pOvfl );
        }
      }
#endif
      if ( rc != 0 )
      {
        releasePage( pToRelease );
        return rc;
      }

      /* If pToRelease is not zero than pPrior points into the data area
      ** of pToRelease.  Make sure pToRelease is still writeable. */
      Debug.Assert( pToRelease == null || sqlite3PagerIswriteable( pToRelease.pDbPage ) );

      /* If pPrior is part of the data area of pPage, then make sure pPage
      ** is still writeable */
      // TODO -- Determine if the following Assert is needed under c#
      //Debug.Assert( pPrior < pPage.aData || pPrior >= &pPage.aData[pBt.pageSize]
      //      || sqlite3PagerIswriteable(pPage.pDbPage) );

      sqlite3Put4byte( pPrior, pPriorIndex, pgnoOvfl );
      releasePage( pToRelease );
      pToRelease = pOvfl;
      pPrior = pOvfl.aData;
      pPriorIndex = 0;
      sqlite3Put4byte( pPrior, 0 );
      pPayload = pOvfl.aData;
      pPayloadIndex = 4; //&pOvfl.aData[4];
      spaceLeft = (int)pBt.usableSize - 4;
    }
    n = nPayload;
    if ( n > spaceLeft )
      n = spaceLeft;

    /* If pToRelease is not zero than pPayload points into the data area
    ** of pToRelease.  Make sure pToRelease is still writeable. */
    Debug.Assert( pToRelease == null || sqlite3PagerIswriteable( pToRelease.pDbPage ) );

    /* If pPayload is part of the data area of pPage, then make sure pPage
    ** is still writeable */
    // TODO -- Determine if the following Assert is needed under c#
    //Debug.Assert( pPayload < pPage.aData || pPayload >= &pPage.aData[pBt.pageSize]
    //        || sqlite3PagerIswriteable(pPage.pDbPage) );

    if ( nSrc > 0 )
    {
      if ( n > nSrc )
        n = nSrc;
      Debug.Assert( pSrc != null );
      Buffer.BlockCopy( pSrc, pSrcIndex, pPayload, pPayloadIndex, n );//memcpy(pPayload, pSrc, n);
    }
    else
    {
      byte[] pZeroBlob = sqlite3Malloc( n ); // memset(pPayload, 0, n);
      Buffer.BlockCopy( pZeroBlob, 0, pPayload, pPayloadIndex, n );
    }
    nPayload -= n;
    pPayloadIndex += n;// pPayload += n;
    pSrcIndex += n;// pSrc += n;
    nSrc -= n;
    spaceLeft -= n;
    if ( nSrc == 0 )
    {
      nSrc = nData;
      pSrc = pData;
    }
  }
  releasePage( pToRelease );
  return SQLITE_OK;
}

/*
** Remove the i-th cell from pPage.  This routine effects pPage only.
** The cell content is not freed or deallocated.  It is assumed that
** the cell content has been copied someplace else.  This routine just
** removes the reference to the cell from pPage.
**
** "sz" must be the number of bytes in the cell.
*/
static void dropCell( MemPage pPage, int idx, int sz, ref int pRC )
{
  u32 pc;         /* Offset to cell content of cell being deleted */
  u8[] data;      /* pPage.aData */
  int ptr;        /* Used to move bytes around within data[] */
  int endPtr;     /* End of loop */
  int rc;         /* The return code */
  int hdr;        /* Beginning of the header.  0 most pages.  100 page 1 */

  if ( pRC != 0 )
    return;

  Debug.Assert( idx >= 0 && idx < pPage.nCell );
#if SQLITE_DEBUG
  Debug.Assert( sz == cellSize( pPage, idx ) );
#endif
  Debug.Assert( sqlite3PagerIswriteable( pPage.pDbPage ) );
  Debug.Assert( sqlite3_mutex_held( pPage.pBt.mutex ) );
  data = pPage.aData;
  ptr = pPage.cellOffset + 2 * idx; //ptr = &data[pPage.cellOffset + 2 * idx];
  pc = (u32)get2byte( data, ptr );
  hdr = pPage.hdrOffset;
  testcase( pc == get2byte( data, hdr + 5 ) );
  testcase( pc + sz == pPage.pBt.usableSize );
  if ( pc < (u32)get2byte( data, hdr + 5 ) || pc + sz > pPage.pBt.usableSize )
  {
    pRC = SQLITE_CORRUPT_BKPT();
    return;
  }
  rc = freeSpace( pPage, pc, sz );
  if ( rc != 0 )
  {
    pRC = rc;
    return;
  }
  //endPtr = &data[pPage->cellOffset + 2*pPage->nCell - 2];
  //assert( (SQLITE_PTR_TO_INT(ptr)&1)==0 );  /* ptr is always 2-byte aligned */
  //while( ptr<endPtr ){
  //  *(u16*)ptr = *(u16*)&ptr[2];
  //  ptr += 2;
  Buffer.BlockCopy( data, ptr + 2, data, ptr, ( pPage.nCell - 1 - idx ) * 2 );
  pPage.nCell--;
  data[pPage.hdrOffset + 3] = (byte)( pPage.nCell >> 8 );
  data[pPage.hdrOffset + 4] = (byte)( pPage.nCell ); //put2byte( data, hdr + 3, pPage.nCell );
  pPage.nFree += 2;
}

/*
** Insert a new cell on pPage at cell index "i".  pCell points to the
** content of the cell.
**
** If the cell content will fit on the page, then put it there.  If it
** will not fit, then make a copy of the cell content into pTemp if
** pTemp is not null.  Regardless of pTemp, allocate a new entry
** in pPage.aOvfl[] and make it point to the cell content (either
** in pTemp or the original pCell) and also record its index.
** Allocating a new entry in pPage.aCell[] implies that
** pPage.nOverflow is incremented.
**
** If nSkip is non-zero, then do not copy the first nSkip bytes of the
** cell. The caller will overwrite them after this function returns. If
** nSkip is non-zero, then pCell may not point to an invalid memory location
** (but pCell+nSkip is always valid).
*/
static void insertCell(
MemPage pPage,      /* Page into which we are copying */
int i,              /* New cell becomes the i-th cell of the page */
u8[] pCell,         /* Content of the new cell */
int sz,             /* Bytes of content in pCell */
u8[] pTemp,         /* Temp storage space for pCell, if needed */
Pgno iChild,        /* If non-zero, replace first 4 bytes with this value */
ref int pRC         /* Read and write return code from here */
)
{
  int idx = 0;      /* Where to write new cell content in data[] */
  int j;            /* Loop counter */
  int end;          /* First byte past the last cell pointer in data[] */
  int ins;          /* Index in data[] where new cell pointer is inserted */
  int cellOffset;   /* Address of first cell pointer in data[] */
  u8[] data;        /* The content of the whole page */
  u8 ptr;           /* Used for moving information around in data[] */
  u8 endPtr;        /* End of the loop */

  int nSkip = ( iChild != 0 ? 4 : 0 );

  if ( pRC != 0 )
    return;

  Debug.Assert( i >= 0 && i <= pPage.nCell + pPage.nOverflow );
  Debug.Assert( pPage.nCell <= MX_CELL( pPage.pBt ) && MX_CELL( pPage.pBt ) <= 10921 );
  Debug.Assert( pPage.nOverflow <= ArraySize( pPage.aOvfl ) );
  Debug.Assert( sqlite3_mutex_held( pPage.pBt.mutex ) );
  /* The cell should normally be sized correctly.  However, when moving a
  ** malformed cell from a leaf page to an interior page, if the cell size
  ** wanted to be less than 4 but got rounded up to 4 on the leaf, then size
  ** might be less than 8 (leaf-size + pointer) on the interior node.  Hence
  ** the term after the || in the following assert(). */
  Debug.Assert( sz == cellSizePtr( pPage, pCell ) || ( sz == 8 && iChild > 0 ) );
  if ( pPage.nOverflow != 0 || sz + 2 > pPage.nFree )
  {
    if ( pTemp != null )
    {
      Buffer.BlockCopy( pCell, nSkip, pTemp, nSkip, sz - nSkip );//memcpy(pTemp+nSkip, pCell+nSkip, sz-nSkip);
      pCell = pTemp;
    }
    if ( iChild != 0 )
    {
      sqlite3Put4byte( pCell, iChild );
    }
    j = pPage.nOverflow++;
    Debug.Assert( j < pPage.aOvfl.Length );//(int)(sizeof(pPage.aOvfl)/sizeof(pPage.aOvfl[0])) );
    pPage.aOvfl[j].pCell = pCell;
    pPage.aOvfl[j].idx = (u16)i;
  }
  else
  {
    int rc = sqlite3PagerWrite( pPage.pDbPage );
    if ( rc != SQLITE_OK )
    {
      pRC = rc;
      return;
    }
    Debug.Assert( sqlite3PagerIswriteable( pPage.pDbPage ) );
    data = pPage.aData;
    cellOffset = pPage.cellOffset;
    end = cellOffset + 2 * pPage.nCell;
    ins = cellOffset + 2 * i;
    rc = allocateSpace( pPage, sz, ref idx );
    if ( rc != 0 )
    {
      pRC = rc;
      return;
    }
    /* The allocateSpace() routine guarantees the following two properties
    ** if it returns success */
    Debug.Assert( idx >= end + 2 );
    Debug.Assert( idx + sz <= (int)pPage.pBt.usableSize );
    pPage.nCell++;
    pPage.nFree -= (u16)( 2 + sz );
    Buffer.BlockCopy( pCell, nSkip, data, idx + nSkip, sz - nSkip ); //memcpy( data[idx + nSkip], pCell + nSkip, sz - nSkip );
    if ( iChild != 0 )
    {
      sqlite3Put4byte( data, idx, iChild );
    }
    //ptr = &data[end];
    //endPtr = &data[ins];
    //assert( ( SQLITE_PTR_TO_INT( ptr ) & 1 ) == 0 );  /* ptr is always 2-byte aligned */
    //while ( ptr > endPtr )
    //{
    //  *(u16*)ptr = *(u16*)&ptr[-2];
    //  ptr -= 2;
    //}
    for ( j = end; j > ins; j -= 2 )
    {
      data[j + 0] = data[j - 2];
      data[j + 1] = data[j - 1];
    }
    put2byte( data, ins, idx );
    put2byte( data, pPage.hdrOffset + 3, pPage.nCell );
#if !SQLITE_OMIT_AUTOVACUUM
    if ( pPage.pBt.autoVacuum )
    {
      /* The cell may contain a pointer to an overflow page. If so, write
      ** the entry for the overflow page into the pointer map.
      */
      ptrmapPutOvflPtr( pPage, pCell, ref pRC );
    }
#endif
  }
}

/*
** Add a list of cells to a page.  The page should be initially empty.
** The cells are guaranteed to fit on the page.
*/
static void assemblePage(
MemPage pPage,    /* The page to be assemblied */
int nCell,        /* The number of cells to add to this page */
u8[] apCell,      /* Pointer to a single the cell bodies */
int[] aSize       /* Sizes of the cells bodie*/
)
{
  int i;            /* Loop counter */
  int pCellptr;     /* Address of next cell pointer */
  int cellbody;     /* Address of next cell body */
  byte[] data = pPage.aData;          /* Pointer to data for pPage */
  int hdr = pPage.hdrOffset;          /* Offset of header on pPage */
  int nUsable = (int)pPage.pBt.usableSize; /* Usable size of page */

  Debug.Assert( pPage.nOverflow == 0 );
  Debug.Assert( sqlite3_mutex_held( pPage.pBt.mutex ) );
  Debug.Assert( nCell >= 0 && nCell <= (int)MX_CELL( pPage.pBt )
        && (int)MX_CELL( pPage.pBt ) <= 10921 );

  Debug.Assert( sqlite3PagerIswriteable( pPage.pDbPage ) );

  /* Check that the page has just been zeroed by zeroPage() */
  Debug.Assert( pPage.nCell == 0 );
  Debug.Assert( get2byteNotZero( data, hdr + 5 ) == nUsable );

  pCellptr = pPage.cellOffset + nCell * 2; //data[pPage.cellOffset + nCell * 2];
  cellbody = nUsable;
  for ( i = nCell - 1; i >= 0; i-- )
  {
    u16 sz = (u16)aSize[i];
    pCellptr -= 2;
    cellbody -= sz;
    put2byte( data, pCellptr, cellbody );
    Buffer.BlockCopy( apCell, 0, data, cellbody, sz );// memcpy(&data[cellbody], apCell[i], sz);
  }
  put2byte( data, hdr + 3, nCell );
  put2byte( data, hdr + 5, cellbody );
  pPage.nFree -= (u16)( nCell * 2 + nUsable - cellbody );
  pPage.nCell = (u16)nCell;
}
static void assemblePage(
MemPage pPage,    /* The page to be assemblied */
int nCell,        /* The number of cells to add to this page */
u8[][] apCell,    /* Pointers to cell bodies */
u16[] aSize,      /* Sizes of the cells */
int offset        /* Offset into the cell bodies, for c#  */
)
{
  int i;            /* Loop counter */
  int pCellptr;      /* Address of next cell pointer */
  int cellbody;     /* Address of next cell body */
  byte[] data = pPage.aData;          /* Pointer to data for pPage */
  int hdr = pPage.hdrOffset;          /* Offset of header on pPage */
  int nUsable = (int)pPage.pBt.usableSize; /* Usable size of page */

  Debug.Assert( pPage.nOverflow == 0 );
  Debug.Assert( sqlite3_mutex_held( pPage.pBt.mutex ) );
  Debug.Assert( nCell >= 0 && nCell <= MX_CELL( pPage.pBt ) && MX_CELL( pPage.pBt ) <= 5460 );
  Debug.Assert( sqlite3PagerIswriteable( pPage.pDbPage ) );

  /* Check that the page has just been zeroed by zeroPage() */
  Debug.Assert( pPage.nCell == 0 );
  Debug.Assert( get2byte( data, hdr + 5 ) == nUsable );

  pCellptr = pPage.cellOffset + nCell * 2; //data[pPage.cellOffset + nCell * 2];
  cellbody = nUsable;
  for ( i = nCell - 1; i >= 0; i-- )
  {
    pCellptr -= 2;
    cellbody -= aSize[i + offset];
    put2byte( data, pCellptr, cellbody );
    Buffer.BlockCopy( apCell[offset + i], 0, data, cellbody, aSize[i + offset] );//          memcpy(&data[cellbody], apCell[i], aSize[i]);
  }
  put2byte( data, hdr + 3, nCell );
  put2byte( data, hdr + 5, cellbody );
  pPage.nFree -= (u16)( nCell * 2 + nUsable - cellbody );
  pPage.nCell = (u16)nCell;
}

static void assemblePage(
MemPage pPage,    /* The page to be assemblied */
int nCell,        /* The number of cells to add to this page */
u8[] apCell,      /* Pointers to cell bodies */
u16[] aSize       /* Sizes of the cells */
)
{
  int i;            /* Loop counter */
  int pCellptr;     /* Address of next cell pointer */
  int cellbody;     /* Address of next cell body */
  u8[] data = pPage.aData;             /* Pointer to data for pPage */
  int hdr = pPage.hdrOffset;           /* Offset of header on pPage */
  int nUsable = (int)pPage.pBt.usableSize; /* Usable size of page */

  Debug.Assert( pPage.nOverflow == 0 );
  Debug.Assert( sqlite3_mutex_held( pPage.pBt.mutex ) );
  Debug.Assert( nCell >= 0 && nCell <= MX_CELL( pPage.pBt ) && MX_CELL( pPage.pBt ) <= 5460 );
  Debug.Assert( sqlite3PagerIswriteable( pPage.pDbPage ) );

  /* Check that the page has just been zeroed by zeroPage() */
  Debug.Assert( pPage.nCell == 0 );
  Debug.Assert( get2byte( data, hdr + 5 ) == nUsable );

  pCellptr = pPage.cellOffset + nCell * 2; //&data[pPage.cellOffset + nCell * 2];
  cellbody = nUsable;
  for ( i = nCell - 1; i >= 0; i-- )
  {
    pCellptr -= 2;
    cellbody -= aSize[i];
    put2byte( data, pCellptr, cellbody );
    Buffer.BlockCopy( apCell, 0, data, cellbody, aSize[i] );//memcpy( data[cellbody], apCell[i], aSize[i] );
  }
  put2byte( data, hdr + 3, nCell );
  put2byte( data, hdr + 5, cellbody );
  pPage.nFree -= (u16)( nCell * 2 + nUsable - cellbody );
  pPage.nCell = (u16)nCell;
}

/*
** The following parameters determine how many adjacent pages get involved
** in a balancing operation.  NN is the number of neighbors on either side
** of the page that participate in the balancing operation.  NB is the
** total number of pages that participate, including the target page and
** NN neighbors on either side.
**
** The minimum value of NN is 1 (of course).  Increasing NN above 1
** (to 2 or 3) gives a modest improvement in SELECT and DELETE performance
** in exchange for a larger degradation in INSERT and UPDATE performance.
** The value of NN appears to give the best results overall.
*/
static int NN = 1;              /* Number of neighbors on either side of pPage */
static int NB = ( NN * 2 + 1 );   /* Total pages involved in the balance */

#if !SQLITE_OMIT_QUICKBALANCE
/*
** This version of balance() handles the common special case where
** a new entry is being inserted on the extreme right-end of the
** tree, in other words, when the new entry will become the largest
** entry in the tree.
**
** Instead of trying to balance the 3 right-most leaf pages, just add
** a new page to the right-hand side and put the one new entry in
** that page.  This leaves the right side of the tree somewhat
** unbalanced.  But odds are that we will be inserting new entries
** at the end soon afterwards so the nearly empty page will quickly
** fill up.  On average.
**
** pPage is the leaf page which is the right-most page in the tree.
** pParent is its parent.  pPage must have a single overflow entry
** which is also the right-most entry on the page.
**
** The pSpace buffer is used to store a temporary copy of the divider
** cell that will be inserted into pParent. Such a cell consists of a 4
** byte page number followed by a variable length integer. In other
** words, at most 13 bytes. Hence the pSpace buffer must be at
** least 13 bytes in size.
*/
static int balance_quick( MemPage pParent, MemPage pPage, u8[] pSpace )
{
  BtShared pBt = pPage.pBt;    /* B-Tree Database */
  MemPage pNew = new MemPage();/* Newly allocated page */
  int rc;                      /* Return Code */
  Pgno pgnoNew = 0;              /* Page number of pNew */

  Debug.Assert( sqlite3_mutex_held( pPage.pBt.mutex ) );
  Debug.Assert( sqlite3PagerIswriteable( pParent.pDbPage ) );
  Debug.Assert( pPage.nOverflow == 1 );

  /* This error condition is now caught prior to reaching this function */
  if ( pPage.nCell <= 0 )
    return SQLITE_CORRUPT_BKPT();

  /* Allocate a new page. This page will become the right-sibling of
  ** pPage. Make the parent page writable, so that the new divider cell
  ** may be inserted. If both these operations are successful, proceed.
  */
  rc = allocateBtreePage( pBt, ref pNew, ref pgnoNew, 0, 0 );

  if ( rc == SQLITE_OK )
  {

    int pOut = 4;//u8 pOut = &pSpace[4];
    u8[] pCell = pPage.aOvfl[0].pCell;
    int[] szCell = new int[1];
    szCell[0] = cellSizePtr( pPage, pCell );
    int pStop;

    Debug.Assert( sqlite3PagerIswriteable( pNew.pDbPage ) );
    Debug.Assert( pPage.aData[0] == ( PTF_INTKEY | PTF_LEAFDATA | PTF_LEAF ) );
    zeroPage( pNew, PTF_INTKEY | PTF_LEAFDATA | PTF_LEAF );
    assemblePage( pNew, 1, pCell, szCell );

    /* If this is an auto-vacuum database, update the pointer map
    ** with entries for the new page, and any pointer from the
    ** cell on the page to an overflow page. If either of these
    ** operations fails, the return code is set, but the contents
    ** of the parent page are still manipulated by thh code below.
    ** That is Ok, at this point the parent page is guaranteed to
    ** be marked as dirty. Returning an error code will cause a
    ** rollback, undoing any changes made to the parent page.
    */
#if !SQLITE_OMIT_AUTOVACUUM //   if ( ISAUTOVACUUM )
    if ( pBt.autoVacuum )
#else
if (false)
#endif
    {
      ptrmapPut( pBt, pgnoNew, PTRMAP_BTREE, pParent.pgno, ref rc );
      if ( szCell[0] > pNew.minLocal )
      {
        ptrmapPutOvflPtr( pNew, pCell, ref rc );
      }
    }

    /* Create a divider cell to insert into pParent. The divider cell
    ** consists of a 4-byte page number (the page number of pPage) and
    ** a variable length key value (which must be the same value as the
    ** largest key on pPage).
    **
    ** To find the largest key value on pPage, first find the right-most
    ** cell on pPage. The first two fields of this cell are the
    ** record-length (a variable length integer at most 32-bits in size)
    ** and the key value (a variable length integer, may have any value).
    ** The first of the while(...) loops below skips over the record-length
    ** field. The second while(...) loop copies the key value from the
    ** cell on pPage into the pSpace buffer.
    */
    int iCell = findCell( pPage, pPage.nCell - 1 ); //pCell = findCell( pPage, pPage.nCell - 1 );
    pCell = pPage.aData;
    int _pCell = iCell;
    pStop = _pCell + 9; //pStop = &pCell[9];
    while ( ( ( pCell[_pCell++] ) & 0x80 ) != 0 && _pCell < pStop )
      ; //while ( ( *( pCell++ ) & 0x80 ) && pCell < pStop ) ;
    pStop = _pCell + 9;//pStop = &pCell[9];
    while ( ( ( pSpace[pOut++] = pCell[_pCell++] ) & 0x80 ) != 0 && _pCell < pStop )
      ; //while ( ( ( *( pOut++ ) = *( pCell++ ) ) & 0x80 ) && pCell < pStop ) ;

    /* Insert the new divider cell into pParent. */
    insertCell( pParent, pParent.nCell, pSpace, pOut, //(int)(pOut-pSpace),
    null, pPage.pgno, ref rc );

    /* Set the right-child pointer of pParent to point to the new page. */
    sqlite3Put4byte( pParent.aData, pParent.hdrOffset + 8, pgnoNew );

    /* Release the reference to the new page. */
    releasePage( pNew );
  }

  return rc;
}
#endif //* SQLITE_OMIT_QUICKBALANCE */

#if FALSE
/*
** This function does not contribute anything to the operation of SQLite.
** it is sometimes activated temporarily while debugging code responsible
** for setting pointer-map entries.
*/
static int ptrmapCheckPages(MemPage **apPage, int nPage){
int i, j;
for(i=0; i<nPage; i++){
Pgno n;
u8 e;
MemPage pPage = apPage[i];
BtShared pBt = pPage.pBt;
Debug.Assert( pPage.isInit!=0 );

for(j=0; j<pPage.nCell; j++){
CellInfo info;
u8 *z;

z = findCell(pPage, j);
btreeParseCellPtr(pPage, z,  info);
if( info.iOverflow ){
Pgno ovfl = sqlite3Get4byte(z[info.iOverflow]);
ptrmapGet(pBt, ovfl, ref e, ref n);
Debug.Assert( n==pPage.pgno && e==PTRMAP_OVERFLOW1 );
}
if( 0==pPage.leaf ){
Pgno child = sqlite3Get4byte(z);
ptrmapGet(pBt, child, ref e, ref n);
Debug.Assert( n==pPage.pgno && e==PTRMAP_BTREE );
}
}
if( 0==pPage.leaf ){
Pgno child = sqlite3Get4byte(pPage.aData,pPage.hdrOffset+8]);
ptrmapGet(pBt, child, ref e, ref n);
Debug.Assert( n==pPage.pgno && e==PTRMAP_BTREE );
}
}
return 1;
}
#endif

/*
** This function is used to copy the contents of the b-tree node stored
** on page pFrom to page pTo. If page pFrom was not a leaf page, then
** the pointer-map entries for each child page are updated so that the
** parent page stored in the pointer map is page pTo. If pFrom contained
** any cells with overflow page pointers, then the corresponding pointer
** map entries are also updated so that the parent page is page pTo.
**
** If pFrom is currently carrying any overflow cells (entries in the
** MemPage.aOvfl[] array), they are not copied to pTo.
**
** Before returning, page pTo is reinitialized using btreeInitPage().
**
** The performance of this function is not critical. It is only used by
** the balance_shallower() and balance_deeper() procedures, neither of
** which are called often under normal circumstances.
*/
static void copyNodeContent( MemPage pFrom, MemPage pTo, ref int pRC )
{
  if ( ( pRC ) == SQLITE_OK )
  {
    BtShared pBt = pFrom.pBt;
    u8[] aFrom = pFrom.aData;
    u8[] aTo = pTo.aData;
    int iFromHdr = pFrom.hdrOffset;
    int iToHdr = ( ( pTo.pgno == 1 ) ? 100 : 0 );
    int rc;
    int iData;


    Debug.Assert( pFrom.isInit != 0 );
    Debug.Assert( pFrom.nFree >= iToHdr );
    Debug.Assert( get2byte( aFrom, iFromHdr + 5 ) <= (int)pBt.usableSize );

    /* Copy the b-tree node content from page pFrom to page pTo. */
    iData = get2byte( aFrom, iFromHdr + 5 );
    Buffer.BlockCopy( aFrom, iData, aTo, iData, (int)pBt.usableSize - iData );//memcpy(aTo[iData], ref aFrom[iData], pBt.usableSize-iData);
    Buffer.BlockCopy( aFrom, iFromHdr, aTo, iToHdr, pFrom.cellOffset + 2 * pFrom.nCell );//memcpy(aTo[iToHdr], ref aFrom[iFromHdr], pFrom.cellOffset + 2*pFrom.nCell);

    /* Reinitialize page pTo so that the contents of the MemPage structure
    ** match the new data. The initialization of pTo can actually fail under
    ** fairly obscure circumstances, even though it is a copy of initialized 
    ** page pFrom.
    */
    pTo.isInit = 0;
    rc = btreeInitPage( pTo );
    if ( rc != SQLITE_OK )
    {
      pRC = rc;
      return;
    }

    /* If this is an auto-vacuum database, update the pointer-map entries
    ** for any b-tree or overflow pages that pTo now contains the pointers to.
    */
#if !SQLITE_OMIT_AUTOVACUUM //   if ( ISAUTOVACUUM )
    if ( pBt.autoVacuum )
#else
if (false)
#endif
    {
      pRC = setChildPtrmaps( pTo );
    }
  }
}

/*
** This routine redistributes cells on the iParentIdx'th child of pParent
** (hereafter "the page") and up to 2 siblings so that all pages have about the
** same amount of free space. Usually a single sibling on either side of the
** page are used in the balancing, though both siblings might come from one
** side if the page is the first or last child of its parent. If the page
** has fewer than 2 siblings (something which can only happen if the page
** is a root page or a child of a root page) then all available siblings
** participate in the balancing.
**
** The number of siblings of the page might be increased or decreased by
** one or two in an effort to keep pages nearly full but not over full.
**
** Note that when this routine is called, some of the cells on the page
** might not actually be stored in MemPage.aData[]. This can happen
** if the page is overfull. This routine ensures that all cells allocated
** to the page and its siblings fit into MemPage.aData[] before returning.
**
** In the course of balancing the page and its siblings, cells may be
** inserted into or removed from the parent page (pParent). Doing so
** may cause the parent page to become overfull or underfull. If this
** happens, it is the responsibility of the caller to invoke the correct
** balancing routine to fix this problem (see the balance() routine).
**
** If this routine fails for any reason, it might leave the database
** in a corrupted state. So if this routine fails, the database should
** be rolled back.
**
** The third argument to this function, aOvflSpace, is a pointer to a
** buffer big enough to hold one page. If while inserting cells into the parent
** page (pParent) the parent page becomes overfull, this buffer is
** used to store the parent's overflow cells. Because this function inserts
** a maximum of four divider cells into the parent page, and the maximum
** size of a cell stored within an internal node is always less than 1/4
** of the page-size, the aOvflSpace[] buffer is guaranteed to be large
** enough for all overflow cells.
**
** If aOvflSpace is set to a null pointer, this function returns
** SQLITE_NOMEM.
*/

// under C#; Try to reuse Memory

static int balance_nonroot(
MemPage pParent,               /* Parent page of siblings being balanced */
int iParentIdx,                /* Index of "the page" in pParent */
u8[] aOvflSpace,               /* page-size bytes of space for parent ovfl */
int isRoot                     /* True if pParent is a root-page */
)
{
  MemPage[] apOld = new MemPage[NB];    /* pPage and up to two siblings */
  MemPage[] apCopy = new MemPage[NB];   /* Private copies of apOld[] pages */
  MemPage[] apNew = new MemPage[NB + 2];/* pPage and up to NB siblings after balancing */
  int[] apDiv = new int[NB - 1];        /* Divider cells in pParent */
  int[] cntNew = new int[NB + 2];       /* Index in aCell[] of cell after i-th page */
  int[] szNew = new int[NB + 2];        /* Combined size of cells place on i-th page */
  u16[] szCell = new u16[1];            /* Local size of all cells in apCell[] */
  BtShared pBt;                /* The whole database */
  int nCell = 0;               /* Number of cells in apCell[] */
  int nMaxCells = 0;           /* Allocated size of apCell, szCell, aFrom. */
  int nNew = 0;                /* Number of pages in apNew[] */
  int nOld;                    /* Number of pages in apOld[] */
  int i, j, k;                 /* Loop counters */
  int nxDiv;                   /* Next divider slot in pParent.aCell[] */
  int rc = SQLITE_OK;          /* The return code */
  u16 leafCorrection;          /* 4 if pPage is a leaf.  0 if not */
  int leafData;                /* True if pPage is a leaf of a LEAFDATA tree */
  int usableSpace;             /* Bytes in pPage beyond the header */
  int pageFlags;               /* Value of pPage.aData[0] */
  int subtotal;                /* Subtotal of bytes in cells on one page */
  //int iSpace1 = 0;             /* First unused byte of aSpace1[] */
  int iOvflSpace = 0;          /* First unused byte of aOvflSpace[] */
  int szScratch;               /* Size of scratch memory requested */
  int pRight;                  /* Location in parent of right-sibling pointer */
  u8[][] apCell = null;                 /* All cells begin balanced */
  //u16[] szCell;                         /* Local size of all cells in apCell[] */
  //u8[] aSpace1;                         /* Space for copies of dividers cells */
  Pgno pgno;                   /* Temp var to store a page number in */

  pBt = pParent.pBt;
  Debug.Assert( sqlite3_mutex_held( pBt.mutex ) );
  Debug.Assert( sqlite3PagerIswriteable( pParent.pDbPage ) );

#if FALSE
TRACE("BALANCE: begin page %d child of %d\n", pPage.pgno, pParent.pgno);
#endif

  /* At this point pParent may have at most one overflow cell. And if
** this overflow cell is present, it must be the cell with
** index iParentIdx. This scenario comes about when this function
** is called (indirectly) from sqlite3BtreeDelete().
*/
  Debug.Assert( pParent.nOverflow == 0 || pParent.nOverflow == 1 );
  Debug.Assert( pParent.nOverflow == 0 || pParent.aOvfl[0].idx == iParentIdx );

  //if( !aOvflSpace ){
  //  return SQLITE_NOMEM;
  //}

  /* Find the sibling pages to balance. Also locate the cells in pParent
  ** that divide the siblings. An attempt is made to find NN siblings on
  ** either side of pPage. More siblings are taken from one side, however,
  ** if there are fewer than NN siblings on the other side. If pParent
  ** has NB or fewer children then all children of pParent are taken.
  **
  ** This loop also drops the divider cells from the parent page. This
  ** way, the remainder of the function does not have to deal with any
  ** overflow cells in the parent page, since if any existed they will
  ** have already been removed.
  */
  i = pParent.nOverflow + pParent.nCell;
  if ( i < 2 )
  {
    nxDiv = 0;
    nOld = i + 1;
  }
  else
  {
    nOld = 3;
    if ( iParentIdx == 0 )
    {
      nxDiv = 0;
    }
    else if ( iParentIdx == i )
    {
      nxDiv = i - 2;
    }
    else
    {
      nxDiv = iParentIdx - 1;
    }
    i = 2;
  }
  if ( ( i + nxDiv - pParent.nOverflow ) == pParent.nCell )
  {
    pRight = pParent.hdrOffset + 8; //&pParent.aData[pParent.hdrOffset + 8];
  }
  else
  {
    pRight = findCell( pParent, i + nxDiv - pParent.nOverflow );
  }
  pgno = sqlite3Get4byte( pParent.aData, pRight );
  while ( true )
  {
    rc = getAndInitPage( pBt, pgno, ref apOld[i] );
    if ( rc != 0 )
    {
      //memset(apOld, 0, (i+1)*sizeof(MemPage*));
      goto balance_cleanup;
    }
    nMaxCells += 1 + apOld[i].nCell + apOld[i].nOverflow;
    if ( ( i-- ) == 0 )
      break;

    if ( i + nxDiv == pParent.aOvfl[0].idx && pParent.nOverflow != 0 )
    {
      apDiv[i] = 0;// = pParent.aOvfl[0].pCell;
      pgno = sqlite3Get4byte( pParent.aOvfl[0].pCell, apDiv[i] );
      szNew[i] = cellSizePtr( pParent, apDiv[i] );
      pParent.nOverflow = 0;
    }
    else
    {
      apDiv[i] = findCell( pParent, i + nxDiv - pParent.nOverflow );
      pgno = sqlite3Get4byte( pParent.aData, apDiv[i] );
      szNew[i] = cellSizePtr( pParent, apDiv[i] );

      /* Drop the cell from the parent page. apDiv[i] still points to
      ** the cell within the parent, even though it has been dropped.
      ** This is safe because dropping a cell only overwrites the first
      ** four bytes of it, and this function does not need the first
      ** four bytes of the divider cell. So the pointer is safe to use
      ** later on.
      **
      ** Unless SQLite is compiled in secure-delete mode. In this case,
      ** the dropCell() routine will overwrite the entire cell with zeroes.
      ** In this case, temporarily copy the cell into the aOvflSpace[]
      ** buffer. It will be copied out again as soon as the aSpace[] buffer
      ** is allocated.  */
      //if (pBt.secureDelete)
      //{
      //  int iOff = (int)(apDiv[i]) - (int)(pParent.aData); //SQLITE_PTR_TO_INT(apDiv[i]) - SQLITE_PTR_TO_INT(pParent.aData);
      //         if( (iOff+szNew[i])>(int)pBt->usableSize )
      //  {
      //    rc = SQLITE_CORRUPT_BKPT();
      //    Array.Clear(apOld[0].aData,0,apOld[0].aData.Length); //memset(apOld, 0, (i + 1) * sizeof(MemPage*));
      //    goto balance_cleanup;
      //  }
      //  else
      //  {
      //    memcpy(&aOvflSpace[iOff], apDiv[i], szNew[i]);
      //    apDiv[i] = &aOvflSpace[apDiv[i] - pParent.aData];
      //  }
      //}
      dropCell( pParent, i + nxDiv - pParent.nOverflow, szNew[i], ref rc );
    }
  }

  /* Make nMaxCells a multiple of 4 in order to preserve 8-byte
  ** alignment */
  nMaxCells = ( nMaxCells + 3 ) & ~3;

  /*
  ** Allocate space for memory structures
  */
  //k = pBt.pageSize + ROUND8(sizeof(MemPage));
  //szScratch =
  //     nMaxCells*sizeof(u8*)                       /* apCell */
  //   + nMaxCells*sizeof(u16)                       /* szCell */
  //   + pBt.pageSize                               /* aSpace1 */
  //   + k*nOld;                                     /* Page copies (apCopy) */
  apCell = sqlite3ScratchMalloc( apCell, nMaxCells );
  //if( apCell==null ){
  //  rc = SQLITE_NOMEM;
  //  goto balance_cleanup;
  //}
  if ( szCell.Length < nMaxCells )
    Array.Resize( ref szCell, nMaxCells ); //(u16*)&apCell[nMaxCells];
  //aSpace1 = new byte[pBt.pageSize * (nMaxCells)];//  aSpace1 = (u8*)&szCell[nMaxCells];
  //Debug.Assert( EIGHT_BYTE_ALIGNMENT(aSpace1) );

  /*
  ** Load pointers to all cells on sibling pages and the divider cells
  ** into the local apCell[] array.  Make copies of the divider cells
  ** into space obtained from aSpace1[] and remove the the divider Cells
  ** from pParent.
  **
  ** If the siblings are on leaf pages, then the child pointers of the
  ** divider cells are stripped from the cells before they are copied
  ** into aSpace1[].  In this way, all cells in apCell[] are without
  ** child pointers.  If siblings are not leaves, then all cell in
  ** apCell[] include child pointers.  Either way, all cells in apCell[]
  ** are alike.
  **
  ** leafCorrection:  4 if pPage is a leaf.  0 if pPage is not a leaf.
  **       leafData:  1 if pPage holds key+data and pParent holds only keys.
  */
  leafCorrection = (u16)( apOld[0].leaf * 4 );
  leafData = apOld[0].hasData;
  for ( i = 0; i < nOld; i++ )
  {
    int limit;

    /* Before doing anything else, take a copy of the i'th original sibling
    ** The rest of this function will use data from the copies rather
    ** that the original pages since the original pages will be in the
    ** process of being overwritten.  */
    //MemPage pOld = apCopy[i] = (MemPage*)&aSpace1[pBt.pageSize + k*i];
    //memcpy(pOld, apOld[i], sizeof(MemPage));
    //pOld.aData = (void*)&pOld[1];
    //memcpy(pOld.aData, apOld[i].aData, pBt.pageSize);
    MemPage pOld = apCopy[i] = apOld[i].Copy();

    limit = pOld.nCell + pOld.nOverflow;
    if( pOld.nOverflow>0 || true){
    for ( j = 0; j < limit; j++ )
    {
      Debug.Assert( nCell < nMaxCells );
      //apCell[nCell] = findOverflowCell( pOld, j );
      //szCell[nCell] = cellSizePtr( pOld, apCell, nCell );
      int iFOFC = findOverflowCell( pOld, j );
      szCell[nCell] = cellSizePtr( pOld, iFOFC );
      // Copy the Data Locally
      if ( apCell[nCell] == null )
        apCell[nCell] = new u8[szCell[nCell]];
      else if ( apCell[nCell].Length < szCell[nCell] )
        Array.Resize( ref apCell[nCell], szCell[nCell] );
      if ( iFOFC < 0 )  // Overflow Cell
        Buffer.BlockCopy( pOld.aOvfl[-( iFOFC + 1 )].pCell, 0, apCell[nCell], 0, szCell[nCell] );
      else
        Buffer.BlockCopy( pOld.aData, iFOFC, apCell[nCell], 0, szCell[nCell] );
      nCell++;
    }
    }
    else
    {
      u8[] aData = pOld.aData;
      u16 maskPage = pOld.maskPage;
      u16 cellOffset = pOld.cellOffset;
      for ( j = 0; j < limit; j++ )
      {
        Debugger.Break();
        Debug.Assert( nCell < nMaxCells );
        apCell[nCell] = findCellv2( aData, maskPage, cellOffset, j );
        szCell[nCell] = cellSizePtr( pOld, apCell[nCell] );
        nCell++;
      }
    }
    if ( i < nOld - 1 && 0 == leafData )
    {
      u16 sz = (u16)szNew[i];
      byte[] pTemp = sqlite3Malloc( sz + leafCorrection );
      Debug.Assert( nCell < nMaxCells );
      szCell[nCell] = sz;
      //pTemp = &aSpace1[iSpace1];
      //iSpace1 += sz;
      Debug.Assert( sz <= pBt.maxLocal + 23 );
      //Debug.Assert(iSpace1 <= (int)pBt.pageSize);
      Buffer.BlockCopy( pParent.aData, apDiv[i], pTemp, 0, sz );//memcpy( pTemp, apDiv[i], sz );
      if ( apCell[nCell] == null || apCell[nCell].Length < sz )
        Array.Resize( ref apCell[nCell], sz );
      Buffer.BlockCopy( pTemp, leafCorrection, apCell[nCell], 0, sz );//apCell[nCell] = pTemp + leafCorrection;
      Debug.Assert( leafCorrection == 0 || leafCorrection == 4 );
      szCell[nCell] = (u16)( szCell[nCell] - leafCorrection );
      if ( 0 == pOld.leaf )
      {
        Debug.Assert( leafCorrection == 0 );
        Debug.Assert( pOld.hdrOffset == 0 );
        /* The right pointer of the child page pOld becomes the left
        ** pointer of the divider cell */
        Buffer.BlockCopy( pOld.aData, 8, apCell[nCell], 0, 4 );//memcpy( apCell[nCell], ref pOld.aData[8], 4 );
      }
      else
      {
        Debug.Assert( leafCorrection == 4 );
        if ( szCell[nCell] < 4 )
        {
          /* Do not allow any cells smaller than 4 bytes. */
          szCell[nCell] = 4;
        }
      }
      nCell++;
    }
  }

  /*
  ** Figure out the number of pages needed to hold all nCell cells.
  ** Store this number in "k".  Also compute szNew[] which is the total
  ** size of all cells on the i-th page and cntNew[] which is the index
  ** in apCell[] of the cell that divides page i from page i+1.
  ** cntNew[k] should equal nCell.
  **
  ** Values computed by this block:
  **
  **           k: The total number of sibling pages
  **    szNew[i]: Spaced used on the i-th sibling page.
  **   cntNew[i]: Index in apCell[] and szCell[] for the first cell to
  **              the right of the i-th sibling page.
  ** usableSpace: Number of bytes of space available on each sibling.
  **
  */
  usableSpace = (int)pBt.usableSize - 12 + leafCorrection;
  for ( subtotal = k = i = 0; i < nCell; i++ )
  {
    Debug.Assert( i < nMaxCells );
    subtotal += szCell[i] + 2;
    if ( subtotal > usableSpace )
    {
      szNew[k] = subtotal - szCell[i];
      cntNew[k] = i;
      if ( leafData != 0 )
      {
        i--;
      }
      subtotal = 0;
      k++;
      if ( k > NB + 1 )
      {
        rc = SQLITE_CORRUPT_BKPT();
        goto balance_cleanup;
      }
    }
  }
  szNew[k] = subtotal;
  cntNew[k] = nCell;
  k++;

  /*
  ** The packing computed by the previous block is biased toward the siblings
  ** on the left side.  The left siblings are always nearly full, while the
  ** right-most sibling might be nearly empty.  This block of code attempts
  ** to adjust the packing of siblings to get a better balance.
  **
  ** This adjustment is more than an optimization.  The packing above might
  ** be so out of balance as to be illegal.  For example, the right-most
  ** sibling might be completely empty.  This adjustment is not optional.
  */
  for ( i = k - 1; i > 0; i-- )
  {
    int szRight = szNew[i];  /* Size of sibling on the right */
    int szLeft = szNew[i - 1]; /* Size of sibling on the left */
    int r;              /* Index of right-most cell in left sibling */
    int d;              /* Index of first cell to the left of right sibling */

    r = cntNew[i - 1] - 1;
    d = r + 1 - leafData;
    Debug.Assert( d < nMaxCells );
    Debug.Assert( r < nMaxCells );
    while ( szRight == 0 || szRight + szCell[d] + 2 <= szLeft - ( szCell[r] + 2 ) )
    {
      szRight += szCell[d] + 2;
      szLeft -= szCell[r] + 2;
      cntNew[i - 1]--;
      r = cntNew[i - 1] - 1;
      d = r + 1 - leafData;
    }
    szNew[i] = szRight;
    szNew[i - 1] = szLeft;
  }

  /* Either we found one or more cells (cntnew[0])>0) or pPage is
  ** a virtual root page.  A virtual root page is when the real root
  ** page is page 1 and we are the only child of that page.
  */
  Debug.Assert( cntNew[0] > 0 || ( pParent.pgno == 1 && pParent.nCell == 0 ) );

  TRACE( "BALANCE: old: %d %d %d  ",
  apOld[0].pgno,
  nOld >= 2 ? apOld[1].pgno : 0,
  nOld >= 3 ? apOld[2].pgno : 0
  );

  /*
  ** Allocate k new pages.  Reuse old pages where possible.
  */
  if ( apOld[0].pgno <= 1 )
  {
    rc = SQLITE_CORRUPT_BKPT();
    goto balance_cleanup;
  }
  pageFlags = apOld[0].aData[0];
  for ( i = 0; i < k; i++ )
  {
    MemPage pNew = new MemPage();
    if ( i < nOld )
    {
      pNew = apNew[i] = apOld[i];
      apOld[i] = null;
      rc = sqlite3PagerWrite( pNew.pDbPage );
      nNew++;
      if ( rc != 0 )
        goto balance_cleanup;
    }
    else
    {
      Debug.Assert( i > 0 );
      rc = allocateBtreePage( pBt, ref pNew, ref pgno, pgno, 0 );
      if ( rc != 0 )
        goto balance_cleanup;
      apNew[i] = pNew;
      nNew++;

      /* Set the pointer-map entry for the new sibling page. */
#if !SQLITE_OMIT_AUTOVACUUM //   if ( ISAUTOVACUUM )
      if ( pBt.autoVacuum )
#else
if (false)
#endif
      {
        ptrmapPut( pBt, pNew.pgno, PTRMAP_BTREE, pParent.pgno, ref rc );
        if ( rc != SQLITE_OK )
        {
          goto balance_cleanup;
        }
      }
    }
  }

  /* Free any old pages that were not reused as new pages.
  */
  while ( i < nOld )
  {
    freePage( apOld[i], ref rc );
    if ( rc != 0 )
      goto balance_cleanup;
    releasePage( apOld[i] );
    apOld[i] = null;
    i++;
  }

  /*
  ** Put the new pages in accending order.  This helps to
  ** keep entries in the disk file in order so that a scan
  ** of the table is a linear scan through the file.  That
  ** in turn helps the operating system to deliver pages
  ** from the disk more rapidly.
  **
  ** An O(n^2) insertion sort algorithm is used, but since
  ** n is never more than NB (a small constant), that should
  ** not be a problem.
  **
  ** When NB==3, this one optimization makes the database
  ** about 25% faster for large insertions and deletions.
  */
  for ( i = 0; i < k - 1; i++ )
  {
    int minV = (int)apNew[i].pgno;
    int minI = i;
    for ( j = i + 1; j < k; j++ )
    {
      if ( apNew[j].pgno < (u32)minV )
      {
        minI = j;
        minV = (int)apNew[j].pgno;
      }
    }
    if ( minI > i )
    {
      MemPage pT;
      pT = apNew[i];
      apNew[i] = apNew[minI];
      apNew[minI] = pT;
    }
  }
  TRACE( "new: %d(%d) %d(%d) %d(%d) %d(%d) %d(%d)\n",
  apNew[0].pgno, szNew[0],
  nNew >= 2 ? apNew[1].pgno : 0, nNew >= 2 ? szNew[1] : 0,
  nNew >= 3 ? apNew[2].pgno : 0, nNew >= 3 ? szNew[2] : 0,
  nNew >= 4 ? apNew[3].pgno : 0, nNew >= 4 ? szNew[3] : 0,
  nNew >= 5 ? apNew[4].pgno : 0, nNew >= 5 ? szNew[4] : 0 );

  Debug.Assert( sqlite3PagerIswriteable( pParent.pDbPage ) );
  sqlite3Put4byte( pParent.aData, pRight, apNew[nNew - 1].pgno );

  /*
  ** Evenly distribute the data in apCell[] across the new pages.
  ** Insert divider cells into pParent as necessary.
  */
  j = 0;
  for ( i = 0; i < nNew; i++ )
  {
    /* Assemble the new sibling page. */
    MemPage pNew = apNew[i];
    Debug.Assert( j < nMaxCells );
    zeroPage( pNew, pageFlags );
    assemblePage( pNew, cntNew[i] - j, apCell, szCell, j );
    Debug.Assert( pNew.nCell > 0 || ( nNew == 1 && cntNew[0] == 0 ) );
    Debug.Assert( pNew.nOverflow == 0 );

    j = cntNew[i];

    /* If the sibling page assembled above was not the right-most sibling,
    ** insert a divider cell into the parent page.
    */
    Debug.Assert( i < nNew - 1 || j == nCell );
    if ( j < nCell )
    {
      u8[] pCell;
      u8[] pTemp;
      int sz;

      Debug.Assert( j < nMaxCells );
      pCell = apCell[j];
      sz = szCell[j] + leafCorrection;
      pTemp = sqlite3Malloc( sz );//&aOvflSpace[iOvflSpace];
      if ( 0 == pNew.leaf )
      {
        Buffer.BlockCopy( pCell, 0, pNew.aData, 8, 4 );//memcpy( pNew.aData[8], pCell, 4 );
      }
      else if ( leafData != 0 )
      {
        /* If the tree is a leaf-data tree, and the siblings are leaves,
        ** then there is no divider cell in apCell[]. Instead, the divider
        ** cell consists of the integer key for the right-most cell of
        ** the sibling-page assembled above only.
        */
        CellInfo info = new CellInfo();
        j--;
        btreeParseCellPtr( pNew, apCell[j], ref info );
        pCell = pTemp;
        sz = 4 + putVarint( pCell, 4, (u64)info.nKey );
        pTemp = null;
      }
      else
      {
        //------------ pCell -= 4;
        byte[] _pCell_4 = sqlite3Malloc( pCell.Length + 4 );
        Buffer.BlockCopy( pCell, 0, _pCell_4, 4, pCell.Length );
        pCell = _pCell_4;
        //
        /* Obscure case for non-leaf-data trees: If the cell at pCell was
        ** previously stored on a leaf node, and its reported size was 4
        ** bytes, then it may actually be smaller than this
        ** (see btreeParseCellPtr(), 4 bytes is the minimum size of
        ** any cell). But it is important to pass the correct size to
        ** insertCell(), so reparse the cell now.
        **
        ** Note that this can never happen in an SQLite data file, as all
        ** cells are at least 4 bytes. It only happens in b-trees used
        ** to evaluate "IN (SELECT ...)" and similar clauses.
        */
        if ( szCell[j] == 4 )
        {
          Debug.Assert( leafCorrection == 4 );
          sz = cellSizePtr( pParent, pCell );
        }
      }
      iOvflSpace += sz;
      Debug.Assert( sz <= pBt.maxLocal + 23 );
      Debug.Assert( iOvflSpace <= (int)pBt.pageSize );
      insertCell( pParent, nxDiv, pCell, sz, pTemp, pNew.pgno, ref rc );
      if ( rc != SQLITE_OK )
        goto balance_cleanup;
      Debug.Assert( sqlite3PagerIswriteable( pParent.pDbPage ) );

      j++;
      nxDiv++;
    }
  }
  Debug.Assert( j == nCell );
  Debug.Assert( nOld > 0 );
  Debug.Assert( nNew > 0 );
  if ( ( pageFlags & PTF_LEAF ) == 0 )
  {
    Buffer.BlockCopy( apCopy[nOld - 1].aData, 8, apNew[nNew - 1].aData, 8, 4 ); //u8* zChild = &apCopy[nOld - 1].aData[8];
    //memcpy( apNew[nNew - 1].aData[8], zChild, 4 );
  }

  if ( isRoot != 0 && pParent.nCell == 0 && pParent.hdrOffset <= apNew[0].nFree )
  {
    /* The root page of the b-tree now contains no cells. The only sibling
    ** page is the right-child of the parent. Copy the contents of the
    ** child page into the parent, decreasing the overall height of the
    ** b-tree structure by one. This is described as the "balance-shallower"
    ** sub-algorithm in some documentation.
    **
    ** If this is an auto-vacuum database, the call to copyNodeContent()
    ** sets all pointer-map entries corresponding to database image pages
    ** for which the pointer is stored within the content being copied.
    **
    ** The second Debug.Assert below verifies that the child page is defragmented
    ** (it must be, as it was just reconstructed using assemblePage()). This
    ** is important if the parent page happens to be page 1 of the database
    ** image.  */
    Debug.Assert( nNew == 1 );
    Debug.Assert( apNew[0].nFree ==
    ( get2byte( apNew[0].aData, 5 ) - apNew[0].cellOffset - apNew[0].nCell * 2 )
    );
    copyNodeContent( apNew[0], pParent, ref rc );
    freePage( apNew[0], ref rc );
  }
  else
#if !SQLITE_OMIT_AUTOVACUUM //   if ( ISAUTOVACUUM )
    if ( pBt.autoVacuum )
#else
if (false)
#endif
    {
      /* Fix the pointer-map entries for all the cells that were shifted around.
      ** There are several different types of pointer-map entries that need to
      ** be dealt with by this routine. Some of these have been set already, but
      ** many have not. The following is a summary:
      **
      **   1) The entries associated with new sibling pages that were not
      **      siblings when this function was called. These have already
      **      been set. We don't need to worry about old siblings that were
      **      moved to the free-list - the freePage() code has taken care
      **      of those.
      **
      **   2) The pointer-map entries associated with the first overflow
      **      page in any overflow chains used by new divider cells. These
      **      have also already been taken care of by the insertCell() code.
      **
      **   3) If the sibling pages are not leaves, then the child pages of
      **      cells stored on the sibling pages may need to be updated.
      **
      **   4) If the sibling pages are not internal intkey nodes, then any
      **      overflow pages used by these cells may need to be updated
      **      (internal intkey nodes never contain pointers to overflow pages).
      **
      **   5) If the sibling pages are not leaves, then the pointer-map
      **      entries for the right-child pages of each sibling may need
      **      to be updated.
      **
      ** Cases 1 and 2 are dealt with above by other code. The next
      ** block deals with cases 3 and 4 and the one after that, case 5. Since
      ** setting a pointer map entry is a relatively expensive operation, this
      ** code only sets pointer map entries for child or overflow pages that have
      ** actually moved between pages.  */
      MemPage pNew = apNew[0];
      MemPage pOld = apCopy[0];
      int nOverflow = pOld.nOverflow;
      int iNextOld = pOld.nCell + nOverflow;
      int iOverflow = ( nOverflow != 0 ? pOld.aOvfl[0].idx : -1 );
      j = 0;                             /* Current 'old' sibling page */
      k = 0;                             /* Current 'new' sibling page */
      for ( i = 0; i < nCell; i++ )
      {
        int isDivider = 0;
        while ( i == iNextOld )
        {
          /* Cell i is the cell immediately following the last cell on old
          ** sibling page j. If the siblings are not leaf pages of an
          ** intkey b-tree, then cell i was a divider cell. */
          pOld = apCopy[++j];
          iNextOld = i + ( 0 == leafData ? 1 : 0 ) + pOld.nCell + pOld.nOverflow;
          if ( pOld.nOverflow != 0 )
          {
            nOverflow = pOld.nOverflow;
            iOverflow = i + ( 0 == leafData ? 1 : 0 ) + pOld.aOvfl[0].idx;
          }
          isDivider = 0 == leafData ? 1 : 0;
        }

        Debug.Assert( nOverflow > 0 || iOverflow < i );
        Debug.Assert( nOverflow < 2 || pOld.aOvfl[0].idx == pOld.aOvfl[1].idx - 1 );
        Debug.Assert( nOverflow < 3 || pOld.aOvfl[1].idx == pOld.aOvfl[2].idx - 1 );
        if ( i == iOverflow )
        {
          isDivider = 1;
          if ( ( --nOverflow ) > 0 )
          {
            iOverflow++;
          }
        }

        if ( i == cntNew[k] )
        {
          /* Cell i is the cell immediately following the last cell on new
          ** sibling page k. If the siblings are not leaf pages of an
          ** intkey b-tree, then cell i is a divider cell.  */
          pNew = apNew[++k];
          if ( 0 == leafData )
            continue;
        }
        Debug.Assert( j < nOld );
        Debug.Assert( k < nNew );

        /* If the cell was originally divider cell (and is not now) or
        ** an overflow cell, or if the cell was located on a different sibling
        ** page before the balancing, then the pointer map entries associated
        ** with any child or overflow pages need to be updated.  */
        if ( isDivider != 0 || pOld.pgno != pNew.pgno )
        {
          if ( 0 == leafCorrection )
          {
            ptrmapPut( pBt, sqlite3Get4byte( apCell[i] ), PTRMAP_BTREE, pNew.pgno, ref rc );
          }
          if ( szCell[i] > pNew.minLocal )
          {
            ptrmapPutOvflPtr( pNew, apCell[i], ref rc );
          }
        }
      }

      if ( 0 == leafCorrection )
      {
        for ( i = 0; i < nNew; i++ )
        {
          u32 key = sqlite3Get4byte( apNew[i].aData, 8 );
          ptrmapPut( pBt, key, PTRMAP_BTREE, apNew[i].pgno, ref rc );
        }
      }

#if FALSE
/* The ptrmapCheckPages() contains Debug.Assert() statements that verify that
** all pointer map pages are set correctly. This is helpful while
** debugging. This is usually disabled because a corrupt database may
** cause an Debug.Assert() statement to fail.  */
ptrmapCheckPages(apNew, nNew);
ptrmapCheckPages(pParent, 1);
#endif
    }

  Debug.Assert( pParent.isInit != 0 );
  TRACE( "BALANCE: finished: old=%d new=%d cells=%d\n",
  nOld, nNew, nCell );

/*
** Cleanup before returning.
*/
balance_cleanup:
  sqlite3ScratchFree( apCell );
  for ( i = 0; i < nOld; i++ )
  {
    releasePage( apOld[i] );
  }
  for ( i = 0; i < nNew; i++ )
  {
    releasePage( apNew[i] );
  }

  return rc;
}


/*
** This function is called when the root page of a b-tree structure is
** overfull (has one or more overflow pages).
**
** A new child page is allocated and the contents of the current root
** page, including overflow cells, are copied into the child. The root
** page is then overwritten to make it an empty page with the right-child
** pointer pointing to the new page.
**
** Before returning, all pointer-map entries corresponding to pages
** that the new child-page now contains pointers to are updated. The
** entry corresponding to the new right-child pointer of the root
** page is also updated.
**
** If successful, ppChild is set to contain a reference to the child
** page and SQLITE_OK is returned. In this case the caller is required
** to call releasePage() on ppChild exactly once. If an error occurs,
** an error code is returned and ppChild is set to 0.
*/
static int balance_deeper( MemPage pRoot, ref MemPage ppChild )
{
  int rc;                        /* Return value from subprocedures */
  MemPage pChild = null;           /* Pointer to a new child page */
  Pgno pgnoChild = 0;            /* Page number of the new child page */
  BtShared pBt = pRoot.pBt;    /* The BTree */

  Debug.Assert( pRoot.nOverflow > 0 );
  Debug.Assert( sqlite3_mutex_held( pBt.mutex ) );

  /* Make pRoot, the root page of the b-tree, writable. Allocate a new
  ** page that will become the new right-child of pPage. Copy the contents
  ** of the node stored on pRoot into the new child page.
  */
  rc = sqlite3PagerWrite( pRoot.pDbPage );
  if ( rc == SQLITE_OK )
  {
    rc = allocateBtreePage( pBt, ref pChild, ref pgnoChild, pRoot.pgno, 0 );
    copyNodeContent( pRoot, pChild, ref rc );
#if !SQLITE_OMIT_AUTOVACUUM //   if ( ISAUTOVACUUM )
    if ( pBt.autoVacuum )
#else
if (false)
#endif
    {
      ptrmapPut( pBt, pgnoChild, PTRMAP_BTREE, pRoot.pgno, ref rc );
    }
  }
  if ( rc != 0 )
  {
    ppChild = null;
    releasePage( pChild );
    return rc;
  }
  Debug.Assert( sqlite3PagerIswriteable( pChild.pDbPage ) );
  Debug.Assert( sqlite3PagerIswriteable( pRoot.pDbPage ) );
  Debug.Assert( pChild.nCell == pRoot.nCell );

  TRACE( "BALANCE: copy root %d into %d\n", pRoot.pgno, pChild.pgno );

  /* Copy the overflow cells from pRoot to pChild */
  Array.Copy( pRoot.aOvfl, pChild.aOvfl, pRoot.nOverflow );//memcpy(pChild.aOvfl, pRoot.aOvfl, pRoot.nOverflow*sizeof(pRoot.aOvfl[0]));
  pChild.nOverflow = pRoot.nOverflow;

  /* Zero the contents of pRoot. Then install pChild as the right-child. */
  zeroPage( pRoot, pChild.aData[0] & ~PTF_LEAF );
  sqlite3Put4byte( pRoot.aData, pRoot.hdrOffset + 8, pgnoChild );

  ppChild = pChild;
  return SQLITE_OK;
}

/*
** The page that pCur currently points to has just been modified in
** some way. This function figures out if this modification means the
** tree needs to be balanced, and if so calls the appropriate balancing
** routine. Balancing routines are:
**
**   balance_quick()
**   balance_deeper()
**   balance_nonroot()
*/
static u8[] aBalanceQuickSpace = new u8[13];
static int balance( BtCursor pCur )
{
  int rc = SQLITE_OK;
  int nMin = (int)pCur.pBt.usableSize * 2 / 3;

  //u8[] pFree = null;

#if !NDEBUG || SQLITE_COVERAGE_TEST || DEBUG
  int balance_quick_called = 0;//TESTONLY( int balance_quick_called = 0 );
  int balance_deeper_called = 0;//TESTONLY( int balance_deeper_called = 0 );
#else
int balance_quick_called = 0;
int balance_deeper_called = 0;
#endif

  do
  {
    int iPage = pCur.iPage;
    MemPage pPage = pCur.apPage[iPage];

    if ( iPage == 0 )
    {
      if ( pPage.nOverflow != 0 )
      {
        /* The root page of the b-tree is overfull. In this case call the
        ** balance_deeper() function to create a new child for the root-page
        ** and copy the current contents of the root-page to it. The
        ** next iteration of the do-loop will balance the child page.
        */
        Debug.Assert( ( balance_deeper_called++ ) == 0 );
        rc = balance_deeper( pPage, ref pCur.apPage[1] );
        if ( rc == SQLITE_OK )
        {
          pCur.iPage = 1;
          pCur.aiIdx[0] = 0;
          pCur.aiIdx[1] = 0;
          Debug.Assert( pCur.apPage[1].nOverflow != 0 );
        }
      }
      else
      {
        break;
      }
    }
    else if ( pPage.nOverflow == 0 && pPage.nFree <= nMin )
    {
      break;
    }
    else
    {
      MemPage pParent = pCur.apPage[iPage - 1];
      int iIdx = pCur.aiIdx[iPage - 1];

      rc = sqlite3PagerWrite( pParent.pDbPage );
      if ( rc == SQLITE_OK )
      {
#if !SQLITE_OMIT_QUICKBALANCE
        if ( pPage.hasData != 0
        && pPage.nOverflow == 1
        && pPage.aOvfl[0].idx == pPage.nCell
        && pParent.pgno != 1
        && pParent.nCell == iIdx
        )
        {
          /* Call balance_quick() to create a new sibling of pPage on which
          ** to store the overflow cell. balance_quick() inserts a new cell
          ** into pParent, which may cause pParent overflow. If this
          ** happens, the next interation of the do-loop will balance pParent
          ** use either balance_nonroot() or balance_deeper(). Until this
          ** happens, the overflow cell is stored in the aBalanceQuickSpace[]
          ** buffer.
          **
          ** The purpose of the following Debug.Assert() is to check that only a
          ** single call to balance_quick() is made for each call to this
          ** function. If this were not verified, a subtle bug involving reuse
          ** of the aBalanceQuickSpace[] might sneak in.
          */
          Debug.Assert( ( balance_quick_called++ ) == 0 );
          rc = balance_quick( pParent, pPage, aBalanceQuickSpace );
        }
        else
#endif
        {
          /* In this case, call balance_nonroot() to redistribute cells
          ** between pPage and up to 2 of its sibling pages. This involves
          ** modifying the contents of pParent, which may cause pParent to
          ** become overfull or underfull. The next iteration of the do-loop
          ** will balance the parent page to correct this.
          **
          ** If the parent page becomes overfull, the overflow cell or cells
          ** are stored in the pSpace buffer allocated immediately below.
          ** A subsequent iteration of the do-loop will deal with this by
          ** calling balance_nonroot() (balance_deeper() may be called first,
          ** but it doesn't deal with overflow cells - just moves them to a
          ** different page). Once this subsequent call to balance_nonroot()
          ** has completed, it is safe to release the pSpace buffer used by
          ** the previous call, as the overflow cell data will have been
          ** copied either into the body of a database page or into the new
          ** pSpace buffer passed to the latter call to balance_nonroot().
          */
          ////u8[] pSpace = new u8[pCur.pBt.pageSize];// u8 pSpace = sqlite3PageMalloc( pCur.pBt.pageSize );
          rc = balance_nonroot( pParent, iIdx, null, iPage == 1 ? 1 : 0 );
          //if (pFree != null)
          //{
          //  /* If pFree is not NULL, it points to the pSpace buffer used
          //  ** by a previous call to balance_nonroot(). Its contents are
          //  ** now stored either on real database pages or within the
          //  ** new pSpace buffer, so it may be safely freed here. */
          //  sqlite3PageFree(ref pFree);
          //}

          /* The pSpace buffer will be freed after the next call to
          ** balance_nonroot(), or just before this function returns, whichever
          ** comes first. */
          //pFree = pSpace;
        }
      }

      pPage.nOverflow = 0;

      /* The next iteration of the do-loop balances the parent page. */
      releasePage( pPage );
      pCur.iPage--;
    }
  } while ( rc == SQLITE_OK );

  //if (pFree != null)
  //{
  //  sqlite3PageFree(ref pFree);
  //}
  return rc;
}


/*
** Insert a new record into the BTree.  The key is given by (pKey,nKey)
** and the data is given by (pData,nData).  The cursor is used only to
** define what table the record should be inserted into.  The cursor
** is left pointing at a random location.
**
** For an INTKEY table, only the nKey value of the key is used.  pKey is
** ignored.  For a ZERODATA table, the pData and nData are both ignored.
**
** If the seekResult parameter is non-zero, then a successful call to
** MovetoUnpacked() to seek cursor pCur to (pKey, nKey) has already
** been performed. seekResult is the search result returned (a negative
** number if pCur points at an entry that is smaller than (pKey, nKey), or
** a positive value if pCur points at an etry that is larger than
** (pKey, nKey)).
**
** If the seekResult parameter is non-zero, then the caller guarantees that
** cursor pCur is pointing at the existing copy of a row that is to be
** overwritten.  If the seekResult parameter is 0, then cursor pCur may
** point to any entry or to no entry at all and so this function has to seek
** the cursor before the new key can be inserted.
*/
static int sqlite3BtreeInsert(
BtCursor pCur,                /* Insert data into the table of this cursor */
byte[] pKey, i64 nKey,        /* The key of the new record */
byte[] pData, int nData,      /* The data of the new record */
int nZero,                    /* Number of extra 0 bytes to append to data */
int appendBias,               /* True if this is likely an append */
int seekResult                /* Result of prior MovetoUnpacked() call */
)
{
  int rc;
  int loc = seekResult;       /* -1: before desired location  +1: after */
  int szNew = 0;
  int idx;
  MemPage pPage;
  Btree p = pCur.pBtree;
  BtShared pBt = p.pBt;
  int oldCell;
  byte[] newCell = null;

  if ( pCur.eState == CURSOR_FAULT )
  {
    Debug.Assert( pCur.skipNext != SQLITE_OK );
    return pCur.skipNext;
  }

  Debug.Assert( cursorHoldsMutex( pCur ) );
  Debug.Assert( pCur.wrFlag != 0 && pBt.inTransaction == TRANS_WRITE && !pBt.readOnly );
  Debug.Assert( hasSharedCacheTableLock( p, pCur.pgnoRoot, pCur.pKeyInfo != null ? 1 : 0, 2 ) );

  /* Assert that the caller has been consistent. If this cursor was opened
  ** expecting an index b-tree, then the caller should be inserting blob
  ** keys with no associated data. If the cursor was opened expecting an
  ** intkey table, the caller should be inserting integer keys with a
  ** blob of associated data.  */
  Debug.Assert( ( pKey == null ) == ( pCur.pKeyInfo == null ) );

  /* If this is an insert into a table b-tree, invalidate any incrblob
  ** cursors open on the row being replaced (assuming this is a replace
  ** operation - if it is not, the following is a no-op).  */
  if ( pCur.pKeyInfo == null )
  {
    invalidateIncrblobCursors( p, nKey, 0 );
  }

  /* Save the positions of any other cursors open on this table.
  **
  ** In some cases, the call to btreeMoveto() below is a no-op. For
  ** example, when inserting data into a table with auto-generated integer
  ** keys, the VDBE layer invokes sqlite3BtreeLast() to figure out the
  ** integer key to use. It then calls this function to actually insert the
  ** data into the intkey B-Tree. In this case btreeMoveto() recognizes
  ** that the cursor is already where it needs to be and returns without
  ** doing any work. To avoid thwarting these optimizations, it is important
  ** not to clear the cursor here.
  */
  rc = saveAllCursors( pBt, pCur.pgnoRoot, pCur );
  if ( rc != 0 )
    return rc;
  if ( 0 == loc )
  {
    rc = btreeMoveto( pCur, pKey, nKey, appendBias, ref loc );
    if ( rc != 0 )
      return rc;
  }
  Debug.Assert( pCur.eState == CURSOR_VALID || ( pCur.eState == CURSOR_INVALID && loc != 0 ) );

  pPage = pCur.apPage[pCur.iPage];
  Debug.Assert( pPage.intKey != 0 || nKey >= 0 );
  Debug.Assert( pPage.leaf != 0 || 0 == pPage.intKey );

  TRACE( "INSERT: table=%d nkey=%lld ndata=%d page=%d %s\n",
  pCur.pgnoRoot, nKey, nData, pPage.pgno,
  loc == 0 ? "overwrite" : "new entry" );
  Debug.Assert( pPage.isInit != 0 );
  allocateTempSpace( pBt );
  newCell = pBt.pTmpSpace;
  //if (newCell == null) return SQLITE_NOMEM;
  rc = fillInCell( pPage, newCell, pKey, nKey, pData, nData, nZero, ref szNew );
  if ( rc != 0 )
    goto end_insert;
  Debug.Assert( szNew == cellSizePtr( pPage, newCell ) );
  Debug.Assert( szNew <= MX_CELL_SIZE( pBt ) );
  idx = pCur.aiIdx[pCur.iPage];
  if ( loc == 0 )
  {
    u16 szOld;
    Debug.Assert( idx < pPage.nCell );
    rc = sqlite3PagerWrite( pPage.pDbPage );
    if ( rc != 0 )
    {
      goto end_insert;
    }
    oldCell = findCell( pPage, idx );
    if ( 0 == pPage.leaf )
    {
      //memcpy(newCell, oldCell, 4);
      newCell[0] = pPage.aData[oldCell + 0];
      newCell[1] = pPage.aData[oldCell + 1];
      newCell[2] = pPage.aData[oldCell + 2];
      newCell[3] = pPage.aData[oldCell + 3];
    }
    szOld = cellSizePtr( pPage, oldCell );
    rc = clearCell( pPage, oldCell );
    dropCell( pPage, idx, szOld, ref rc );
    if ( rc != 0 )
      goto end_insert;
  }
  else if ( loc < 0 && pPage.nCell > 0 )
  {
    Debug.Assert( pPage.leaf != 0 );
    idx = ++pCur.aiIdx[pCur.iPage];
  }
  else
  {
    Debug.Assert( pPage.leaf != 0 );
  }
  insertCell( pPage, idx, newCell, szNew, null, 0, ref rc );
  Debug.Assert( rc != SQLITE_OK || pPage.nCell > 0 || pPage.nOverflow > 0 );

  /* If no error has occured and pPage has an overflow cell, call balance()
  ** to redistribute the cells within the tree. Since balance() may move
  ** the cursor, zero the BtCursor.info.nSize and BtCursor.validNKey
  ** variables.
  **
  ** Previous versions of SQLite called moveToRoot() to move the cursor
  ** back to the root page as balance() used to invalidate the contents
  ** of BtCursor.apPage[] and BtCursor.aiIdx[]. Instead of doing that,
  ** set the cursor state to "invalid". This makes common insert operations
  ** slightly faster.
  **
  ** There is a subtle but important optimization here too. When inserting
  ** multiple records into an intkey b-tree using a single cursor (as can
  ** happen while processing an "INSERT INTO ... SELECT" statement), it
  ** is advantageous to leave the cursor pointing to the last entry in
  ** the b-tree if possible. If the cursor is left pointing to the last
  ** entry in the table, and the next row inserted has an integer key
  ** larger than the largest existing key, it is possible to insert the
  ** row without seeking the cursor. This can be a big performance boost.
  */
  pCur.info.nSize = 0;
  pCur.validNKey = false;
  if ( rc == SQLITE_OK && pPage.nOverflow != 0 )
  {
    rc = balance( pCur );

    /* Must make sure nOverflow is reset to zero even if the balance()
    ** fails. Internal data structure corruption will result otherwise.
    ** Also, set the cursor state to invalid. This stops saveCursorPosition()
    ** from trying to save the current position of the cursor.  */
    pCur.apPage[pCur.iPage].nOverflow = 0;
    pCur.eState = CURSOR_INVALID;
  }
  Debug.Assert( pCur.apPage[pCur.iPage].nOverflow == 0 );

end_insert:
  return rc;
}

/*
** Delete the entry that the cursor is pointing to.  The cursor
** is left pointing at a arbitrary location.
*/
static int sqlite3BtreeDelete( BtCursor pCur )
{
  Btree p = pCur.pBtree;
  BtShared pBt = p.pBt;
  int rc;                             /* Return code */
  MemPage pPage;                      /* Page to delete cell from */
  int pCell;                          /* Pointer to cell to delete */
  int iCellIdx;                       /* Index of cell to delete */
  int iCellDepth;                     /* Depth of node containing pCell */

  Debug.Assert( cursorHoldsMutex( pCur ) );
  Debug.Assert( pBt.inTransaction == TRANS_WRITE );
  Debug.Assert( !pBt.readOnly );
  Debug.Assert( pCur.wrFlag != 0 );
  Debug.Assert( hasSharedCacheTableLock( p, pCur.pgnoRoot, pCur.pKeyInfo != null ? 1 : 0, 2 ) );
  Debug.Assert( !hasReadConflicts( p, pCur.pgnoRoot ) );

  if ( NEVER( pCur.aiIdx[pCur.iPage] >= pCur.apPage[pCur.iPage].nCell )
  || NEVER( pCur.eState != CURSOR_VALID )
  )
  {
    return SQLITE_ERROR;  /* Something has gone awry. */
  }

  /* If this is a delete operation to remove a row from a table b-tree,
  ** invalidate any incrblob cursors open on the row being deleted.  */
  if ( pCur.pKeyInfo == null )
  {
    invalidateIncrblobCursors( p, pCur.info.nKey, 0 );
  }

  iCellDepth = pCur.iPage;
  iCellIdx = pCur.aiIdx[iCellDepth];
  pPage = pCur.apPage[iCellDepth];
  pCell = findCell( pPage, iCellIdx );

  /* If the page containing the entry to delete is not a leaf page, move
  ** the cursor to the largest entry in the tree that is smaller than
  ** the entry being deleted. This cell will replace the cell being deleted
  ** from the internal node. The 'previous' entry is used for this instead
  ** of the 'next' entry, as the previous entry is always a part of the
  ** sub-tree headed by the child page of the cell being deleted. This makes
  ** balancing the tree following the delete operation easier.  */
  if ( 0 == pPage.leaf )
  {
    int notUsed = 0;
    rc = sqlite3BtreePrevious( pCur, ref notUsed );
    if ( rc != 0 )
      return rc;
  }

  /* Save the positions of any other cursors open on this table before
  ** making any modifications. Make the page containing the entry to be
  ** deleted writable. Then free any overflow pages associated with the
  ** entry and finally remove the cell itself from within the page.
  */
  rc = saveAllCursors( pBt, pCur.pgnoRoot, pCur );
  if ( rc != 0 )
    return rc;
  rc = sqlite3PagerWrite( pPage.pDbPage );
  if ( rc != 0 )
    return rc;
  rc = clearCell( pPage, pCell );
  dropCell( pPage, iCellIdx, cellSizePtr( pPage, pCell ), ref rc );
  if ( rc != 0 )
    return rc;

  /* If the cell deleted was not located on a leaf page, then the cursor
  ** is currently pointing to the largest entry in the sub-tree headed
  ** by the child-page of the cell that was just deleted from an internal
  ** node. The cell from the leaf node needs to be moved to the internal
  ** node to replace the deleted cell.  */
  if ( 0 == pPage.leaf )
  {
    MemPage pLeaf = pCur.apPage[pCur.iPage];
    int nCell;
    Pgno n = pCur.apPage[iCellDepth + 1].pgno;
    //byte[] pTmp;

    pCell = findCell( pLeaf, pLeaf.nCell - 1 );
    nCell = cellSizePtr( pLeaf, pCell );
    Debug.Assert( MX_CELL_SIZE( pBt ) >= nCell );

    //allocateTempSpace(pBt);
    //pTmp = pBt.pTmpSpace;

    rc = sqlite3PagerWrite( pLeaf.pDbPage );
    byte[] pNext_4 = sqlite3Malloc( nCell + 4 );
    Buffer.BlockCopy( pLeaf.aData, pCell - 4, pNext_4, 0, nCell + 4 );
    insertCell( pPage, iCellIdx, pNext_4, nCell + 4, null, n, ref rc ); //insertCell( pPage, iCellIdx, pCell - 4, nCell + 4, pTmp, n, ref rc );
    dropCell( pLeaf, pLeaf.nCell - 1, nCell, ref rc );
    if ( rc != 0 )
      return rc;
  }

  /* Balance the tree. If the entry deleted was located on a leaf page,
  ** then the cursor still points to that page. In this case the first
  ** call to balance() repairs the tree, and the if(...) condition is
  ** never true.
  **
  ** Otherwise, if the entry deleted was on an internal node page, then
  ** pCur is pointing to the leaf page from which a cell was removed to
  ** replace the cell deleted from the internal node. This is slightly
  ** tricky as the leaf node may be underfull, and the internal node may
  ** be either under or overfull. In this case run the balancing algorithm
  ** on the leaf node first. If the balance proceeds far enough up the
  ** tree that we can be sure that any problem in the internal node has
  ** been corrected, so be it. Otherwise, after balancing the leaf node,
  ** walk the cursor up the tree to the internal node and balance it as
  ** well.  */
  rc = balance( pCur );
  if ( rc == SQLITE_OK && pCur.iPage > iCellDepth )
  {
    while ( pCur.iPage > iCellDepth )
    {
      releasePage( pCur.apPage[pCur.iPage--] );
    }
    rc = balance( pCur );
  }

  if ( rc == SQLITE_OK )
  {
    moveToRoot( pCur );
  }
  return rc;
}

/*
** Create a new BTree table.  Write into piTable the page
** number for the root page of the new table.
**
** The type of type is determined by the flags parameter.  Only the
** following values of flags are currently in use.  Other values for
** flags might not work:
**
**     BTREE_INTKEY|BTREE_LEAFDATA     Used for SQL tables with rowid keys
**     BTREE_ZERODATA                  Used for SQL indices
*/
static int btreeCreateTable( Btree p, ref int piTable, int createTabFlags )
{
  BtShared pBt = p.pBt;
  MemPage pRoot = new MemPage();
  Pgno pgnoRoot = 0;
  int rc;
  int ptfFlags;          /* Page-type flage for the root page of new table */

  Debug.Assert( sqlite3BtreeHoldsMutex( p ) );
  Debug.Assert( pBt.inTransaction == TRANS_WRITE );
  Debug.Assert( !pBt.readOnly );

#if SQLITE_OMIT_AUTOVACUUM
rc = allocateBtreePage(pBt, ref pRoot, ref pgnoRoot, 1, 0);
if( rc !=0){
return rc;
}
#else
  if ( pBt.autoVacuum )
  {
    Pgno pgnoMove = 0;                    /* Move a page here to make room for the root-page */
    MemPage pPageMove = new MemPage();  /* The page to move to. */

    /* Creating a new table may probably require moving an existing database
    ** to make room for the new tables root page. In case this page turns
    ** out to be an overflow page, delete all overflow page-map caches
    ** held by open cursors.
    */
    invalidateAllOverflowCache( pBt );

    /* Read the value of meta[3] from the database to determine where the
    ** root page of the new table should go. meta[3] is the largest root-page
    ** created so far, so the new root-page is (meta[3]+1).
    */
    sqlite3BtreeGetMeta( p, BTREE_LARGEST_ROOT_PAGE, ref pgnoRoot );
    pgnoRoot++;

    /* The new root-page may not be allocated on a pointer-map page, or the
    ** PENDING_BYTE page.
    */
    while ( pgnoRoot == PTRMAP_PAGENO( pBt, pgnoRoot ) ||
    pgnoRoot == PENDING_BYTE_PAGE( pBt ) )
    {
      pgnoRoot++;
    }
    Debug.Assert( pgnoRoot >= 3 );

    /* Allocate a page. The page that currently resides at pgnoRoot will
    ** be moved to the allocated page (unless the allocated page happens
    ** to reside at pgnoRoot).
    */
    rc = allocateBtreePage( pBt, ref pPageMove, ref pgnoMove, pgnoRoot, 1 );
    if ( rc != SQLITE_OK )
    {
      return rc;
    }

    if ( pgnoMove != pgnoRoot )
    {
      /* pgnoRoot is the page that will be used for the root-page of
      ** the new table (assuming an error did not occur). But we were
      ** allocated pgnoMove. If required (i.e. if it was not allocated
      ** by extending the file), the current page at position pgnoMove
      ** is already journaled.
      */
      u8 eType = 0;
      Pgno iPtrPage = 0;

      releasePage( pPageMove );

      /* Move the page currently at pgnoRoot to pgnoMove. */
      rc = btreeGetPage( pBt, pgnoRoot, ref pRoot, 0 );
      if ( rc != SQLITE_OK )
      {
        return rc;
      }
      rc = ptrmapGet( pBt, pgnoRoot, ref eType, ref iPtrPage );
      if ( eType == PTRMAP_ROOTPAGE || eType == PTRMAP_FREEPAGE )
      {
        rc = SQLITE_CORRUPT_BKPT();
      }
      if ( rc != SQLITE_OK )
      {
        releasePage( pRoot );
        return rc;
      }
      Debug.Assert( eType != PTRMAP_ROOTPAGE );
      Debug.Assert( eType != PTRMAP_FREEPAGE );
      rc = relocatePage( pBt, pRoot, eType, iPtrPage, pgnoMove, 0 );
      releasePage( pRoot );

      /* Obtain the page at pgnoRoot */
      if ( rc != SQLITE_OK )
      {
        return rc;
      }
      rc = btreeGetPage( pBt, pgnoRoot, ref pRoot, 0 );
      if ( rc != SQLITE_OK )
      {
        return rc;
      }
      rc = sqlite3PagerWrite( pRoot.pDbPage );
      if ( rc != SQLITE_OK )
      {
        releasePage( pRoot );
        return rc;
      }
    }
    else
    {
      pRoot = pPageMove;
    }

    /* Update the pointer-map and meta-data with the new root-page number. */
    ptrmapPut( pBt, pgnoRoot, PTRMAP_ROOTPAGE, 0, ref rc );
    if ( rc != 0 )
    {
      releasePage( pRoot );
      return rc;
    }

    /* When the new root page was allocated, page 1 was made writable in
    ** order either to increase the database filesize, or to decrement the
    ** freelist count.  Hence, the sqlite3BtreeUpdateMeta() call cannot fail.
    */
    Debug.Assert( sqlite3PagerIswriteable( pBt.pPage1.pDbPage ) );
    rc = sqlite3BtreeUpdateMeta( p, 4, pgnoRoot );
    if ( NEVER( rc != 0 ) )
    {
      releasePage( pRoot );
      return rc;
    }

  }
  else
  {
    rc = allocateBtreePage( pBt, ref pRoot, ref pgnoRoot, 1, 0 );
    if ( rc != 0 )
      return rc;
  }
#endif
  Debug.Assert( sqlite3PagerIswriteable( pRoot.pDbPage ) );
  if ( ( createTabFlags & BTREE_INTKEY ) != 0 )
  {
    ptfFlags = PTF_INTKEY | PTF_LEAFDATA | PTF_LEAF;
  }
  else
  {
    ptfFlags = PTF_ZERODATA | PTF_LEAF;
  }
  zeroPage( pRoot, ptfFlags );
  sqlite3PagerUnref( pRoot.pDbPage );
  Debug.Assert( ( pBt.openFlags & BTREE_SINGLE ) == 0 || pgnoRoot == 2 );
  piTable = (int)pgnoRoot;
  return SQLITE_OK;
}
static int sqlite3BtreeCreateTable( Btree p, ref int piTable, int flags )
{
  int rc;
  sqlite3BtreeEnter( p );
  rc = btreeCreateTable( p, ref piTable, flags );
  sqlite3BtreeLeave( p );
  return rc;
}

/*
** Erase the given database page and all its children.  Return
** the page to the freelist.
*/
static int clearDatabasePage(
BtShared pBt,         /* The BTree that contains the table */
Pgno pgno,            /* Page number to clear */
int freePageFlag,     /* Deallocate page if true */
ref int pnChange      /* Add number of Cells freed to this counter */
)
{
  MemPage pPage = new MemPage();
  int rc;
  byte[] pCell;
  int i;

  Debug.Assert( sqlite3_mutex_held( pBt.mutex ) );
  if ( pgno > btreePagecount( pBt ) )
  {
    return SQLITE_CORRUPT_BKPT();
  }

  rc = getAndInitPage( pBt, pgno, ref pPage );
  if ( rc != 0 )
    return rc;
  for ( i = 0; i < pPage.nCell; i++ )
  {
    int iCell = findCell( pPage, i );
    pCell = pPage.aData; //        pCell = findCell( pPage, i );
    if ( 0 == pPage.leaf )
    {
      rc = clearDatabasePage( pBt, sqlite3Get4byte( pCell, iCell ), 1, ref pnChange );
      if ( rc != 0 )
        goto cleardatabasepage_out;
    }
    rc = clearCell( pPage, iCell );
    if ( rc != 0 )
      goto cleardatabasepage_out;
  }
  if ( 0 == pPage.leaf )
  {
    rc = clearDatabasePage( pBt, sqlite3Get4byte( pPage.aData, 8 ), 1, ref pnChange );
    if ( rc != 0 )
      goto cleardatabasepage_out;
  }
  else //if (pnChange != 0)
  {
    //Debug.Assert(pPage.intKey != 0);
    pnChange += pPage.nCell;
  }
  if ( freePageFlag != 0 )
  {
    freePage( pPage, ref rc );
  }
  else if ( ( rc = sqlite3PagerWrite( pPage.pDbPage ) ) == 0 )
  {
    zeroPage( pPage, pPage.aData[0] | PTF_LEAF );
  }

cleardatabasepage_out:
  releasePage( pPage );
  return rc;
}

/*
** Delete all information from a single table in the database.  iTable is
** the page number of the root of the table.  After this routine returns,
** the root page is empty, but still exists.
**
** This routine will fail with SQLITE_LOCKED if there are any open
** read cursors on the table.  Open write cursors are moved to the
** root of the table.
**
** If pnChange is not NULL, then table iTable must be an intkey table. The
** integer value pointed to by pnChange is incremented by the number of
** entries in the table.
*/
static int sqlite3BtreeClearTable( Btree p, int iTable, ref int pnChange )
{
  int rc;
  BtShared pBt = p.pBt;
  sqlite3BtreeEnter( p );
  Debug.Assert( p.inTrans == TRANS_WRITE );

  /* Invalidate all incrblob cursors open on table iTable (assuming iTable
  ** is the root of a table b-tree - if it is not, the following call is
  ** a no-op).  */
  invalidateIncrblobCursors( p, 0, 1 );

  rc = saveAllCursors( pBt, (Pgno)iTable, null );
  if ( SQLITE_OK == rc )
  {
    rc = clearDatabasePage( pBt, (Pgno)iTable, 0, ref pnChange );
  }
  sqlite3BtreeLeave( p );
  return rc;
}

/*
** Erase all information in a table and add the root of the table to
** the freelist.  Except, the root of the principle table (the one on
** page 1) is never added to the freelist.
**
** This routine will fail with SQLITE_LOCKED if there are any open
** cursors on the table.
**
** If AUTOVACUUM is enabled and the page at iTable is not the last
** root page in the database file, then the last root page
** in the database file is moved into the slot formerly occupied by
** iTable and that last slot formerly occupied by the last root page
** is added to the freelist instead of iTable.  In this say, all
** root pages are kept at the beginning of the database file, which
** is necessary for AUTOVACUUM to work right.  piMoved is set to the
** page number that used to be the last root page in the file before
** the move.  If no page gets moved, piMoved is set to 0.
** The last root page is recorded in meta[3] and the value of
** meta[3] is updated by this procedure.
*/
static int btreeDropTable( Btree p, Pgno iTable, ref int piMoved )
{
  int rc;
  MemPage pPage = null;
  BtShared pBt = p.pBt;

  Debug.Assert( sqlite3BtreeHoldsMutex( p ) );
  Debug.Assert( p.inTrans == TRANS_WRITE );

  /* It is illegal to drop a table if any cursors are open on the
  ** database. This is because in auto-vacuum mode the backend may
  ** need to move another root-page to fill a gap left by the deleted
  ** root page. If an open cursor was using this page a problem would
  ** occur.
  **
  ** This error is caught long before control reaches this point.
  */
  if ( NEVER( pBt.pCursor ) )
  {
    sqlite3ConnectionBlocked( p.db, pBt.pCursor.pBtree.db );
    return SQLITE_LOCKED_SHAREDCACHE;
  }

  rc = btreeGetPage( pBt, (Pgno)iTable, ref pPage, 0 );
  if ( rc != 0 )
    return rc;
  int Dummy0 = 0;
  rc = sqlite3BtreeClearTable( p, (int)iTable, ref Dummy0 );
  if ( rc != 0 )
  {
    releasePage( pPage );
    return rc;
  }

  piMoved = 0;

  if ( iTable > 1 )
  {
#if SQLITE_OMIT_AUTOVACUUM
freePage(pPage, ref rc);
releasePage(pPage);
#else
    if ( pBt.autoVacuum )
    {
      Pgno maxRootPgno = 0;
      sqlite3BtreeGetMeta( p, BTREE_LARGEST_ROOT_PAGE, ref maxRootPgno );

      if ( iTable == maxRootPgno )
      {
        /* If the table being dropped is the table with the largest root-page
        ** number in the database, put the root page on the free list.
        */
        freePage( pPage, ref rc );
        releasePage( pPage );
        if ( rc != SQLITE_OK )
        {
          return rc;
        }
      }
      else
      {
        /* The table being dropped does not have the largest root-page
        ** number in the database. So move the page that does into the
        ** gap left by the deleted root-page.
        */
        MemPage pMove = new MemPage();
        releasePage( pPage );
        rc = btreeGetPage( pBt, maxRootPgno, ref pMove, 0 );
        if ( rc != SQLITE_OK )
        {
          return rc;
        }
        rc = relocatePage( pBt, pMove, PTRMAP_ROOTPAGE, 0, iTable, 0 );
        releasePage( pMove );
        if ( rc != SQLITE_OK )
        {
          return rc;
        }
        pMove = null;
        rc = btreeGetPage( pBt, maxRootPgno, ref pMove, 0 );
        freePage( pMove, ref rc );
        releasePage( pMove );
        if ( rc != SQLITE_OK )
        {
          return rc;
        }
        piMoved = (int)maxRootPgno;
      }

      /* Set the new 'max-root-page' value in the database header. This
      ** is the old value less one, less one more if that happens to
      ** be a root-page number, less one again if that is the
      ** PENDING_BYTE_PAGE.
      */
      maxRootPgno--;
      while ( maxRootPgno == PENDING_BYTE_PAGE( pBt )
      || PTRMAP_ISPAGE( pBt, maxRootPgno ) )
      {
        maxRootPgno--;
      }
      Debug.Assert( maxRootPgno != PENDING_BYTE_PAGE( pBt ) );

      rc = sqlite3BtreeUpdateMeta( p, 4, maxRootPgno );
    }
    else
    {
      freePage( pPage, ref rc );
      releasePage( pPage );
    }
#endif
  }
  else
  {
    /* If sqlite3BtreeDropTable was called on page 1.
    ** This really never should happen except in a corrupt
    ** database.
    */
    zeroPage( pPage, PTF_INTKEY | PTF_LEAF );
    releasePage( pPage );
  }
  return rc;
}
static int sqlite3BtreeDropTable( Btree p, int iTable, ref int piMoved )
{
  int rc;
  sqlite3BtreeEnter( p );
  rc = btreeDropTable( p, (u32)iTable, ref piMoved );
  sqlite3BtreeLeave( p );
  return rc;
}


/*
** This function may only be called if the b-tree connection already
** has a read or write transaction open on the database.
**
** Read the meta-information out of a database file.  Meta[0]
** is the number of free pages currently in the database.  Meta[1]
** through meta[15] are available for use by higher layers.  Meta[0]
** is read-only, the others are read/write.
**
** The schema layer numbers meta values differently.  At the schema
** layer (and the SetCookie and ReadCookie opcodes) the number of
** free pages is not visible.  So Cookie[0] is the same as Meta[1].
*/
static void sqlite3BtreeGetMeta( Btree p, int idx, ref u32 pMeta )
{
  BtShared pBt = p.pBt;

  sqlite3BtreeEnter( p );
  Debug.Assert( p.inTrans > TRANS_NONE );
  Debug.Assert( SQLITE_OK == querySharedCacheTableLock( p, MASTER_ROOT, READ_LOCK ) );
  Debug.Assert( pBt.pPage1 != null );
  Debug.Assert( idx >= 0 && idx <= 15 );

  pMeta = sqlite3Get4byte( pBt.pPage1.aData, 36 + idx * 4 );

  /* If auto-vacuum is disabled in this build and this is an auto-vacuum
  ** database, mark the database as read-only.  */
#if SQLITE_OMIT_AUTOVACUUM
if( idx==BTREE_LARGEST_ROOT_PAGE && pMeta>0 ) pBt.readOnly = 1;
#endif

  sqlite3BtreeLeave( p );
}

/*
** Write meta-information back into the database.  Meta[0] is
** read-only and may not be written.
*/
static int sqlite3BtreeUpdateMeta( Btree p, int idx, u32 iMeta )
{
  BtShared pBt = p.pBt;
  byte[] pP1;
  int rc;
  Debug.Assert( idx >= 1 && idx <= 15 );
  sqlite3BtreeEnter( p );
  Debug.Assert( p.inTrans == TRANS_WRITE );
  Debug.Assert( pBt.pPage1 != null );
  pP1 = pBt.pPage1.aData;
  rc = sqlite3PagerWrite( pBt.pPage1.pDbPage );
  if ( rc == SQLITE_OK )
  {
    sqlite3Put4byte( pP1, 36 + idx * 4, iMeta );
#if !SQLITE_OMIT_AUTOVACUUM
    if ( idx == BTREE_INCR_VACUUM )
    {
      Debug.Assert( pBt.autoVacuum || iMeta == 0 );
      Debug.Assert( iMeta == 0 || iMeta == 1 );
      pBt.incrVacuum = iMeta != 0;
    }
#endif
  }
  sqlite3BtreeLeave( p );
  return rc;
}

#if !SQLITE_OMIT_BTREECOUNT
/*
** The first argument, pCur, is a cursor opened on some b-tree. Count the
** number of entries in the b-tree and write the result to pnEntry.
**
** SQLITE_OK is returned if the operation is successfully executed.
** Otherwise, if an error is encountered (i.e. an IO error or database
** corruption) an SQLite error code is returned.
*/
static int sqlite3BtreeCount( BtCursor pCur, ref i64 pnEntry )
{
  i64 nEntry = 0;                      /* Value to return in pnEntry */
  int rc;                              /* Return code */
  rc = moveToRoot( pCur );

  /* Unless an error occurs, the following loop runs one iteration for each
  ** page in the B-Tree structure (not including overflow pages).
  */
  while ( rc == SQLITE_OK )
  {
    int iIdx;                          /* Index of child node in parent */
    MemPage pPage;                    /* Current page of the b-tree */

    /* If this is a leaf page or the tree is not an int-key tree, then
    ** this page contains countable entries. Increment the entry counter
    ** accordingly.
    */
    pPage = pCur.apPage[pCur.iPage];
    if ( pPage.leaf != 0 || 0 == pPage.intKey )
    {
      nEntry += pPage.nCell;
    }

    /* pPage is a leaf node. This loop navigates the cursor so that it
    ** points to the first interior cell that it points to the parent of
    ** the next page in the tree that has not yet been visited. The
    ** pCur.aiIdx[pCur.iPage] value is set to the index of the parent cell
    ** of the page, or to the number of cells in the page if the next page
    ** to visit is the right-child of its parent.
    **
    ** If all pages in the tree have been visited, return SQLITE_OK to the
    ** caller.
    */
    if ( pPage.leaf != 0 )
    {
      do
      {
        if ( pCur.iPage == 0 )
        {
          /* All pages of the b-tree have been visited. Return successfully. */
          pnEntry = nEntry;
          return SQLITE_OK;
        }
        moveToParent( pCur );
      } while ( pCur.aiIdx[pCur.iPage] >= pCur.apPage[pCur.iPage].nCell );

      pCur.aiIdx[pCur.iPage]++;
      pPage = pCur.apPage[pCur.iPage];
    }

    /* Descend to the child node of the cell that the cursor currently
    ** points at. This is the right-child if (iIdx==pPage.nCell).
    */
    iIdx = pCur.aiIdx[pCur.iPage];
    if ( iIdx == pPage.nCell )
    {
      rc = moveToChild( pCur, sqlite3Get4byte( pPage.aData, pPage.hdrOffset + 8 ) );
    }
    else
    {
      rc = moveToChild( pCur, sqlite3Get4byte( pPage.aData, findCell( pPage, iIdx ) ) );
    }
  }

  /* An error has occurred. Return an error code. */
  return rc;
}
#endif

/*
** Return the pager associated with a BTree.  This routine is used for
** testing and debugging only.
*/
static Pager sqlite3BtreePager( Btree p )
{
  return p.pBt.pPager;
}

#if !SQLITE_OMIT_INTEGRITY_CHECK
/*
** Append a message to the error message string.
*/
static void checkAppendMsg(
IntegrityCk pCheck,
string zMsg1,
string zFormat,
params object[] ap
)
{
  if ( 0 == pCheck.mxErr )
    return;
  //va_list ap;
  lock ( lock_va_list )
  {
    pCheck.mxErr--;
    pCheck.nErr++;
    va_start( ap, zFormat );
    if ( pCheck.errMsg.zText.Length != 0 )
    {
      sqlite3StrAccumAppend( pCheck.errMsg, "\n", 1 );
    }
    if ( zMsg1.Length > 0 )
    {
      sqlite3StrAccumAppend( pCheck.errMsg, zMsg1.ToString(), -1 );
    }
    sqlite3VXPrintf( pCheck.errMsg, 1, zFormat, ap );
    va_end( ref ap );
  }
}

static void checkAppendMsg(
IntegrityCk pCheck,
StringBuilder zMsg1,
string zFormat,
params object[] ap
)
{
  if ( 0 == pCheck.mxErr )
    return;
  //va_list ap;
  lock ( lock_va_list )
  {
    pCheck.mxErr--;
    pCheck.nErr++;
    va_start( ap, zFormat );
    if ( pCheck.errMsg.zText.Length != 0 )
    {
      sqlite3StrAccumAppend( pCheck.errMsg, "\n", 1 );
    }
    if ( zMsg1.Length > 0 )
    {
      sqlite3StrAccumAppend( pCheck.errMsg, zMsg1.ToString(), -1 );
    }
    sqlite3VXPrintf( pCheck.errMsg, 1, zFormat, ap );
    va_end( ref ap );
  }      
  //if( pCheck.errMsg.mallocFailed ){
  //  pCheck.mallocFailed = 1;
  //}
}
#endif //* SQLITE_OMIT_INTEGRITY_CHECK */

#if !SQLITE_OMIT_INTEGRITY_CHECK
/*
** Add 1 to the reference count for page iPage.  If this is the second
** reference to the page, add an error message to pCheck.zErrMsg.
** Return 1 if there are 2 ore more references to the page and 0 if
** if this is the first reference to the page.
**
** Also check that the page number is in bounds.
*/
static int checkRef( IntegrityCk pCheck, Pgno iPage, string zContext )
{
  if ( iPage == 0 )
    return 1;
  if ( iPage > pCheck.nPage )
  {
    checkAppendMsg( pCheck, zContext, "invalid page number %d", iPage );
    return 1;
  }
  if ( pCheck.anRef[iPage] == 1 )
  {
    checkAppendMsg( pCheck, zContext, "2nd reference to page %d", iPage );
    return 1;
  }
  return ( ( pCheck.anRef[iPage]++ ) > 1 ) ? 1 : 0;
}

#if !SQLITE_OMIT_AUTOVACUUM
/*
** Check that the entry in the pointer-map for page iChild maps to
** page iParent, pointer type ptrType. If not, append an error message
** to pCheck.
*/
static void checkPtrmap(
IntegrityCk pCheck,    /* Integrity check context */
Pgno iChild,           /* Child page number */
u8 eType,              /* Expected pointer map type */
Pgno iParent,          /* Expected pointer map parent page number */
string zContext /* Context description (used for error msg) */
)
{
  int rc;
  u8 ePtrmapType = 0;
  Pgno iPtrmapParent = 0;

  rc = ptrmapGet( pCheck.pBt, iChild, ref ePtrmapType, ref iPtrmapParent );
  if ( rc != SQLITE_OK )
  {
    //if( rc==SQLITE_NOMEM || rc==SQLITE_IOERR_NOMEM ) pCheck.mallocFailed = 1;
    checkAppendMsg( pCheck, zContext, "Failed to read ptrmap key=%d", iChild );
    return;
  }

  if ( ePtrmapType != eType || iPtrmapParent != iParent )
  {
    checkAppendMsg( pCheck, zContext,
    "Bad ptr map entry key=%d expected=(%d,%d) got=(%d,%d)",
    iChild, eType, iParent, ePtrmapType, iPtrmapParent );
  }
}
#endif

/*
** Check the integrity of the freelist or of an overflow page list.
** Verify that the number of pages on the list is N.
*/
static void checkList(
IntegrityCk pCheck,  /* Integrity checking context */
int isFreeList,       /* True for a freelist.  False for overflow page list */
int iPage,            /* Page number for first page in the list */
int N,                /* Expected number of pages in the list */
string zContext        /* Context for error messages */
)
{
  int i;
  int expected = N;
  int iFirst = iPage;
  while ( N-- > 0 && pCheck.mxErr != 0 )
  {
    PgHdr pOvflPage = new PgHdr();
    byte[] pOvflData;
    if ( iPage < 1 )
    {
      checkAppendMsg( pCheck, zContext,
      "%d of %d pages missing from overflow list starting at %d",
      N + 1, expected, iFirst );
      break;
    }
    if ( checkRef( pCheck, (u32)iPage, zContext ) != 0 )
      break;
    if ( sqlite3PagerGet( pCheck.pPager, (Pgno)iPage, ref pOvflPage ) != 0 )
    {
      checkAppendMsg( pCheck, zContext, "failed to get page %d", iPage );
      break;
    }
    pOvflData = sqlite3PagerGetData( pOvflPage );
    if ( isFreeList != 0 )
    {
      int n = (int)sqlite3Get4byte( pOvflData, 4 );
#if !SQLITE_OMIT_AUTOVACUUM
      if ( pCheck.pBt.autoVacuum )
      {
        checkPtrmap( pCheck, (u32)iPage, PTRMAP_FREEPAGE, 0, zContext );
      }
#endif
      if ( n > (int)pCheck.pBt.usableSize / 4 - 2 )
      {
        checkAppendMsg( pCheck, zContext,
        "freelist leaf count too big on page %d", iPage );
        N--;
      }
      else
      {
        for ( i = 0; i < n; i++ )
        {
          Pgno iFreePage = sqlite3Get4byte( pOvflData, 8 + i * 4 );
#if !SQLITE_OMIT_AUTOVACUUM
          if ( pCheck.pBt.autoVacuum )
          {
            checkPtrmap( pCheck, iFreePage, PTRMAP_FREEPAGE, 0, zContext );
          }
#endif
          checkRef( pCheck, iFreePage, zContext );
        }
        N -= n;
      }
    }
#if !SQLITE_OMIT_AUTOVACUUM
    else
    {
      /* If this database supports auto-vacuum and iPage is not the last
      ** page in this overflow list, check that the pointer-map entry for
      ** the following page matches iPage.
      */
      if ( pCheck.pBt.autoVacuum && N > 0 )
      {
        i = (int)sqlite3Get4byte( pOvflData );
        checkPtrmap( pCheck, (u32)i, PTRMAP_OVERFLOW2, (u32)iPage, zContext );
      }
    }
#endif
    iPage = (int)sqlite3Get4byte( pOvflData );
    sqlite3PagerUnref( pOvflPage );
  }
}
#endif //* SQLITE_OMIT_INTEGRITY_CHECK */

#if !SQLITE_OMIT_INTEGRITY_CHECK
/*
** Do various sanity checks on a single page of a tree.  Return
** the tree depth.  Root pages return 0.  Parents of root pages
** return 1, and so forth.
**
** These checks are done:
**
**      1.  Make sure that cells and freeblocks do not overlap
**          but combine to completely cover the page.
**  NO  2.  Make sure cell keys are in order.
**  NO  3.  Make sure no key is less than or equal to zLowerBound.
**  NO  4.  Make sure no key is greater than or equal to zUpperBound.
**      5.  Check the integrity of overflow pages.
**      6.  Recursively call checkTreePage on all children.
**      7.  Verify that the depth of all children is the same.
**      8.  Make sure this page is at least 33% full or else it is
**          the root of the tree.
*/

static i64 refNULL = 0;   //Dummy for C# ref NULL

static int checkTreePage(
IntegrityCk pCheck,    /* Context for the sanity check */
int iPage,             /* Page number of the page to check */
string zParentContext, /* Parent context */
ref i64 pnParentMinKey,
ref i64 pnParentMaxKey,
object _pnParentMinKey, /* C# Needed to determine if content passed*/
object _pnParentMaxKey  /* C# Needed to determine if content passed*/
)
{
  MemPage pPage = new MemPage();
  int i, rc, depth, d2, pgno, cnt;
  int hdr, cellStart;
  int nCell;
  u8[] data;
  BtShared pBt;
  int usableSize;
  StringBuilder zContext = new StringBuilder( 100 );
  byte[] hit = null;
  i64 nMinKey = 0;
  i64 nMaxKey = 0;


  sqlite3_snprintf( 200, zContext, "Page %d: ", iPage );

  /* Check that the page exists
  */
  pBt = pCheck.pBt;
  usableSize = (int)pBt.usableSize;
  if ( iPage == 0 )
    return 0;
  if ( checkRef( pCheck, (u32)iPage, zParentContext ) != 0 )
    return 0;
  if ( ( rc = btreeGetPage( pBt, (Pgno)iPage, ref pPage, 0 ) ) != 0 )
  {
    checkAppendMsg( pCheck, zContext.ToString(),
    "unable to get the page. error code=%d", rc );
    return 0;
  }

  /* Clear MemPage.isInit to make sure the corruption detection code in
  ** btreeInitPage() is executed.  */
  pPage.isInit = 0;
  if ( ( rc = btreeInitPage( pPage ) ) != 0 )
  {
    Debug.Assert( rc == SQLITE_CORRUPT );  /* The only possible error from InitPage */
    checkAppendMsg( pCheck, zContext.ToString(),
    "btreeInitPage() returns error code %d", rc );
    releasePage( pPage );
    return 0;
  }

  /* Check out all the cells.
  */
  depth = 0;
  for ( i = 0; i < pPage.nCell && pCheck.mxErr != 0; i++ )
  {
    u8[] pCell;
    u32 sz;
    CellInfo info = new CellInfo();

    /* Check payload overflow pages
    */
    sqlite3_snprintf( 200, zContext,
    "On tree page %d cell %d: ", iPage, i );
    int iCell = findCell( pPage, i ); //pCell = findCell( pPage, i );
    pCell = pPage.aData;
    btreeParseCellPtr( pPage, iCell, ref info ); //btreeParseCellPtr( pPage, pCell, info );
    sz = info.Data;
    if ( 0 == pPage.intKey )
      sz += (u32)info.nKey;
    /* For intKey pages, check that the keys are in order.
    */
    else if ( i == 0 )
      nMinKey = nMaxKey = info.nKey;
    else
    {
      if ( info.nKey <= nMaxKey )
      {
        checkAppendMsg( pCheck, zContext.ToString(),
        "Rowid %lld out of order (previous was %lld)", info.nKey, nMaxKey );
      }
      nMaxKey = info.nKey;
    }
    Debug.Assert( sz == info.Payload );
    if ( ( sz > info.Local )
      //&& (pCell[info.iOverflow]<=&pPage.aData[pBt.usableSize])
    )
    {
      int nPage = (int)( sz - info.Local + usableSize - 5 ) / ( usableSize - 4 );
      Pgno pgnoOvfl = sqlite3Get4byte( pCell, iCell, info.Overflow );
#if !SQLITE_OMIT_AUTOVACUUM
      if ( pBt.autoVacuum )
      {
        checkPtrmap( pCheck, pgnoOvfl, PTRMAP_OVERFLOW1, (u32)iPage, zContext.ToString() );
      }
#endif
      checkList( pCheck, 0, (int)pgnoOvfl, nPage, zContext.ToString() );
    }

    /* Check sanity of left child page.
    */
    if ( 0 == pPage.leaf )
    {
      pgno = (int)sqlite3Get4byte( pCell, iCell ); //sqlite3Get4byte( pCell );
#if !SQLITE_OMIT_AUTOVACUUM
      if ( pBt.autoVacuum )
      {
        checkPtrmap( pCheck, (u32)pgno, PTRMAP_BTREE, (u32)iPage, zContext.ToString() );
      }
#endif
      if ( i == 0 )
        d2 = checkTreePage( pCheck, pgno, zContext.ToString(), ref nMinKey, ref refNULL, pCheck, null );
      else
        d2 = checkTreePage( pCheck, pgno, zContext.ToString(), ref nMinKey, ref nMaxKey, pCheck, pCheck );

      if ( i > 0 && d2 != depth )
      {
        checkAppendMsg( pCheck, zContext, "Child page depth differs" );
      }
      depth = d2;
    }
  }
  if ( 0 == pPage.leaf )
  {
    pgno = (int)sqlite3Get4byte( pPage.aData, pPage.hdrOffset + 8 );
    sqlite3_snprintf( 200, zContext,
    "On page %d at right child: ", iPage );
#if !SQLITE_OMIT_AUTOVACUUM
    if ( pBt.autoVacuum )
    {
      checkPtrmap( pCheck, (u32)pgno, PTRMAP_BTREE, (u32)iPage, zContext.ToString() );
    }
#endif
    //    checkTreePage(pCheck, pgno, zContext, NULL, !pPage->nCell ? NULL : &nMaxKey);
    if ( 0 == pPage.nCell )
      checkTreePage( pCheck, pgno, zContext.ToString(), ref refNULL, ref refNULL, null, null );
    else
      checkTreePage( pCheck, pgno, zContext.ToString(), ref refNULL, ref nMaxKey, null, pCheck );
  }

  /* For intKey leaf pages, check that the min/max keys are in order
  ** with any left/parent/right pages.
  */
  if ( pPage.leaf != 0 && pPage.intKey != 0 )
  {
    /* if we are a left child page */
    if ( _pnParentMinKey != null )
    {
      /* if we are the left most child page */
      if ( _pnParentMaxKey == null )
      {
        if ( nMaxKey > pnParentMinKey )
        {
          checkAppendMsg( pCheck, zContext,
          "Rowid %lld out of order (max larger than parent min of %lld)",
          nMaxKey, pnParentMinKey );
        }
      }
      else
      {
        if ( nMinKey <= pnParentMinKey )
        {
          checkAppendMsg( pCheck, zContext,
          "Rowid %lld out of order (min less than parent min of %lld)",
          nMinKey, pnParentMinKey );
        }
        if ( nMaxKey > pnParentMaxKey )
        {
          checkAppendMsg( pCheck, zContext,
          "Rowid %lld out of order (max larger than parent max of %lld)",
          nMaxKey, pnParentMaxKey );
        }
        pnParentMinKey = nMaxKey;
      }
      /* else if we're a right child page */
    }
    else if ( _pnParentMaxKey != null )
    {
      if ( nMinKey <= pnParentMaxKey )
      {
        checkAppendMsg( pCheck, zContext,
        "Rowid %lld out of order (min less than parent max of %lld)",
        nMinKey, pnParentMaxKey );
      }
    }
  }

  /* Check for complete coverage of the page
  */
  data = pPage.aData;
  hdr = pPage.hdrOffset;
  hit = sqlite3Malloc( pBt.pageSize );
  //if( hit==null ){
  //  pCheck.mallocFailed = 1;
  //}else
  {
    int contentOffset = get2byteNotZero( data, hdr + 5 );
    Debug.Assert( contentOffset <= usableSize );  /* Enforced by btreeInitPage() */
    Array.Clear( hit, contentOffset, usableSize - contentOffset );//memset(hit+contentOffset, 0, usableSize-contentOffset);
    for ( int iLoop = contentOffset - 1; iLoop >= 0; iLoop-- )
      hit[iLoop] = 1;//memset(hit, 1, contentOffset);
    nCell = get2byte( data, hdr + 3 );
    cellStart = hdr + 12 - 4 * pPage.leaf;
    for ( i = 0; i < nCell; i++ )
    {
      int pc = get2byte( data, cellStart + i * 2 );
      u32 size = 65536;
      int j;
      if ( pc <= usableSize - 4 )
      {
        size = cellSizePtr( pPage, data, pc );
      }
      if ( (int)( pc + size - 1 ) >= usableSize )
      {
        checkAppendMsg( pCheck, "",
        "Corruption detected in cell %d on page %d", i, iPage );
      }
      else
      {
        for ( j = (int)( pc + size - 1 ); j >= pc; j-- )
          hit[j]++;
      }
    }
    i = get2byte( data, hdr + 1 );
    while ( i > 0 )
    {
      int size, j;
      Debug.Assert( i <= usableSize - 4 );     /* Enforced by btreeInitPage() */
      size = get2byte( data, i + 2 );
      Debug.Assert( i + size <= usableSize );  /* Enforced by btreeInitPage() */
      for ( j = i + size - 1; j >= i; j-- )
        hit[j]++;
      j = get2byte( data, i );
      Debug.Assert( j == 0 || j > i + size );  /* Enforced by btreeInitPage() */
      Debug.Assert( j <= usableSize - 4 );   /* Enforced by btreeInitPage() */
      i = j;
    }
    for ( i = cnt = 0; i < usableSize; i++ )
    {
      if ( hit[i] == 0 )
      {
        cnt++;
      }
      else if ( hit[i] > 1 )
      {
        checkAppendMsg( pCheck, "",
        "Multiple uses for byte %d of page %d", i, iPage );
        break;
      }
    }
    if ( cnt != data[hdr + 7] )
    {
      checkAppendMsg( pCheck, "",
      "Fragmentation of %d bytes reported as %d on page %d",
      cnt, data[hdr + 7], iPage );
    }
  }
  sqlite3PageFree( ref hit );
  releasePage( pPage );
  return depth + 1;
}
#endif //* SQLITE_OMIT_INTEGRITY_CHECK */

#if !SQLITE_OMIT_INTEGRITY_CHECK
/*
** This routine does a complete check of the given BTree file.  aRoot[] is
** an array of pages numbers were each page number is the root page of
** a table.  nRoot is the number of entries in aRoot.
**
** A read-only or read-write transaction must be opened before calling
** this function.
**
** Write the number of error seen in pnErr.  Except for some memory
** allocation errors,  an error message held in memory obtained from
** malloc is returned if pnErr is non-zero.  If pnErr==null then NULL is
** returned.  If a memory allocation error occurs, NULL is returned.
*/
static string sqlite3BtreeIntegrityCheck(
Btree p,       /* The btree to be checked */
int[] aRoot,   /* An array of root pages numbers for individual trees */
int nRoot,     /* Number of entries in aRoot[] */
int mxErr,     /* Stop reporting errors after this many */
ref int pnErr  /* Write number of errors seen to this variable */
)
{
  Pgno i;
  int nRef;
  IntegrityCk sCheck = new IntegrityCk();
  BtShared pBt = p.pBt;

  sqlite3BtreeEnter( p );
  Debug.Assert( p.inTrans > TRANS_NONE && pBt.inTransaction > TRANS_NONE );
  nRef = sqlite3PagerRefcount( pBt.pPager );
  sCheck.pBt = pBt;
  sCheck.pPager = pBt.pPager;
  sCheck.nPage = btreePagecount( sCheck.pBt );
  sCheck.mxErr = mxErr;
  sCheck.nErr = 0;
  //sCheck.mallocFailed = 0;
  pnErr = 0;
  if ( sCheck.nPage == 0 )
  {
    sqlite3BtreeLeave( p );
    return "";
  }
  sCheck.anRef = sqlite3Malloc( sCheck.anRef, (int)sCheck.nPage + 1 );
  //if( !sCheck.anRef ){
  //  pnErr = 1;
  //  sqlite3BtreeLeave(p);
  //  return 0;
  //}
  // for (i = 0; i <= sCheck.nPage; i++) { sCheck.anRef[i] = 0; }
  i = PENDING_BYTE_PAGE( pBt );
  if ( i <= sCheck.nPage )
  {
    sCheck.anRef[i] = 1;
  }
  sqlite3StrAccumInit( sCheck.errMsg, null, 1000, 20000 );
  //sCheck.errMsg.useMalloc = 2;

  /* Check the integrity of the freelist
  */
  checkList( sCheck, 1, (int)sqlite3Get4byte( pBt.pPage1.aData, 32 ),
  (int)sqlite3Get4byte( pBt.pPage1.aData, 36 ), "Main freelist: " );

  /* Check all the tables.
  */
  for ( i = 0; (int)i < nRoot && sCheck.mxErr != 0; i++ )
  {
    if ( aRoot[i] == 0 )
      continue;
#if !SQLITE_OMIT_AUTOVACUUM
    if ( pBt.autoVacuum && aRoot[i] > 1 )
    {
      checkPtrmap( sCheck, (u32)aRoot[i], PTRMAP_ROOTPAGE, 0, "" );
    }
#endif
    checkTreePage( sCheck, aRoot[i], "List of tree roots: ", ref refNULL, ref refNULL, null, null );
  }

  /* Make sure every page in the file is referenced
  */
  for ( i = 1; i <= sCheck.nPage && sCheck.mxErr != 0; i++ )
  {
#if SQLITE_OMIT_AUTOVACUUM
if( sCheck.anRef[i]==null ){
checkAppendMsg(sCheck, 0, "Page %d is never used", i);
}
#else
    /* If the database supports auto-vacuum, make sure no tables contain
** references to pointer-map pages.
*/
    if ( sCheck.anRef[i] == 0 &&
    ( PTRMAP_PAGENO( pBt, i ) != i || !pBt.autoVacuum ) )
    {
      checkAppendMsg( sCheck, "", "Page %d is never used", i );
    }
    if ( sCheck.anRef[i] != 0 &&
    ( PTRMAP_PAGENO( pBt, i ) == i && pBt.autoVacuum ) )
    {
      checkAppendMsg( sCheck, "", "Pointer map page %d is referenced", i );
    }
#endif
  }

  /* Make sure this analysis did not leave any unref() pages.
  ** This is an internal consistency check; an integrity check
  ** of the integrity check.
  */
  if ( NEVER( nRef != sqlite3PagerRefcount( pBt.pPager ) ) )
  {
    checkAppendMsg( sCheck, "",
    "Outstanding page count goes from %d to %d during this analysis",
    nRef, sqlite3PagerRefcount( pBt.pPager )
    );
  }

  /* Clean  up and report errors.
  */
  sqlite3BtreeLeave( p );
  sCheck.anRef = null;// sqlite3_free( ref sCheck.anRef );
  //if( sCheck.mallocFailed ){
  //  sqlite3StrAccumReset(sCheck.errMsg);
  //  pnErr = sCheck.nErr+1;
  //  return 0;
  //}
  pnErr = sCheck.nErr;
  if ( sCheck.nErr == 0 )
    sqlite3StrAccumReset( sCheck.errMsg );
  return sqlite3StrAccumFinish( sCheck.errMsg );
}
#endif //* SQLITE_OMIT_INTEGRITY_CHECK */

/*
** Return the full pathname of the underlying database file.
**
** The pager filename is invariant as long as the pager is
** open so it is safe to access without the BtShared mutex.
*/
static string sqlite3BtreeGetFilename( Btree p )
{
  Debug.Assert( p.pBt.pPager != null );
  return sqlite3PagerFilename( p.pBt.pPager );
}

/*
** Return the pathname of the journal file for this database. The return
** value of this routine is the same regardless of whether the journal file
** has been created or not.
**
** The pager journal filename is invariant as long as the pager is
** open so it is safe to access without the BtShared mutex.
*/
static string sqlite3BtreeGetJournalname( Btree p )
{
  Debug.Assert( p.pBt.pPager != null );
  return sqlite3PagerJournalname( p.pBt.pPager );
}

/*
** Return non-zero if a transaction is active.
*/
static bool sqlite3BtreeIsInTrans( Btree p )
{
  Debug.Assert( p == null || sqlite3_mutex_held( p.db.mutex ) );
  return ( p != null && ( p.inTrans == TRANS_WRITE ) );
}

#if !SQLITE_OMIT_WAL
/*
** Run a checkpoint on the Btree passed as the first argument.
**
** Return SQLITE_LOCKED if this or any other connection has an open 
** transaction on the shared-cache the argument Btree is connected to.
**
** Parameter eMode is one of SQLITE_CHECKPOINT_PASSIVE, FULL or RESTART.
*/
static int sqlite3BtreeCheckpointBtree *p, int eMode, int *pnLog, int *pnCkpt){
int rc = SQLITE_OK;
if( p != null){
BtShared pBt = p.pBt;
sqlite3BtreeEnter(p);
if( pBt.inTransaction!=TRANS_NONE ){
rc = SQLITE_LOCKED;
}else{
rc = sqlite3PagerCheckpoint(pBt.pPager, eMode, pnLog, pnCkpt);
}
sqlite3BtreeLeave(p);
}
return rc;
}
#endif

/*
** Return non-zero if a read (or write) transaction is active.
*/
static bool sqlite3BtreeIsInReadTrans( Btree p )
{
  Debug.Assert( p != null );
  Debug.Assert( sqlite3_mutex_held( p.db.mutex ) );
  return p.inTrans != TRANS_NONE;
}

static bool sqlite3BtreeIsInBackup( Btree p )
{
  Debug.Assert( p != null );
  Debug.Assert( sqlite3_mutex_held( p.db.mutex ) );
  return p.nBackup != 0;
}

/*
** This function returns a pointer to a blob of memory associated with
** a single shared-btree. The memory is used by client code for its own
** purposes (for example, to store a high-level schema associated with
** the shared-btree). The btree layer manages reference counting issues.
**
** The first time this is called on a shared-btree, nBytes bytes of memory
** are allocated, zeroed, and returned to the caller. For each subsequent
** call the nBytes parameter is ignored and a pointer to the same blob
** of memory returned.
**
** If the nBytes parameter is 0 and the blob of memory has not yet been
** allocated, a null pointer is returned. If the blob has already been
** allocated, it is returned as normal.
**
** Just before the shared-btree is closed, the function passed as the 
** xFree argument when the memory allocation was made is invoked on the 
** blob of allocated memory. The xFree function should not call sqlite3_free()
** on the memory, the btree layer does that.
*/
static Schema sqlite3BtreeSchema( Btree p, int nBytes, dxFreeSchema xFree )
{
  BtShared pBt = p.pBt;
  sqlite3BtreeEnter( p );
  if ( null == pBt.pSchema && nBytes != 0 )
  {
    pBt.pSchema = new Schema();//sqlite3DbMallocZero(0, nBytes);
    pBt.xFreeSchema = xFree;
  }
  sqlite3BtreeLeave( p );
  return pBt.pSchema;
}

/*
** Return SQLITE_LOCKED_SHAREDCACHE if another user of the same shared
** btree as the argument handle holds an exclusive lock on the
** sqlite_master table. Otherwise SQLITE_OK.
*/
static int sqlite3BtreeSchemaLocked( Btree p )
{
  int rc;
  Debug.Assert( sqlite3_mutex_held( p.db.mutex ) );
  sqlite3BtreeEnter( p );
  rc = querySharedCacheTableLock( p, MASTER_ROOT, READ_LOCK );
  Debug.Assert( rc == SQLITE_OK || rc == SQLITE_LOCKED_SHAREDCACHE );
  sqlite3BtreeLeave( p );
  return rc;
}


#if !SQLITE_OMIT_SHARED_CACHE
/*
** Obtain a lock on the table whose root page is iTab.  The
** lock is a write lock if isWritelock is true or a read lock
** if it is false.
*/
int sqlite3BtreeLockTable(Btree p, int iTab, u8 isWriteLock){
int rc = SQLITE_OK;
Debug.Assert( p.inTrans!=TRANS_NONE );
if( p.sharable ){
u8 lockType = READ_LOCK + isWriteLock;
Debug.Assert( READ_LOCK+1==WRITE_LOCK );
Debug.Assert( isWriteLock==null || isWriteLock==1 );

sqlite3BtreeEnter(p);
rc = querySharedCacheTableLock(p, iTab, lockType);
if( rc==SQLITE_OK ){
rc = setSharedCacheTableLock(p, iTab, lockType);
}
sqlite3BtreeLeave(p);
}
return rc;
}
#endif

#if !SQLITE_OMIT_INCRBLOB
/*
** Argument pCsr must be a cursor opened for writing on an
** INTKEY table currently pointing at a valid table entry.
** This function modifies the data stored as part of that entry.
**
** Only the data content may only be modified, it is not possible to
** change the length of the data stored. If this function is called with
** parameters that attempt to write past the end of the existing data,
** no modifications are made and SQLITE_CORRUPT is returned.
*/
int sqlite3BtreePutData(BtCursor pCsr, u32 offset, u32 amt, void *z){
int rc;
Debug.Assert( cursorHoldsMutex(pCsr) );
Debug.Assert( sqlite3_mutex_held(pCsr.pBtree.db.mutex) );
Debug.Assert( pCsr.isIncrblobHandle );

rc = restoreCursorPosition(pCsr);
if( rc!=SQLITE_OK ){
return rc;
}
Debug.Assert( pCsr.eState!=CURSOR_REQUIRESEEK );
if( pCsr.eState!=CURSOR_VALID ){
return SQLITE_ABORT;
}

/* Check some assumptions:
**   (a) the cursor is open for writing,
**   (b) there is a read/write transaction open,
**   (c) the connection holds a write-lock on the table (if required),
**   (d) there are no conflicting read-locks, and
**   (e) the cursor points at a valid row of an intKey table.
*/
if( !pCsr.wrFlag ){
return SQLITE_READONLY;
}
Debug.Assert( !pCsr.pBt.readOnly && pCsr.pBt.inTransaction==TRANS_WRITE );
Debug.Assert( hasSharedCacheTableLock(pCsr.pBtree, pCsr.pgnoRoot, 0, 2) );
Debug.Assert( !hasReadConflicts(pCsr.pBtree, pCsr.pgnoRoot) );
Debug.Assert( pCsr.apPage[pCsr.iPage].intKey );

return accessPayload(pCsr, offset, amt, (byte[] *)z, 1);
}

/*
** Set a flag on this cursor to cache the locations of pages from the
** overflow list for the current row. This is used by cursors opened
** for incremental blob IO only.
**
** This function sets a flag only. The actual page location cache
** (stored in BtCursor.aOverflow[]) is allocated and used by function
** accessPayload() (the worker function for sqlite3BtreeData() and
** sqlite3BtreePutData()).
*/
static void sqlite3BtreeCacheOverflow(BtCursor pCur){
Debug.Assert( cursorHoldsMutex(pCur) );
Debug.Assert( sqlite3_mutex_held(pCur.pBtree.db.mutex) );
invalidateOverflowCache(pCur)
pCur.isIncrblobHandle = 1;
}
#endif

/*
** Set both the "read version" (single byte at byte offset 18) and 
** "write version" (single byte at byte offset 19) fields in the database
** header to iVersion.
*/
static int sqlite3BtreeSetVersion( Btree pBtree, int iVersion )
{
  BtShared pBt = pBtree.pBt;
  int rc;                         /* Return code */

  Debug.Assert( pBtree.inTrans == TRANS_NONE );
  Debug.Assert( iVersion == 1 || iVersion == 2 );

  /* If setting the version fields to 1, do not automatically open the
  ** WAL connection, even if the version fields are currently set to 2.
  */
  pBt.doNotUseWAL = iVersion == 1;

  rc = sqlite3BtreeBeginTrans( pBtree, 0 );
  if ( rc == SQLITE_OK )
  {
    u8[] aData = pBt.pPage1.aData;
    if ( aData[18] != (u8)iVersion || aData[19] != (u8)iVersion )
    {
      rc = sqlite3BtreeBeginTrans( pBtree, 2 );
      if ( rc == SQLITE_OK )
      {
        rc = sqlite3PagerWrite( pBt.pPage1.pDbPage );
        if ( rc == SQLITE_OK )
        {
          aData[18] = (u8)iVersion;
          aData[19] = (u8)iVersion;
        }
      }
    }
  }

  pBt.doNotUseWAL = false;
  return rc;
}
#endif
        //#endregion

    }
}
