using System;
using Pgno = System.UInt32;
using System.Diagnostics;
using System.Text;
namespace Core
{
    public partial class PCache1
    {
        private static void BufferSetup(object buffer, int size, int n)
        {
            if (pcache1.IsInit)
            {
                size = SysEx.ROUNDDOWN8(size);
                pcache1.SizeSlot = size;
                pcache1.Slots = pcache1.FreeSlots = n;
                pcache1.Reserves = (n > 90 ? 10 : (n / 10 + 1));
                pcache1.Start = buffer;
                pcache1.End = null;
                pcache1.Free = null;
                pcache1.UnderPressure = false;
                while (n-- > 0)
                {
                    var p = new PgFreeslot();
                    p.Next = pcache1.Free;
                    pcache1.Free = p;
                }
                pcache1.End = buffer;
            }
        }

        private static PgHdr Alloc(int bytes)
        {
            PgHdr p = null;
            Debug.Assert(MutexEx.NotHeld(pcache1.Mutex));
            StatusEx.StatusSet(StatusEx.STATUS.PAGECACHE_SIZE, bytes);
            if (bytes <= pcache1.SizeSlot)
            {
                MutexEx.Enter(pcache1.Mutex);
                p = pcache1.Free._PgHdr;
                if (p != null)
                {
                    pcache1.Free = pcache1.Free.Next;
                    pcache1.FreeSlots--;
                    pcache1.UnderPressure = (pcache1.FreeSlots < pcache1.Reserves);
                    Debug.Assert(pcache1.FreeSlots >= 0);
                    StatusEx.StatusAdd(StatusEx.STATUS.PAGECACHE_USED, 1);
                }
                MutexEx.Leave(pcache1.Mutex);
            }
            if (p == null)
            {
                // Memory is not available in the SQLITE_CONFIG_PAGECACHE pool.  Get it from sqlite3Malloc instead.
                p = new PgHdr();
                {
                    var sz = bytes;
                    MutexEx.Enter(pcache1.Mutex);
                    StatusEx.StatusAdd(StatusEx.STATUS.PAGECACHE_OVERFLOW, sz);
                    MutexEx.Leave(pcache1.Mutex);
                }
                SysEx.MemdebugSetType(p, SysEx.MEMTYPE.PCACHE);
            }
            return p;
        }

        private static int Free(ref PgHdr p)
        {
            int freed = 0;
            if (p == null)
                return 0;
            if (p.CacheAllocated)
            {
                MutexEx.Enter(pcache1.Mutex);
                StatusEx.StatusAdd(StatusEx.STATUS.PAGECACHE_USED, -1);
                var slot = new PgFreeslot(p);
                slot.Next = pcache1.Free;
                pcache1.Free = slot;
                pcache1.FreeSlots++;
                pcache1.UnderPressure = (pcache1.FreeSlots < pcache1.Reserves);
                Debug.Assert(pcache1.FreeSlots <= pcache1.Slots);
                MutexEx.Leave(pcache1.Mutex);
            }
            else
            {
                Debug.Assert(SysEx.MemdebugHasType(p, SysEx.MEMTYPE.PCACHE));
                SysEx.MemdebugSetType(p, SysEx.MEMTYPE.HEAP);
                freed = SysEx.MallocSize(p._Data);
                MutexEx.Enter(pcache1.Mutex);
                StatusEx.StatusAdd(StatusEx.STATUS.PAGECACHE_OVERFLOW, -freed);
                MutexEx.Leave(pcache1.Mutex);
                SysEx.Free(ref p._Data);
            }
            return freed;
        }

#if ENABLE_MEMORY_MANAGEMENT
        private static int MemSize(PgHdr p)
        {
            if (p.CacheAllocated)
                return pcache1.SizeSlot;
            else
            {
                Debug.Assert(SysEx.MemdebugHasType(p, SysEx.MEMTYPE.PCACHE));
                SysEx.MemdebugSetType(p, SysEx.MEMTYPE.HEAP);
                var size = SysEx.MallocSize(p._Data);
                SysEx.MemdebugSetType(p, SysEx.MEMTYPE.PCACHE);
                return size;
            }
        }
#endif

        private PgHdr1 AllocPage()
        {
            var pPg = Alloc(SizePage);
            var p = new PgHdr1();
            p.Page.Buffer = pPg;
            p.Page.Extra = null; // TODO: map for extra
            if (Purgeable)
                Group.CurrentPages++;
            return p;
        }

        private static void FreePage(ref PgHdr1 p)
        {
            if (SysEx.ALWAYS(p))
            {
                var cache = p.Cache;
                if (cache.Purgeable)
                    cache.Group.CurrentPages--;
                Free(ref p.Page);
            }
        }

        private static PgHdr PageMalloc(int sz) { return Alloc(sz); }

        public static void PageFree(ref byte[] p)
        {
            if (p != null)
            {
                SysEx.Free(ref p);
                p = null;
            }
        }
        public static void PageFree(ref PgHdr p) { Free(ref p); }

        private bool UnderMemoryPressure() { return (pcache1.Slots != 0 && SizePage <= pcache1.SizeSlot ? pcache1.UnderPressure : MallocEx.HeapNearlyFull()); }
    }
}
