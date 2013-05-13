﻿using Pid = System.UInt32;
using IPage = Core.PgHdr;
using System;
using System.Diagnostics;

namespace Core
{
    public partial class PCache
    {
        public PgHdr Dirty, DirtyTail;  // List of dirty pages in LRU order
        public PgHdr Synced;        // Last synced page in dirty page list
        public int Refs;            // Number of referenced pages
        public int SizeCache;       // Configured cache size
        public int SizePage;        // Size of every page in this cache
        public int SizeExtra;       // Size of extra space for each page
        public bool Purgeable;      // True if pages are on backing store
        public Func<object, PgHdr, RC> Stress;   // Call to try make a page clean
        public object StressArg;    // Argument to xStress
        public IPCache Cache;       // Pluggable cache module
        public PgHdr Page1;         // Reference to page 1

        public void memset()
        {
            Dirty = DirtyTail = null;
            Synced = null;
            Refs = 0;
        }
    }

    partial class PCache
    {
        #region Linked List

#if EXPENSIVE_ASSERT
        private static bool CheckSynced(PCache cache)
        {
            PgHdr p;
            for (p = cache.DirtyTail; p != cache.Synced; p = p.DirtyPrev)
                Debug.Assert(p.Refs != 0 || (p.Flags & PgHdr.PGHDR.NEED_SYNC) != 0);
            return (p == null || p.Refs != 0 || (p.Flags & PgHdr.PGHDR.NEED_SYNC) == 0);
        }
#endif

        private static void RemoveFromDirtyList(PgHdr page)
        {
            var p = page.Cache;
            Debug.Assert(page.DirtyNext != null || page == p.DirtyTail);
            Debug.Assert(page.DirtyPrev != null || page == p.Dirty);
            // Update the PCache1.pSynced variable if necessary.
            if (p.Synced == page)
            {
                var synced = page.DirtyPrev;
                while (synced != null && (synced.Flags & PgHdr.PGHDR.NEED_SYNC) != 0)
                    synced = synced.DirtyPrev;
                p.Synced = synced;
            }
            if (page.DirtyNext != null)
                page.DirtyNext.DirtyPrev = page.DirtyPrev;
            else
            {
                Debug.Assert(page == p.DirtyTail);
                p.DirtyTail = page.DirtyPrev;
            }
            if (page.DirtyPrev != null)
                page.DirtyPrev.DirtyNext = page.DirtyNext;
            else
            {
                Debug.Assert(page == p.Dirty);
                p.Dirty = page.DirtyNext;
            }
            page.DirtyNext = null;
            page.DirtyPrev = null;
#if EXPENSIVE_ASSERT
            Debug.Assert(CheckSynced(p));
#endif
        }

        private static void AddToDirtyList(PgHdr page)
        {
            var p = page.Cache;
            Debug.Assert(page.DirtyNext == null && page.DirtyPrev == null && p.Dirty != page);
            page.DirtyNext = p.Dirty;
            if (page.DirtyNext != null)
            {
                Debug.Assert(page.DirtyNext.DirtyPrev == null);
                page.DirtyNext.DirtyPrev = page;
            }
            p.Dirty = page;
            if (p.DirtyTail == null)
                p.DirtyTail = page;
            if (p.Synced == null && (page.Flags & PgHdr.PGHDR.NEED_SYNC) == 0)
                p.Synced = page;
#if EXPENSIVE_ASSERT
            Debug.Assert(CheckSynced(p));
#endif
        }

        private static void Unpin(PgHdr p)
        {
            var cache = p.Cache;
            if (cache.Purgeable)
            {
                if (p.ID == 1)
                    cache.Page1 = null;
                cache.Cache.Unpin(p.Page, false);
            }
        }

        #endregion

        #region Interface

        static IPCache _pcache;

        public static RC Initialize()
        {
            if (_pcache == null)
                _pcache = new PCache1();
            return _pcache.Init();
        }
        public static void Shutdown()
        {
            _pcache.Shutdown();
        }
        //internal static int Size() { return 4; }

        public void Open(int sizePage, int sizeExtra, bool purgeable, Func<object, PgHdr, RC> stress, object stressArg, PCache p)
        {
            p.memset();
            p.SizePage = sizePage;
            p.SizeExtra = sizeExtra;
            p.Purgeable = purgeable;
            p.Stress = stress;
            p.StressArg = stressArg;
            p.SizeCache = 100;
        }

        public void SetPageSize(int sizePage)
        {
            Debug.Assert(Refs == 0 && Dirty == null);
            if (Cache != null)
            {
                _pcache.Destroy(ref Cache);
                Cache = null;
                Page1 = null;
            }
            SizePage = sizePage;
        }

        private static uint NumberOfCachePages(PCache p)
        {
            if (p.SizeCache >= 0)
                return (uint)p.SizeCache;
            return (uint)((-1024 * (long)p.SizeCache) / (p.SizePage + p.SizeExtra));
        }

        public RC Fetch(Pid id, bool createFlag, out PgHdr pageOut)
        {
            Debug.Assert(id > 0);
            // If the pluggable cache (sqlite3_pcache*) has not been allocated, allocate it now.
            if (Cache == null && createFlag)
            {
                var p = _pcache.Create(SizePage, SizeExtra + 0, Purgeable);
                p.Cachesize(NumberOfCachePages(this));
                Cache = p;
            }
            PgHdr page = null;
            var create = (createFlag ? 1 : 0) * (1 + ((!Purgeable || Dirty == null) ? 1 : 0));
            if (Cache != null)
                page = Cache.Fetch(id, create);
            if (page == null && create == 1)
            {
                // Find a dirty page to write-out and recycle. First try to find a page that does not require a journal-sync (one with PGHDR_NEED_SYNC
                // cleared), but if that is not possible settle for any other unreferenced dirty page.
#if EXPENSIVE_ASSERT
                CheckSynced(this);
#endif
                PgHdr pg;
                for (pg = Synced; pg != null && (pg.Refs != 0 || (pg.Flags & PgHdr.PGHDR.NEED_SYNC) != 0); pg = pg.DirtyPrev) ;
                Synced = pg;
                if (pg == null)
                    for (pg = DirtyTail; pg != null && pg.Refs != 0; pg = pg.DirtyPrev) ;
                if (pg != null)
                {
#if LOG_CACHE_SPILL
                    SysEx.Log(RC.FULL, "spill page %d making room for %d - cache used: %d/%d", pg.ID, id, _pcache->Pagecount(Cache), NumberOfCachePages(this));
#endif
                    var rc = Stress(StressArg, pg);
                    if (rc != RC.OK && rc != RC.BUSY)
                    {
                        pageOut = null;
                        return rc;
                    }
                }
                page = Cache.Fetch(id, 2);
            }
            PgHdr pgHdr = null;
            if (page != null)
            {
                //pgHdr = page.Extra;
                if (page.Data == null)
                {
                    //page.Page = page;
                    page.Data = SysEx.Alloc(SizePage);
                    //page.Extra = this;
                    page.Cache = this;
                    page.ID = id;
                }
                Debug.Assert(page.Cache == Cache);
                Debug.Assert(page.ID == id);
                //Debug.Assert(page.Data == page.Buffer);
                //Debug.Assert(page.Extra == this);
                if (page.Refs == 0)
                    Refs++;
                page.Refs++;
                if (id == 1)
                    Page1 = pgHdr;
            }
            pageOut = pgHdr;
            return (pgHdr == null && create != 0 ? RC.NOMEM : RC.OK);
        }

        internal static void Release(PgHdr p)
        {
            Debug.Assert(p.Refs > 0);
            p.Refs--;
            if (p.Refs == 0)
            {
                var cache = p.Cache;
                cache.Refs--;
                if ((p.Flags & PgHdr.PGHDR.DIRTY) == 0)
                    Unpin(p);
                else
                {
                    // Move the page to the head of the dirty list.
                    RemoveFromDirtyList(p);
                    AddToDirtyList(p);
                }
            }
        }

        internal static void Ref(PgHdr p)
        {
            Debug.Assert(p.Refs > 0);
            p.Refs++;
        }

        internal static void Drop(PgHdr p)
        {
            Debug.Assert(p.Refs == 1);
            if ((p.Flags & PgHdr.PGHDR.DIRTY) != 0)
                RemoveFromDirtyList(p);
            var cache = p.Cache;
            cache.Refs--;
            if (p.ID == 1)
                cache.Page1 = null;
            cache.Cache.Unpin(p.Page, true);
        }

        internal static void MakeDirty(PgHdr p)
        {
            p.Flags &= ~PgHdr.PGHDR.DONT_WRITE;
            Debug.Assert(p.Refs > 0);
            if ((p.Flags & PgHdr.PGHDR.DIRTY) == 0)
            {
                p.Flags |= PgHdr.PGHDR.DIRTY;
                AddToDirtyList(p);
            }
        }

        internal static void MakeClean(PgHdr p)
        {
            if ((p.Flags & PgHdr.PGHDR.DIRTY) != 0)
            {
                RemoveFromDirtyList(p);
                p.Flags &= ~(PgHdr.PGHDR.DIRTY | PgHdr.PGHDR.NEED_SYNC);
                if (p.Refs == 0)
                    Unpin(p);
            }
        }

        internal static void CleanAll(PCache cache)
        {
            PgHdr p;
            while ((p = cache.Dirty) != null)
                MakeClean(p);
        }

        internal static void ClearSyncFlags(PCache cache)
        {
            for (var p = cache.Dirty; p != null; p = p.DirtyNext)
                p.Flags &= ~PgHdr.PGHDR.NEED_SYNC;
            cache.Synced = cache.DirtyTail;
        }

        internal static void MovePage(PgHdr p, Pid newID)
        {
            PCache cache = p.Cache;
            Debug.Assert(p.Refs > 0);
            Debug.Assert(newID > 0);
            cache.Cache.Rekey(p.Page, p.ID, newID);
            p.ID = newID;
            if ((p.Flags & PgHdr.PGHDR.DIRTY) != 0 && (p.Flags & PgHdr.PGHDR.NEED_SYNC) != 0)
            {
                RemoveFromDirtyList(p);
                AddToDirtyList(p);
            }
        }

        internal static void TruncatePage(PCache cache, Pid id)
        {
            if (cache.Cache != null)
            {
                PgHdr p;
                PgHdr next;
                for (p = cache.Dirty; p != null; p = next)
                {
                    next = p.DirtyNext;
                    // This routine never gets call with a positive pgno except right after sqlite3PcacheCleanAll().  So if there are dirty pages, it must be that pgno==0.
                    Debug.Assert(p.ID > 0);
                    if (SysEx.ALWAYS(p.ID > id))
                    {
                        Debug.Assert((p.Flags & PgHdr.PGHDR.DIRTY) != 0);
                        MakeClean(p);
                    }
                }
                if (id == 0 && cache.Page1 != null)
                {
                    cache.Page1.memsetData(cache.SizePage);
                    id = 1;
                }
                cache.Cache.Truncate(id + 1);
            }
        }

        internal static void Close(PCache cache)
        {
            if (cache.Cache != null)
                _pcache.Destroy(ref cache.Cache);
        }

        internal static void Clear(PCache cache)
        {
            TruncatePage(cache, 0);
        }

        private static PgHdr MergeDirtyList(PgHdr a, PgHdr b)
        {
            var result = new PgHdr();
            var tail = result;
            while (a != null && b != null)
            {
                if (a.ID < b.ID)
                {
                    tail.Dirty = a;
                    tail = a;
                    a = a.Dirty;
                }
                else
                {
                    tail.Dirty = b;
                    tail = b;
                    b = b.Dirty;
                }
            }
            if (a != null)
                tail.Dirty = a;
            else if (b != null)
                tail.Dirty = b;
            else
                tail.Dirty = null;
            return result.Dirty;
        }

        private const int N_SORT_BUCKET = 32;

        private static PgHdr SortDirtyList(PgHdr @in)
        {
            var a = new PgHdr[N_SORT_BUCKET];
            PgHdr p;
            int i;
            while (@in != null)
            {
                p = @in;
                @in = p.Dirty;
                p.Dirty = null;
                for (i = 0; SysEx.ALWAYS(i < N_SORT_BUCKET - 1); i++)
                {
                    if (a[i] == null)
                    {
                        a[i] = p;
                        break;
                    }
                    else
                    {
                        p = MergeDirtyList(a[i], p);
                        a[i] = null;
                    }
                }
                if (SysEx.NEVER(i == N_SORT_BUCKET - 1))
                    // To get here, there need to be 2^(N_SORT_BUCKET) elements in the input list.  But that is impossible.
                    a[i] = MergeDirtyList(a[i], p);
            }
            p = a[0];
            for (i = 1; i < N_SORT_BUCKET; i++)
                p = MergeDirtyList(p, a[i]);
            return p;
        }

        internal static PgHdr DirtyList(PCache cache)
        {
            for (var p = cache.Dirty; p != null; p = p.DirtyNext)
                p.Dirty = p.DirtyNext;
            return SortDirtyList(cache.Dirty);
        }

        internal static int RefCount(PCache cache)
        {
            return cache.Refs;
        }

        internal static int PageRefcount(PgHdr p)
        {
            return p.Refs;
        }

        internal static int Pagecount(PCache cache)
        {
            return (cache.Cache != null ? cache.Cache.Pagecount() : 0);
        }

        internal static void SetCachesize(PCache cache, int maxPage)
        {
            cache.SizeCache = maxPage;
            if (cache.Cache != null)
                cache.Cache.Cachesize(NumberOfCachePages(cache));
        }

        internal static void Shrink(PCache cache)
        {
            if (cache.Cache != null)
                cache.Cache.Shrink();
        }

#if CHECK_PAGES || DEBUG
        // For all dirty pages currently in the cache, invoke the specified callback. This is only used if the SQLITE_CHECK_PAGES macro is defined.
        internal static void IterateDirty(PCache cache, Action<PgHdr> iter)
        {
            for (var dirty = cache.Dirty; dirty != null; dirty = dirty.DirtyNext)
                iter(dirty);
        }
#endif

        #endregion
    }
}
