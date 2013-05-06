using Pid = System.UInt32;
using IPCache = Core.PCache1;
using System;
using System.Diagnostics;
namespace Core
{
    public partial class PCache
    {
        //static PCache()
        //{
        //    Name.PCache1.xInit(0);
        //}

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

        public void ClearState()
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
        private bool CheckSynced()
        {
            PgHdr p;
            for (p = DirtyTail; p != Synced; p = p.DirtyPrev)
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
            Debug.Assert(pcacheCheckSynced(p));
#endif
        }

        private static void Unpin(PgHdr page)
        {
            var cache = page.Cache;
            if (cache.Purgeable)
            {
                if (page.ID == 1)
                    cache.Page1 = null;
                cache.Cache.Unpin(page, false);
            }
        }

        #endregion

        #region Interface

        private const int N_SORT_BUCKET = 32;

        private static RC Initialize() { return IPCache.Init(); }
        private static void Shutdown() { IPCache.Shutdown(); }
        internal static int Size() { return 4; }

        internal static void Open(int sizePage, int sizeExtra, bool purgeable, Func<object, PgHdr, RC> stress, object stressArg, PCache p)
        {
            p.ClearState();
            p.SizePage = sizePage;
            p.SizeExtra = sizeExtra;
            p.Purgeable = purgeable;
            p.Stress = stress;
            p.StressArg = stressArg;
            p.SizeCache = 100;
        }

        internal void SetPageSize(int sizePage)
        {
            Debug.Assert(Refs == 0 && Dirty == null);
            if (Cache != null)
            {
                PCache1.Destroy(ref Cache);
                Cache = null;
                Page1 = null;
            }
            SizePage = sizePage;
        }

        private int NumberOfCachePages()
        {
            if (SizeCache >= 0)
                return SizeCache;
            return (int)((-1024 * (long)SizeCache) / (SizePage + SizeExtra));
        }

        internal RC Fetch(Pid id, int createFlag, out PgHdr pageOut)
        {
            Debug.Assert(createFlag == 1 || createFlag == 0);
            Debug.Assert(id > 0);
            // If the pluggable cache (sqlite3_pcache*) has not been allocated, allocate it now.
            if (Cache == null && createFlag != 0)
            {
                var p = PCache1.Create(SizePage, SizeExtra + 0, Purgeable);
                p.Cachesize(NumberOfCachePages());
                Cache = p;
            }
            PgHdr page = null;
            var create = createFlag * (1 + ((!Purgeable || Dirty == null) ? 1 : 0));
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
                    sqlite3_log(SQLITE_FULL, "spill page %d making room for %d - cache used: %d/%d", pPg.pgno, pgno, sqlite3GlobalConfig.pcache.xPagecount(pCache.pCache), pCache.nMax);
#endif
                    var rc = Stress(StressArg, pg);
                    if (rc != RC.OK && rc != RC.BUSY)
                        return rc;
                }
                page = Cache.Fetch(id, 2);
            }
            if (page != null)
            {
                if (page._Data == null)
                {
                    page._Data = SysEx.Alloc(Cache.SizePage);
                    page.Extra = this;
                    page.Cache = this;
                    page.ID = id;
                }
                Debug.Assert(page.Cache == this);
                Debug.Assert(page.ID == id);
                if (page.Refs == 0)
                    Refs++;
                page.Refs++;
                if (id == 1)
                    Page1 = page;
            }
            pageOut = page;
            return (page == null && create != 0 ? RC.NOMEM : RC.OK);
        }

        internal static void ReleasePage(PgHdr p)
        {
            Debug.Assert(p.Refs > 0);
            p.Refs--;
            if (p.Refs == 0)
            {
                var pCache = p.Cache;
                pCache.nRef--;
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

        internal static void AddPageRef(PgHdr p) { Debug.Assert(p.Refs > 0); p.Refs++; }

        internal static void DropPage(PgHdr p)
        {
            PCache pCache;
            Debug.Assert(p.Refs == 1);
            if ((p.Flags & PgHdr.PGHDR.DIRTY) != 0)
                RemoveFromDirtyList(p);
            pCache = p.Cache;
            pCache.nRef--;
            if (p.ID == 1)
                pCache.Page1 = null;
            pCache.pCache.xUnpin(p, true);
        }

        internal static void MakePageDirty(PgHdr p)
        {
            p.Flags &= ~PgHdr.PGHDR.DONT_WRITE;
            Debug.Assert(p.Refs > 0);
            if (0 == (p.Flags & PgHdr.PGHDR.DIRTY))
            {
                p.Flags |= PgHdr.PGHDR.DIRTY;
                AddToDirtyList(p);
            }
        }

        internal static void MakePageClean(PgHdr p)
        {
            if ((p.Flags & PgHdr.PGHDR.DIRTY) != 0)
            {
                RemoveFromDirtyList(p);
                p.Flags &= ~(PgHdr.PGHDR.DIRTY | PgHdr.PGHDR.NEED_SYNC);
                if (p.Refs == 0)
                    Unpin(p);
            }
        }

        internal void CleanAllPages()
        {
            PgHdr p;
            while ((p = Dirty) != null)
                MakePageClean(p);
        }

        internal void ClearSyncFlags()
        {
            for (var p = Dirty; p != null; p = p.DirtyNext)
                p.Flags &= ~PgHdr.PGHDR.NEED_SYNC;
            Synced = DirtyTail;
        }

        internal static void MovePage(PgHdr p, Pgno newPgno)
        {
            PCache pCache = p.Cache;
            Debug.Assert(p.Refs > 0);
            Debug.Assert(newPgno > 0);
            pCache.pCache.xRekey(p, p.ID, newPgno);
            p.ID = newPgno;
            if ((p.Flags & PgHdr.PGHDR.DIRTY) != 0 && (p.Flags & PgHdr.PGHDR.NEED_SYNC) != 0)
            {
                RemoveFromDirtyList(p);
                AddToDirtyList(p);
            }
        }

        internal void TruncatePage(Pgno pgno)
        {
            if (pCache != null)
            {
                PgHdr p;
                PgHdr pNext;
                for (p = Dirty; p != null; p = pNext)
                {
                    pNext = p.DirtyNext;
                    // This routine never gets call with a positive pgno except right after sqlite3PcacheCleanAll().  So if there are dirty pages,
                    // it must be that pgno==0.
                    Debug.Assert(p.ID > 0);
                    if (SysEx.ALWAYS(p.ID > pgno))
                    {
                        Debug.Assert((p.Flags & PgHdr.PGHDR.DIRTY) != 0);
                        MakePageClean(p);
                    }
                }
                if (pgno == 0 && Page1 != null)
                {
                    Page1._Data = MallocEx.Malloc(szPage);
                    pgno = 1;
                }
                pCache.xTruncate(pgno + 1);
            }
        }

        internal void Close()
        {
            if (pCache != null)
                IPCache.xDestroy(ref pCache);
        }

        internal void Clear() { TruncatePage(0); }

        private static PgHdr pcacheMergeDirtyList(PgHdr pA, PgHdr pB)
        {
            var result = new PgHdr();
            var pTail = result;
            while (pA != null && pB != null)
            {
                if (pA.ID < pB.ID)
                {
                    pTail.Dirtys = pA;
                    pTail = pA;
                    pA = pA.Dirtys;
                }
                else
                {
                    pTail.Dirtys = pB;
                    pTail = pB;
                    pB = pB.Dirtys;
                }
            }
            if (pA != null)
                pTail.Dirtys = pA;
            else if (pB != null)
                pTail.Dirtys = pB;
            else
                pTail.Dirtys = null;
            return result.Dirtys;
        }

        private static PgHdr pcacheSortDirtyList(PgHdr pIn)
        {
            var a = new PgHdr[N_SORT_BUCKET];
            PgHdr p;
            while (pIn != null)
            {
                p = pIn;
                pIn = p.Dirtys;
                p.Dirtys = null;
                int i;
                for (i = 0; SysEx.ALWAYS(i < N_SORT_BUCKET - 1); i++)
                {
                    if (a[i] == null)
                    {
                        a[i] = p;
                        break;
                    }
                    else
                    {
                        p = pcacheMergeDirtyList(a[i], p);
                        a[i] = null;
                    }
                }
                if (SysEx.NEVER(i == N_SORT_BUCKET - 1))
                    // To get here, there need to be 2^(N_SORT_BUCKET) elements in the input list.  But that is impossible.
                    a[i] = pcacheMergeDirtyList(a[i], p);
            }
            p = a[0];
            for (var i = 1; i < N_SORT_BUCKET; i++)
                p = pcacheMergeDirtyList(p, a[i]);
            return p;
        }

        internal PgHdr sqlite3PcacheDirtyList()
        {
            for (var p = Dirty; p != null; p = p.DirtyNext)
                p.Dirtys = p.DirtyNext;
            return pcacheSortDirtyList(Dirty);
        }

        internal int RefCount() { return nRef; }
        internal static int sqlite3PcachePageRefcount(PgHdr p) { return p.Refs; }
        internal int sqlite3PcachePagecount() { return (pCache != null ? pCache.xPagecount() : 0); }
        internal void sqlite3PcacheSetCachesize(int mxPage)
        {
            nMax = mxPage;
            if (pCache != null)
                pCache.xCachesize(mxPage);
        }

#if DEBUG || CHECK_PAGES
        // For all dirty pages currently in the cache, invoke the specified callback. This is only used if the SQLITE_CHECK_PAGES macro is defined.
        internal void IterateDirty(Action<PgHdr> xIter)
        {
            for (var pDirty = this.Dirty; pDirty != null; pDirty = pDirty.DirtyNext)
                xIter(pDirty);
        }
#endif

        #endregion
    }
}
