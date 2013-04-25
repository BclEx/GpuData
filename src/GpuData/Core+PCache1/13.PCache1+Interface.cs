using System;
using Pgno = System.UInt32;
using System.Diagnostics;
using System.Text;
using IPCache = Core.Name.PCache1;
namespace Core
{
    public partial class PCache1
    {
        private static bool sqlite3GlobalConfig_bCoreMutex = false;
        private static PCacheGlobal pcache1;

        public static object pArg
        {
            get { return null; }
        }

        public static RC xInit(object NotUsed)
        {
            Debug.Assert(pcache1 == null);
            pcache1 = new PCacheGlobal();
            if (sqlite3GlobalConfig_bCoreMutex)
            {
                pcache1.Group.Mutex = MutexEx.sqlite3_mutex_alloc(MutexEx.MUTEX.STATIC_LRU);
                pcache1.Mutex = MutexEx.sqlite3_mutex_alloc(MutexEx.MUTEX.STATIC_PMEM);
            }
            pcache1.Group.MaxPinned = 10;
            return RC.OK;
        }

        public static void xShutdown(object NotUsed)
        {
            Debug.Assert(pcache1 != null);
            pcache1 = null;
        }

        public static IPCache xCreate(int szPage, bool bPurgeable)
        {
            // The seperateCache variable is true if each PCache has its own private PGroup.  In other words, separateCache is true for mode (1) where no
            // mutexing is required.
            //   *  Always use a unified cache (mode-2) if ENABLE_MEMORY_MANAGEMENT
            //   *  Always use a unified cache in single-threaded applications
            //   *  Otherwise (if multi-threaded and ENABLE_MEMORY_MANAGEMENT is off) use separate caches (mode-1)
#if SQLITE_ENABLE_MEMORY_MANAGEMENT || !SQLITE_THREADSAF
            const int separateCache = 0;
#else
            int separateCache = sqlite3GlobalConfig.bCoreMutex > 0;
#endif
            var pCache = new PCache1();
            {
                PGroup pGroup;       // The group the new page cache will belong to
                if (separateCache != 0)
                {
                    //pGroup = new PGroup();
                    //pGroup.mxPinned = 10;
                }
                else
                    pGroup = pcache1.Group;
                pCache.Group = pGroup;
                pCache.SizePage = szPage;
                pCache.Purgeable = bPurgeable;
                if (bPurgeable)
                {
                    pCache.Min = 10;
                    EnterMutex(pGroup);
                    pGroup.MinPages += (int)pCache.Min;
                    pGroup.MaxPinned = pGroup.MaxPages + 10 - pGroup.MinPages;
                    LeaveMutex(pGroup);
                }
            }
            return (IPCache)pCache;
        }

        public void xCachesize(int nCachesize)
        {
            if (Purgeable)
            {
                var pGroup = this.Group;
                EnterMutex(pGroup);
                pGroup.MaxPages += nCachesize - Max;
                pGroup.MaxPinned = pGroup.MaxPages + 10 - pGroup.MinPages;
                Max = nCachesize;
                N90pct = Max * 9 / 10;
                pcache1EnforceMaxPage(pGroup);
                LeaveMutex(pGroup);
            }
        }

        public int xPagecount()
        {
            EnterMutex(Group);
            var n = (int)Pages;
            LeaveMutex(Group);
            return n;
        }

        public PgHdr xFetch(Pgno key, int createFlag)
        {
            Debug.Assert(Purgeable || createFlag != 1);
            Debug.Assert(Purgeable || Min == 0);
            Debug.Assert(!Purgeable || Min == 10);
            Debug.Assert(Min == 0 || Purgeable);
            PGroup pGroup;
            EnterMutex(pGroup = this.Group);
            // Step 1: Search the hash table for an existing entry.
            PgHdr1 pPage = null;
            if (nHash > 0)
            {
                var h = (int)(key % nHash);
                for (pPage = Hash[h]; pPage != null && pPage.Key != key; pPage = pPage.Next) ;
            }
            // Step 2: Abort if no existing page is found and createFlag is 0
            if (pPage != null || createFlag == 0)
            {
                pcache1PinPage(pPage);
                goto fetch_out;
            }
            // The pGroup local variable will normally be initialized by the pcache1EnterMutex() macro above.  But if SQLITE_MUTEX_OMIT is defined,
            // then pcache1EnterMutex() is a no-op, so we have to initialize the local variable here.  Delaying the initialization of pGroup is an
            // optimization:  The common case is to exit the module before reaching
            // this point.
#if  SQLITE_MUTEX_OMIT
      pGroup = pCache.pGroup;
#endif
            // Step 3: Abort if createFlag is 1 but the cache is nearly full
            var nPinned = Pages - Recyclables;
            Debug.Assert(nPinned >= 0);
            Debug.Assert(pGroup.MaxPinned == pGroup.MaxPages + 10 - pGroup.MinPages);
            Debug.Assert(N90pct == Max * 9 / 10);
            if (createFlag == 1 && (nPinned >= pGroup.MaxPinned || nPinned >= (int)N90pct || pcache1UnderMemoryPressure()))
                goto fetch_out;
            if (Pages >= nHash && pcache1ResizeHash() != 0)
                goto fetch_out;
            // Step 4. Try to recycle a page.
            if (Purgeable && pGroup.LruTail != null && ((Pages + 1 >= Max) || pGroup.CurrentPages >= pGroup.MaxPages || pcache1UnderMemoryPressure()))
            {
                pPage = pGroup.LruTail;
                pcache1RemoveFromHash(pPage);
                pcache1PinPage(pPage);
                PCache1 pOtherCache;
                if ((pOtherCache = pPage.Cache).SizePage != SizePage)
                {
                    pcache1FreePage(ref pPage);
                    pPage = null;
                }
                else
                    pGroup.CurrentPages -= (pOtherCache.Purgeable ? 1 : 0) - (Purgeable ? 1 : 0);
            }
            // Step 5. If a usable page buffer has still not been found, attempt to allocate a new one. 
            if (null == pPage)
            {
                if (createFlag == 1)
                    MallocEx.BeginBenignMalloc();
                LeaveMutex(pGroup);
                pPage = pcache1AllocPage();
                EnterMutex(pGroup);
                if (createFlag == 1)
                    MallocEx.EndBenignMalloc();
            }
            if (pPage != null)
            {
                var h = (int)(key % nHash);
                Pages++;
                pPage.Key = key;
                pPage.Next = Hash[h];
                pPage.Cache = this;
                pPage.LruPrev = null;
                pPage.LruNext = null;
                PGHDR1_TO_PAGE(pPage).ClearState();
                pPage.Page.PgHdr1 = pPage;
                Hash[h] = pPage;
            }
        fetch_out:
            if (pPage != null && key > MaxKey)
                MaxKey = key;
            LeaveMutex(pGroup);
            return (pPage != null ? PGHDR1_TO_PAGE(pPage) : null);
        }

        public void xUnpin(PgHdr p2, bool discard)
        {
            var pPage = PAGE_TO_PGHDR1(this, p2);
            Debug.Assert(pPage.Cache == this);
            var pGroup = this.Group;
            EnterMutex(pGroup);
            // It is an error to call this function if the page is already  part of the PGroup LRU list.
            Debug.Assert(pPage.LruPrev == null && pPage.LruNext == null);
            Debug.Assert(pGroup.LruHead != pPage && pGroup.LruTail != pPage);
            if (discard || pGroup.CurrentPages > pGroup.MaxPages)
            {
                pcache1RemoveFromHash(pPage);
                pcache1FreePage(ref pPage);
            }
            else
            {
                // Add the page to the PGroup LRU list. 
                if (pGroup.LruHead != null)
                {
                    pGroup.LruHead.LruPrev = pPage;
                    pPage.LruNext = pGroup.LruHead;
                    pGroup.LruHead = pPage;
                }
                else
                {
                    pGroup.LruTail = pPage;
                    pGroup.LruHead = pPage;
                }
                Recyclables++;
            }
            LeaveMutex(pGroup);
        }

        public void xRekey(PgHdr p2, Pgno oldKey, Pgno newKey)
        {
            var pPage = PAGE_TO_PGHDR1(this, p2);
            Debug.Assert(pPage.Key == oldKey);
            Debug.Assert(pPage.Cache == this);
            EnterMutex(Group);
            var h = (int)(oldKey % nHash);
            var pp = Hash[h];
            while (pp != pPage)
                pp = pp.Next;
            if (pp == Hash[h])
                Hash[h] = pp.Next;
            else
                pp.Next = pPage.Next;
            h = (int)(newKey % nHash);
            pPage.Key = newKey;
            pPage.Next = Hash[h];
            Hash[h] = pPage;
            if (newKey > MaxKey)
                MaxKey = newKey;
            LeaveMutex(Group);
        }

        public void xTruncate(Pgno iLimit)
        {
            EnterMutex(Group);
            if (iLimit <= MaxKey)
            {
                pcache1TruncateUnsafe(iLimit);
                MaxKey = iLimit - 1;
            }
            LeaveMutex(Group);
        }

        public static void xDestroy(ref IPCache pCache)
        {
            PGroup pGroup = pCache.Group;
            Debug.Assert(pCache.Purgeable || (pCache.Max == 0 && pCache.Min == 0));
            EnterMutex(pGroup);
            pCache.pcache1TruncateUnsafe(0);
            pGroup.MaxPages -= pCache.Max;
            pGroup.MinPages -= pCache.Min;
            pGroup.MaxPinned = pGroup.MaxPages + 10 - pGroup.MinPages;
            pcache1EnforceMaxPage(pGroup);
            LeaveMutex(pGroup);
            pCache = null;
        }

#if SQLITE_ENABLE_MEMORY_MANAGEMENT
int sqlite3PcacheReleaseMemory(int nReq){
  int nFree = 0;
  Debug.Assert( sqlite3_mutex_notheld(pcache1.grp.mutex) );
  Debug.Assert( sqlite3_mutex_notheld(pcache1.mutex) );
  if( pcache1.pStart==0 ){
    PgHdr1 p;
    pcache1EnterMutex(&pcache1.grp);
    while( (nReq<0 || nFree<nReq) && ((p=pcache1.grp.pLruTail)!=0) ){
      nFree += pcache1MemSize(PGHDR1_TO_PAGE(p));
      PCache1pinPage(p);
      pcache1RemoveFromHash(p);
      pcache1FreePage(p);
    }
    pcache1LeaveMutex(&pcache1.grp);
  }
  return nFree;
}
#endif
    }
}
