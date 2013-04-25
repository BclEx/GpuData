using System;
using Pgno = System.UInt32;
using System.Diagnostics;
using System.Text;
namespace Core
{
    public partial class PCache1
    {
        private RC pcache1ResizeHash()
        {
            Debug.Assert(MutexEx.Held(Group.Mutex));
            var nNew = nHash * 2;
            if (nNew < 256)
                nNew = 256;
            LeaveMutex(Group);
            if (nHash != 0)
                MallocEx.BeginBenignMalloc();
            var apNew = new PgHdr1[nNew];
            if (nHash != 0)
                MallocEx.EndBenignMalloc();
            EnterMutex(Group);
            if (apNew != null)
            {
                for (var i = 0; i < nHash; i++)
                {
                    var pNext = Hash[i];
                    PgHdr1 pPage;
                    while ((pPage = pNext) != null)
                    {
                        var h = (Pgno)(pPage.Key % nNew);
                        pNext = pPage.Next;
                        pPage.Next = apNew[h];
                        apNew[h] = pPage;
                    }
                }
                Hash = apNew;
                nHash = nNew;
            }
            return (Hash != null ? RC.OK : RC.NOMEM);
        }

        private static void pcache1PinPage(PgHdr1 pPage)
        {
            if (pPage == null)
                return;
            var pCache = pPage.Cache;
            var pGroup = pCache.Group;
            Debug.Assert(MutexEx.Held(pGroup.Mutex));
            if (pPage.LruNext != null || pPage == pGroup.LruTail)
            {
                if (pPage.LruPrev != null)
                    pPage.LruPrev.LruNext = pPage.LruNext;
                if (pPage.LruNext != null)
                    pPage.LruNext.LruPrev = pPage.LruPrev;
                if (pGroup.LruHead == pPage)
                    pGroup.LruHead = pPage.LruNext;
                if (pGroup.LruTail == pPage)
                    pGroup.LruTail = pPage.LruPrev;
                pPage.LruNext = null;
                pPage.LruPrev = null;
                pPage.Cache.Recyclables--;
            }
        }

        private static void pcache1RemoveFromHash(PgHdr1 pPage)
        {
            var pCache = pPage.Cache;
            Debug.Assert(MutexEx.Held(pCache.Group.Mutex));
            var h = (int)(pPage.Key % pCache.nHash);
            PgHdr1 pPrev = null;
            PgHdr1 pp;
            for (pp = pCache.Hash[h]; pp != pPage; pPrev = pp, pp = pp.Next) ;
            if (pPrev == null)
                pCache.Hash[h] = pp.Next;
            else
                pPrev.Next = pp.Next;
            pCache.Pages--;
        }

        private static void pcache1EnforceMaxPage(PGroup pGroup)
        {
            Debug.Assert(MutexEx.Held(pGroup.Mutex));
            while (pGroup.CurrentPages > pGroup.MaxPages && pGroup.LruTail != null)
            {
                PgHdr1 p = pGroup.LruTail;
                Debug.Assert(p.Cache.Group == pGroup);
                pcache1PinPage(p);
                pcache1RemoveFromHash(p);
                pcache1FreePage(ref p);
            }
        }

        private void pcache1TruncateUnsafe(uint iLimit)
        {
#if !DEBUG
            uint nPage = 0;
#endif
            Debug.Assert(MutexEx.Held(Group.Mutex));
            for (uint h = 0; h < nHash; h++)
            {
                var pp = Hash[h];
                PgHdr1 pPrev = null;
                PgHdr1 pPage;
                while ((pPage = pp) != null)
                {
                    if (pPage.Key >= iLimit)
                    {
                        Pages--;
                        pp = pPage.Next;
                        pcache1PinPage(pPage);
                        if (Hash[h] == pPage)
                            Hash[h] = pPage.Next;
                        else
                            pPrev.Next = pp;
                        pcache1FreePage(ref pPage);
                    }
                    else
                    {
                        pp = pPage.Next;
#if !DEBUG
                        nPage++;
#endif
                    }
                    pPrev = pPage;
                }
            }
#if !DEBUG
            Debug.Assert(pCache.nPage == nPage);
#endif
        }
    }
}
