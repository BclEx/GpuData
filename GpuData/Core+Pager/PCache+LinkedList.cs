using System.Diagnostics;
namespace Core
{
    public partial class PCache
    {

#if EXPENSIVE_ASSERT
        private int pcacheCheckSynced()
        {
            PgHdr p;
            for (p = DirtyTail; p != Synced; p = p.DirtyPrev)
                Debug.Assert(p.Refs != 0 || (p.Flags & PgHdr.PGHDR.NEED_SYNC) != 0);
            return (p == null || p.Refs != 0 || (p.Flags & PgHdr.PGHDR.NEED_SYNC) == 0) ? 1 : 0;
        }
#endif

        private static void pcacheRemoveFromDirtyList(PgHdr page)
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
            Debug.Assert(pcacheCheckSynced(p));
#endif
        }

        private static void pcacheAddToDirtyList(PgHdr page)
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
            if (null == p.DirtyTail)
                p.DirtyTail = page;
            if (null == p.Synced && 0 == (page.Flags & PgHdr.PGHDR.NEED_SYNC))
                p.Synced = page;
#if EXPENSIVE_ASSERT
            Debug.Assert(pcacheCheckSynced(p));
#endif
        }

        private static void pcacheUnpin(PgHdr page)
        {
            var p = page.Cache;
            if (p.Purgeable)
            {
                if (page.ID == 1)
                    p.Page1 = null;
                p.pCache.xUnpin(page, false);
            }
        }
    }
}
