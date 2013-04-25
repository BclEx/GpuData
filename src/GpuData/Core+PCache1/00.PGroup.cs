namespace Core
{
    public class PGroup
    {
        public MutexEx Mutex;           // MUTEX_STATIC_LRU or NULL
        public uint MaxPages;           // Sum of nMax for purgeable caches
        public uint MinPages;           // Sum of nMin for purgeable caches
        public uint MaxPinned;          // nMaxpage + 10 - nMinPage
        public uint CurrentPages;       // Number of purgeable pages allocated
        public PgHdr1 LruHead, LruTail; // LRU list of unpinned pages
    }
}
