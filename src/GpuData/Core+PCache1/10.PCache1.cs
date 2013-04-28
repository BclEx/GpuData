using Pid = System.UInt32;
namespace Core
{
    public partial class PCache1
    {
        // Cache configuration parameters. Page size (szPage) and the purgeable flag (bPurgeable) are set when the cache is created. nMax may be 
        // modified at any time by a call to the pcache1CacheSize() method. The PGroup mutex must be held when accessing nMax.
        public PGroup Group;        // PGroup this cache belongs to
        public int SizePage;        // Size of allocated pages in bytes
        public int SizeExtra;       // Size of extra space in bytes
        public bool Purgeable;      // True if cache is purgeable
        public uint Min;             // Minimum number of pages reserved
        public uint Max;             // Configured "cache_size" value
        public uint N90pct;          // nMax*9/10
        public Pid MaxKey;          // Largest key seen since xTruncate()
        
        // Hash table of all pages. The following variables may only be accessed when the accessor is holding the PGroup mutex.
        public uint Recyclables;    // Number of pages in the LRU list
        public uint Pages;          // Total number of pages in apHash
        public PgHdr1[] Hash;       // Hash table for fast lookup by key

        public void Clear()
        {
            Recyclables = 0;
            Pages = 0;
            Hash = null;
            MaxKey = 0;
        }

        //private static PgHdr PGHDR1_TO_PAGE(PgHdr1 p) { return p.Page; }
        //private static PgHdr1 PAGE_TO_PGHDR1(PCache1 c, PgHdr p) { return (PgHdr1)p.PgHdr1; }
        private static void PCache1_EnterMutex(PGroup x) { MutexEx.Enter(x.Mutex); }
        private static void PCache1_LeaveMutex(PGroup x) { MutexEx.Leave(x.Mutex); }
    }
}
