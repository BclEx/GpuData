using System;
using IPCache = Core.Name.PCache1;
namespace Core
{
    public partial class PCache
    {
        static PCache()
        {
            Name.PCache1.xInit(0);
        }

        public PgHdr Dirty;        // List of dirty pages in LRU order
        public PgHdr DirtyTail;
        public PgHdr Synced;       // Last synced page in dirty page list
        public int Refs;            // Number of referenced pages
        public int nMax;            // Configured cache size
        public int sszPage;          // Size of every page in this cache
        public int szExtra;         // Size of extra space for each page
        public bool Purgeable;     // True if pages are on backing store
        public Func<object, PgHdr, RC> xStress;   // Call to try make a page clean
        public object pStress;      // Argument to xStress
        public IPCache pCache;      // Pluggable cache module
        public PgHdr Page1;        // Reference to page 1

        public void ClearState()
        {
            Dirty = null;
            DirtyTail = null;
            Synced = null;
            Refs = 0;
        }
    }
}
