using Pid = System.UInt32;
namespace Core
{
    public class PgHdr1
    {
        public PgHdr Page;
        public Pid Key;             // Key value (page number)
        public PgHdr1 Next;         // Next in hash table chain
        public PCache1 Cache;       // Cache that currently owns this page
        public PgHdr1 LruNext;      // Next in LRU list of unpinned pages
        public PgHdr1 LruPrev;      // Previous in LRU list of unpinned pages

        //public PgHdr Page = new PgHdr();  // Pointer to Actual Page Header

        public void Clear()
        {
            Key = 0;
            Next = null;
            Cache = null;
          //  Page.ClearState();
        }
    }
}
