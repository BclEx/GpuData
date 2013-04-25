using IPgHdr = Core.Name.PgHdr1;
using Pid = System.UInt32;
namespace Core
{
    public class PgHdr
    {
        public byte[] _Data;

        public enum PGHDR : ushort
        {
            DIRTY = 0x002, // Page has changed
            NEED_SYNC = 0x004, // Fsync the rollback journal before writing this page to the database
            NEED_READ = 0x008, // Content is unread
            REUSE_UNLIKELY = 0x010, // A hint that reuse is unlikely
            DONT_WRITE = 0x020, // Do not write content to disk
        }

        public object Extra;        // Extra content
        public PgHdr Dirtys;          // Transient list of dirty pages
        public Pid ID;             // The page number for this page
        public Pager Pager;          // The pager to which this page belongs
#if CHECK_PAGES
        public uint PageHash;          // Hash of page content
#endif
        public PGHDR Flags;             // PGHDR flags defined below

        public bool CacheAllocated; // True, if allocated from cache
        public IPgHdr PgHdr1;        // Cache page header this this page

        // Elements above are public.  All that follows is private to pcache.c and should not be accessed by other modules.
        internal int Refs;            // Number of users of this page
        internal PCache Cache;        // Cache that owns this page
        internal PgHdr DirtyNext;      // Next element in list of dirty pages
        internal PgHdr DirtyPrev;      // Previous element in list of dirty pages

        public static implicit operator bool(PgHdr b) { return (b != null); }

        public void ClearState()
        {
            _Data = null;
            Extra = null;
            Dirtys = null;
            ID = 0;
            Pager = null;
#if DEBUG
            PageHash = 0;
#endif
            Flags = 0;
            Refs = 0;
            CacheAllocated = false;
            Cache = null;
            DirtyNext = null;
            DirtyPrev = null;
            PgHdr1 = null;
        }
    }
}
