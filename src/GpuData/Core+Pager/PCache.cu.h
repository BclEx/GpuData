// pcache.h
namespace Core
{
	typedef struct PgHdr PgHdr;
	typedef struct PCache PCache;

	struct ICachePage
	{
		void *Buffer;	// The content of the page
		void *Extra;	// Extra information associated with the page
	};

	class IPCache
	{
	public:
		virtual RC Init() = 0;
		virtual void Shutdown() = 0;
		virtual IPCache *Create(int sizePage, int sizeExtra, bool purgeable) = 0;
		virtual void Cachesize(uint max) = 0;
		virtual void Shrink() = 0;
		virtual int get_Pages() = 0;
		virtual ICachePage *Fetch(Pid key, int createFlag) = 0;
		virtual void Unpin(ICachePage *pg, bool reuseUnlikely) = 0;
		virtual void Rekey(ICachePage *pg, Pid old, Pid new_) = 0;
		virtual void Truncate(Pid limit) = 0;
		virtual void Destroy(IPCache *p) = 0;
	};

	struct PgHdr
	{
		enum PGHDR : uint16
		{
			DIRTY = 0x002,			// Page has changed
			NEED_SYNC = 0x004,		// Fsync the rollback journal before writing this page to the database
			NEED_READ = 0x008,		// Content is unread
			REUSE_UNLIKELY = 0x010, // A hint that reuse is unlikely
			DONT_WRITE = 0x020		// Do not write content to disk 
		};
		ICachePage *Page;				// Pcache object page handle
		void *Data;					// Page data
		void *Extra;				// Extra content
		PgHdr *Dirty;				// Transient list of dirty pages
		Pager *Pager;				// The pager this page is part of
		Pid ID;						// Page number for this page
#ifdef CHECK_PAGES
		uint32 PageHash;            // Hash of page content
#endif
		uint16 Flags;                // PGHDR flags defined below
		// Elements above are public.  All that follows is private to pcache.c and should not be accessed by other modules.
		int16 Refs;					// Number of users of this page
		PCache *Cache;              // Cache that owns this page
		PgHdr *DirtyNext;           // Next element in list of dirty pages
		PgHdr *DirtyPrev;           // Previous element in list of dirty pages
	};

	struct PCache
	{
		PgHdr *Dirty, *DirtyTail;   // List of dirty pages in LRU order
		PgHdr *Synced;              // Last synced page in dirty page list
		int Refs;                   // Number of referenced pages
		int SizeCache;              // Configured cache size
		int SizePage;               // Size of every page in this cache
		int SizeExtra;              // Size of extra space for each page
		bool Purgeable;             // True if pages are on backing store
		RC (*Stress)(void *, PgHdr *);// Call to try make a page clean
		void *StressArg;            // Argument to xStress
		IPCache *Cache;				// Pluggable cache module
		PgHdr *Page1;				// Reference to page 1
	public:
		//	void sqlite3PCacheBufferSetup(void *, int sz, int n);
		__device__ static int Initialize();
		__device__ static void Shutdown();
		__device__ static int SizeOf();
		__device__ static void Open(int sizePage, int sizeExtra, bool purgeable, RC (*stress)(void *, PgHdr *), void *stressArg, PCache *p);
		__device__ void SetPageSize(int sizePage);
		__device__ int Fetch(Pid id, bool createFlag, PgHdr **pageOut);
		__device__ static void Release(PgHdr *p);
		__device__ static void Ref(PgHdr *p);
		__device__ static void Drop(PgHdr *p);			// Remove page from cache
		__device__ static void MakeDirty(PgHdr *p);	// Make sure page is marked dirty
		__device__ static void MakeClean(PgHdr *p);	// Mark a single page as clean
		__device__ void CleanAll();					// Mark all dirty list pages as clean
		__device__ void ClearSyncFlags();
		__device__ static void Move(PgHdr *p, Pid newID);
		__device__ void Truncate(Pid id);
		__device__ void Close();
		__device__ void Clear();
		__device__ PgHdr *DirtyList();
		__device__ int get_Refs();
		__device__ static int get_PageRefs(PgHdr *p);
		__device__ int get_Pages();
		__device__ void SetCachesize(int maxPage);
		__device__ void Shrink();
#if defined(CHECK_PAGES) || defined(_DEBUG)
		__device__ void IterateDirty(void (*iter)(PgHdr *));
#endif
#ifdef ENABLE_MEMORY_MANAGEMENT
		__device__ static int ReleaseMemory(int required);
#endif
#ifdef TEST
		__device__ uint PCache_testGetCachesize(PCache *cache);
		__device__ void PCache1_testStats(uint *current, uint *max, uint *min, uint *recyclables);
#endif
	};
}
