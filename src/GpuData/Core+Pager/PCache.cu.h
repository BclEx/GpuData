// pcache.h
namespace Core
{
	typedef struct PgHdr PgHdr;
	typedef struct PCache PCache;

	class IPCache
	{
	public:
		virtual RC Init();
		virtual void Shutdown();
		virtual IPCache *Create(int sizePage, int sizeExtra, bool purgeable);
		virtual void Cachesize(uint max);
		virtual void Shrink();
		virtual int Pagecount();
		virtual IPage *Fetch(Pid key, int createFlag);
		virtual void Unpin(IPage *pg, bool reuseUnlikely);
		virtual void Rekey(IPage *pg, Pid old, Pid new_);
		virtual void Truncate(Pid limit);
		virtual void Destroy(IPCache *p);
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
		IPage *Page;				// Pcache object page handle
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


	//
	//#ifdef TEST
	//	void Stats(int*,int*,int*,int*);
	//#endif
}
