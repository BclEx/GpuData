// pcache.c
#include "Core+Pager.cu.h"
using namespace Core;

namespace Core
{
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
		static int Initialize();
		static void Shutdown();
		//	static int SizeOf();
		void Open(int sizePage, int sizeExtra, bool purgeable, RC (*stress)(void *, PgHdr *), void *stressArg, PCache *p);
		void SetPageSize(int sizePage);
		int Fetch(Pid id, bool createFlag, PgHdr **pageOut);
		//	void Release(PgHdr *);
		//	void Drop(PgHdr *);         // Remove page from cache
		//	void MakeDirty(PgHdr *);    // Make sure page is marked dirty
		//	void MakeClean(PgHdr *);	// Mark a single page as clean
		//	void CleanAll(PCache *);	// Mark all dirty list pages as clean
		//	void Move(PgHdr*, Pid);
		//	void Truncate(PCache*, Pid);
		//	PgHdr *DirtyList(PCache *);
		//	void Close(PCache *);
		//	void ClearSyncFlags(PCache *);
		//	void Clear(PCache *);
		//	int RefCount(PCache *);
		//	void Ref(PgHdr *);
		//	int PageRefcount(PgHdr *);
		//	int Pagecount(PCache *);
		//#if defined(CHECK_PAGES) || defined(DEBUG)
		//	void IterateDirty(PCache *cache, void (*iter)(PgHdr *));
		//#endif
		//	void SetCachesize(PCache *, int);
		//#ifdef TEST
		//	int GetCachesize(PCache *);
		//#endif
		//	void Shrink(PCache *);
		//#ifdef ENABLE_MEMORY_MANAGEMENT
		//	int ReleaseMemory(int);
		//#endif
		//	void BufferSetup(void *, int sz, int n);

	};

#pragma region Linked List

#if EXPENSIVE_ASSERT
	static bool CheckSynced(PCache *cache)
	{
		PgHdr *p;
		for (p = cache->DirtyTail; p != cache->Synced; p = p->DirtyPrev)
			_assert(p->Refs || (p->Flags & PgHdr::PGHDR::NEED_SYNC));
		return (p == nullptr || p->Refs || (p->Flags & PgHdr::PGHDR::NEED_SYNC) == 0);
	}
#endif

	static void RemoveFromDirtyList(PgHdr *page)
	{
		PCache *p = page->Cache;
		_assert(page->DirtyNext || page == p->DirtyTail);
		_assert(page->DirtyPrev || page == p->Dirty);
		// Update the PCache1.Synced variable if necessary.
		if (p->Synced == page)
		{
			PgHdr *synced = page->DirtyPrev;
			while (synced && (synced->Flags & PgHdr::PGHDR::NEED_SYNC))
				synced = synced->DirtyPrev;
			p->Synced = synced;
		}
		if (page->DirtyNext)
			page->DirtyNext->DirtyPrev = page->DirtyPrev;
		else
		{
			_assert(page == p->DirtyTail);
			p->DirtyTail = page->DirtyPrev;
		}
		if (page->DirtyPrev)
			page->DirtyPrev->DirtyNext = page->DirtyNext;
		else
		{
			_assert(page == p->Dirty);
			p->Dirty = page->DirtyNext;
		}
		page->DirtyNext = nullptr;
		page->DirtyPrev = nullptr;
#if EXPENSIVE_ASSERT
		_assert(CheckSynced(p));
#endif
	}

	static void AddToDirtyList(PgHdr *page)
	{
		PCache *p = page->Cache;
		_assert(page->DirtyNext == nullptr && page->DirtyPrev == nullptr && p->Dirty != page);
		page->DirtyNext = p->Dirty;
		if (page->DirtyNext)
		{
			_assert(page->DirtyNext->DirtyPrev == nullptr);
			page->DirtyNext->DirtyPrev = page;
		}
		p->Dirty = page;
		if (!p->DirtyTail)
			p->DirtyTail = page;
		if (!p->Synced && (page->Flags & PgHdr::PGHDR::NEED_SYNC) == 0)
			p->Synced = page;
#if EXPENSIVE_ASSERT
		_assert(CheckSynced(p));
#endif
	}

	static void Unpin(PgHdr *p)
	{
		PCache *cache = p->Cache;
		if (cache->Purgeable)
		{
			if (p->ID == 1)
				cache->Page1 = nullptr;
			cache->Cache->Unpin(p->Page, false);
		}
	}

#pragma endregion

#pragma region Interface

	static IPCache *_pcache;

	int PCache::Initialize() 
	{ 
		//if (_pcache == nullptr)
		//	_pcache = new PCache1();
		return _pcache->Init(); 
	}
	void PCache::Shutdown()
	{
		_pcache->Shutdown(); 
	}
	//int PCache::SizeOf() { return sizeof(PCache); }

	void PCache::Open(int sizePage, int sizeExtra, bool purgeable, RC (*stress)(void *, PgHdr *), void *stressArg, PCache *p)
	{
		_memset(p, 0, sizeof(PCache));
		p->SizePage = sizePage;
		p->SizeExtra = sizeExtra;
		p->Purgeable = purgeable;
		p->Stress = stress;
		p->StressArg = stressArg;
		p->SizeCache = 100;
	}

	void PCache::SetPageSize(int sizePage)
	{
		_assert(Refs == 0 && Dirty == nullptr);
		if (Cache)
		{
			_pcache->Destroy(Cache);
			Cache = nullptr;
			Page1 = nullptr;
		}
		SizePage = sizePage;
	}

	static uint NumberOfCachePages(PCache *p)
	{
		if (p->SizeCache >= 0)
			return (uint)p->SizeCache;
		return (uint)((-1024 * (int64)p->SizeCache) / (p->SizePage + p->SizeExtra));
	}

	int PCache::Fetch(Pid id, bool createFlag, PgHdr **pageOut)
	{
		_assert(id > 0);
		// If the pluggable cache (sqlite3_pcache*) has not been allocated, allocate it now.
		if (!Cache && createFlag)
		{
			IPCache *p = _pcache->Create(SizePage, SizeExtra + sizeof(PgHdr), Purgeable);
			if (!p)
				return RC::NOMEM;
			p->Cachesize(NumberOfCachePages(this));
			Cache = p;
		}
		IPage *page = nullptr;
		int create = createFlag * (1 + (!Purgeable || !Dirty));
		if (Cache)
			page = Cache->Fetch(id, create);
		if (!page && create)
		{
			// Find a dirty page to write-out and recycle. First try to find a page that does not require a journal-sync (one with PGHDR_NEED_SYNC
			// cleared), but if that is not possible settle for any other unreferenced dirty page.
#if EXPENSIVE_ASSERT
			CheckSynced(this);
#endif
			PgHdr *pg;
			for (pg = Synced; pg && (pg->Refs || (pg->Flags & PgHdr::PGHDR::NEED_SYNC)); pg = pg->DirtyPrev) ;
			Synced = pg;
			if (!pg)
				for (pg = DirtyTail; pg && pg->Refs; pg = pg->DirtyPrev) ;
			if (pg)
			{
#ifdef LOG_CACHE_SPILL
				SysEx::Log(RC::FULL, "spill page %d making room for %d - cache used: %d/%d", pg->ID, id, _pcache->Pagecount(), NumberOfCachePages(this));
#endif
				RC rc = Stress(StressArg, pg);
				if (rc != RC::OK && rc != RC::BUSY)
					return rc;
			}
			page = Cache->Fetch(id, 2);
		}
		PgHdr *pgHdr = nullptr;
		if (page)
		{
			pgHdr = (PgHdr *)page->Extra;
			if (!pgHdr->Page)
			{
				_memset(pgHdr, 0, sizeof(PgHdr));
				pgHdr->Page = page;
				pgHdr->Data = page->Buffer;
				pgHdr->Extra = (void *)&pgHdr[1];
				_memset(pgHdr->Extra, 0, SizeExtra);
				pgHdr->Cache = this;
				pgHdr->ID = id;
			}
			_assert(pgHdr->Cache == this);
			_assert(pgHdr->ID == id);
			_assert(pgHdr->Data == page->Buffer);
			_assert(pgHdr->Extra == (void *)&pgHdr[1]);
			if (pgHdr->Refs == 0)
				Refs++;
			pgHdr->Refs++;
			if (id == 1)
				Page1 = pgHdr;
		}
		*pageOut = pgHdr;
		return (pgHdr == nullptr && create ? RC::NOMEM : RC::OK);
	}

	void Release(PgHdr *p)
	{
		_assert(p->Refs > 0);
		p->Refs--;
		if (p->Refs == 0)
		{
			PCache *cache = p->Cache;
			cache->Refs--;
			if ((p->Flags & PgHdr::PGHDR::DIRTY) == 0)
				Unpin(p);
			else
			{
				// Move the page to the head of the dirty list.
				RemoveFromDirtyList(p);
				AddToDirtyList(p);
			}
		}
	}

	void Ref(PgHdr *p)
	{
		_assert(p->Refs > 0);
		p->Refs++;
	}

	void Drop(PgHdr *p)
	{
		_assert(p->Refs == 1);
		if (p->Flags & PgHdr::PGHDR::DIRTY)
			RemoveFromDirtyList(p);
		PCache *cache = p->Cache;
		cache->Refs--;
		if (p->ID == 1)
			cache->Page1 = nullptr;
		cache->Cache->Unpin(p->Page, true);
	}

	void MakeDirty(PgHdr *p)
	{
		p->Flags &= ~PgHdr::PGHDR::DONT_WRITE;
		_assert(p->Refs > 0);
		if ((p->Flags & PgHdr::PGHDR::DIRTY) == 0)
		{
			p->Flags |= PgHdr::PGHDR::DIRTY;
			AddToDirtyList(p);
		}
	}

	void MakeClean(PgHdr *p)
	{
		if ((p->Flags & PgHdr::PGHDR::DIRTY))
		{
			RemoveFromDirtyList(p);
			p->Flags &= ~(PgHdr::PGHDR::DIRTY | PgHdr::PGHDR::NEED_SYNC);
			if (p->Refs == 0)
				Unpin(p);
		}
	}

	void CleanAll(PCache *cache)
	{
		PgHdr *p;
		while ((p = cache->Dirty) != nullptr)
			MakeClean(p);
	}

	void ClearSyncFlags(PCache *cache)
	{
		for (PgHdr *p = cache->Dirty; p; p = p->DirtyNext)
			p->Flags &= ~PgHdr::PGHDR::NEED_SYNC;
		cache->Synced = cache->DirtyTail;
	}

	void Move(PgHdr *p, Pid newID)
	{
		PCache *cache = p->Cache;
		_assert(p->Refs > 0);
		_assert(newID > 0);
		cache->Cache->Rekey(p->Page, p->ID, newID);
		p->ID = newID;
		if ((p->Flags & PgHdr::PGHDR::DIRTY) && (p->Flags & PgHdr::PGHDR::NEED_SYNC))
		{
			RemoveFromDirtyList(p);
			AddToDirtyList(p);
		}
	}

	void Truncate(PCache *cache, Pid id)
	{
		if (cache->Cache)
		{
			PgHdr *p;
			PgHdr *next;
			for (p = cache->Dirty; p; p = next)
			{
				next = p->DirtyNext;
				// This routine never gets call with a positive pgno except right after sqlite3PcacheCleanAll().  So if there are dirty pages, it must be that pgno==0.
				_assert(p->ID > 0);
				if (SysEx_ALWAYS(p->ID > id))
				{
					_assert(p->Flags & PgHdr::PGHDR::DIRTY);
					MakeClean(p);
				}
			}
			if (id == 0 && cache->Page1)
			{
				_memset(cache->Page1->Data, 0, cache->SizePage);
				id = 1;
			}
			cache->Cache->Truncate(id + 1);
		}
	}

	void Close(PCache *cache)
	{
		if (cache->Cache)
			_pcache->Destroy(cache->Cache);
	}

	void Clear(PCache *cache)
	{
		Truncate(cache, 0); 
	}

	static PgHdr *MergeDirtyList(PgHdr *a, PgHdr *b)
	{
		PgHdr result;
		PgHdr *tail = &result;
		while (a && b)
		{
			if (a->ID < b->ID)
			{
				tail->Dirty = a;
				tail = a;
				a = a->Dirty;
			}
			else
			{
				tail->Dirty = b;
				tail = b;
				b = b->Dirty;
			}
		}
		if (a)
			tail->Dirty = a;
		else if (b)
			tail->Dirty = b;
		else
			tail->Dirty = nullptr;
		return result.Dirty;
	}

#define N_SORT_BUCKET 32

	static PgHdr *SortDirtyList(PgHdr *in)
	{
		PgHdr *a[N_SORT_BUCKET], *p;
		_memset(a, 0, sizeof(a));
		int i;
		while (in)
		{
			p = in;
			in = p->Dirty;
			p->Dirty = nullptr;
			for (i = 0; SysEx_ALWAYS(i < N_SORT_BUCKET - 1); i++)
			{
				if (a[i] == nullptr)
				{
					a[i] = p;
					break;
				}
				else
				{
					p = MergeDirtyList(a[i], p);
					a[i] = nullptr;
				}
			}
			if (SysEx_NEVER(i == N_SORT_BUCKET - 1))
				// To get here, there need to be 2^(N_SORT_BUCKET) elements in the input list.  But that is impossible.
					a[i] = MergeDirtyList(a[i], p);
		}
		p = a[0];
		for (i = 1; i < N_SORT_BUCKET; i++)
			p = MergeDirtyList(p, a[i]);
		return p;
	}

	PgHdr *DirtyList(PCache *cache)
	{
		for (PgHdr *p = cache->Dirty; p; p = p->DirtyNext)
			p->Dirty = p->DirtyNext;
		return SortDirtyList(cache->Dirty);
	}

	int RefCount(PCache *cache)
	{
		return cache->Refs;
	}

	int PageRefcount(PgHdr *p)
	{
		return p->Refs;
	}

	int Pagecount(PCache *cache)
	{
		return (cache->Cache ? cache->Cache->Pagecount() : 0);
	}

	void SetCachesize(PCache *cache, int maxPage)
	{
		cache->SizeCache = maxPage;
		if (cache->Cache)
			cache->Cache->Cachesize(NumberOfCachePages(cache));
	}

	void Shrink(PCache *cache)
	{
		if (cache->Cache)
			cache->Cache->Shrink();
	}

#if defined(CHECK_PAGES) || defined(DEBUG)
	void IterateDirty(PCache *cache, void (*iter)(PgHdr *))
	{
		for (PgHdr *dirty = cache->Dirty; dirty; dirty = dirty->DirtyNext)
			iter(dirty);
	}
#endif

#pragma endregion

#pragma region Test 
#ifdef TEST

	int GetCachesize(PCache *cache)
	{
		return NumberOfCachePages(cache);
	}

#endif
#pragma endregion
}