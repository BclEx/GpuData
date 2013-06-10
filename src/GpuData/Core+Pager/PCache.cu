﻿// pcache.c
#include "Core+Pager.cu.h"
using namespace Core;

namespace Core
{
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
	extern IPCache *newPCache1();

	int PCache::Initialize() 
	{ 
		if (_pcache == nullptr)
			_pcache = newPCache1();
		return _pcache->Init(); 
	}
	void PCache::Shutdown()
	{
		_pcache->Shutdown(); 
	}
	int PCache::SizeOf()
	{
		return sizeof(PCache);
	}

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

	uint PCache::get_CacheSize() // NumberOfCachePages
	{
		if (SizeCache >= 0)
			return (uint)SizeCache;
		return (uint)((-1024 * (int64)SizeCache) / (SizePage + SizeExtra));
	}

	RC PCache::Fetch(Pid id, bool createFlag, PgHdr **pageOut)
	{
		_assert(id > 0);
		// If the pluggable cache (sqlite3_pcache*) has not been allocated, allocate it now.
		if (!Cache && createFlag)
		{
			IPCache *p = _pcache->Create(SizePage, SizeExtra + sizeof(PgHdr), Purgeable);
			if (!p)
				return RC::NOMEM;
			p->Cachesize(get_CacheSize());
			Cache = p;
		}
		ICachePage *page = nullptr;
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
			page = Cache->Fetch(id, true);
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

	void PCache::Release(PgHdr *p)
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

	void PCache::Ref(PgHdr *p)
	{
		_assert(p->Refs > 0);
		p->Refs++;
	}

	void PCache::Drop(PgHdr *p)
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

	void PCache::MakeDirty(PgHdr *p)
	{
		p->Flags &= ~PgHdr::PGHDR::DONT_WRITE;
		_assert(p->Refs > 0);
		if ((p->Flags & PgHdr::PGHDR::DIRTY) == 0)
		{
			p->Flags |= PgHdr::PGHDR::DIRTY;
			AddToDirtyList(p);
		}
	}

	void PCache::MakeClean(PgHdr *p)
	{
		if ((p->Flags & PgHdr::PGHDR::DIRTY))
		{
			RemoveFromDirtyList(p);
			p->Flags &= ~(PgHdr::PGHDR::DIRTY | PgHdr::PGHDR::NEED_SYNC);
			if (p->Refs == 0)
				Unpin(p);
		}
	}

	void PCache::CleanAll()
	{
		PgHdr *p;
		while ((p = Dirty) != nullptr)
			MakeClean(p);
	}

	void PCache::ClearSyncFlags()
	{
		for (PgHdr *p = Dirty; p; p = p->DirtyNext)
			p->Flags &= ~PgHdr::PGHDR::NEED_SYNC;
		Synced = DirtyTail;
	}

	void PCache::Move(PgHdr *p, Pid newID)
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

	void PCache::Truncate(Pid id)
	{
		if (Cache)
		{
			PgHdr *p;
			PgHdr *next;
			for (p = Dirty; p; p = next)
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
			if (id == 0 && Page1)
			{
				_memset(Page1->Data, 0, SizePage);
				id = 1;
			}
			Cache->Truncate(id + 1);
		}
	}

	void PCache::Close()
	{
		if (Cache)
			_pcache->Destroy(Cache);
	}

	void PCache::Clear()
	{
		Truncate(0); 
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

	PgHdr *PCache::DirtyList()
	{
		for (PgHdr *p = Dirty; p; p = p->DirtyNext)
			p->Dirty = p->DirtyNext;
		return SortDirtyList(Dirty);
	}

	int PCache::get_Refs()
	{
		return Refs;
	}

	int PCache::get_PageRefs(PgHdr *p)
	{
		return p->Refs;
	}

	int PCache::get_Pages()
	{
		return (Cache ? Cache->get_Pages() : 0);
	}

	void PCache::set_CacheSize(int maxPage)
	{
		SizeCache = maxPage;
		if (Cache)
			Cache->Cachesize(get_CacheSize());
	}

	void PCache::Shrink()
	{
		if (Cache)
			Cache->Shrink();
	}

#if defined(CHECK_PAGES) || defined(_DEBUG)
	void PCache::IterateDirty(void (*iter)(PgHdr *))
	{
		for (PgHdr *dirty = Dirty; dirty; dirty = dirty->DirtyNext)
			iter(dirty);
	}
#endif

#pragma endregion
}