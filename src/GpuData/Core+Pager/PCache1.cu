﻿// pcache1.c
#include "Core+Pager.cu.h"

namespace Core
{
#pragma region Struct

	typedef struct PgHdr1 PgHdr1;

	struct PGroup 
	{
		MutexEx Mutex;					// MUTEX_STATIC_LRU or NULL
		uint MaxPages;					// Sum of nMax for purgeable caches
		uint MinPages;					// Sum of nMin for purgeable caches
		uint MaxPinned;					// nMaxpage + 10 - nMinPage
		uint CurrentPages;				// Number of purgeable pages allocated
		PgHdr1 *LruHead, *LruTail;		// LRU list of unpinned pages
	};

	class PCache1 : IPCache
	{
	public:
		// Cache configuration parameters. Page size (szPage) and the purgeable flag (bPurgeable) are set when the cache is created. nMax may be 
		// modified at any time by a call to the pcache1Cachesize() method. The PGroup mutex must be held when accessing nMax.
		PGroup *Group;			// PGroup this cache belongs to
		int SizePage;           // Size of allocated pages in bytes
		int SizeExtra;          // Size of extra space in bytes
		bool Purgeable;			// True if cache is purgeable
		uint Min;				// Minimum number of pages reserved
		uint Max;				// Configured "cache_size" value
		uint N90pct;			// nMax*9/10
		uint MaxID;				// Largest key seen since xTruncate()
		// Hash table of all pages. The following variables may only be accessed when the accessor is holding the PGroup mutex.
		uint Recyclables;       // Number of pages in the LRU list
		uint Pages;             // Total number of pages in apHash
		uint _0; PgHdr1 **Hash;	// Hash table for fast lookup by key
	public:
		//static void *PageAlloc(int size);
		//static void PageFree(void *p);
		//bool UnderMemoryPressure();
		//
		RC Init();
		void Shutdown();
		IPCache *Create(int sizePage, int sizeExtra, bool purgeable);
		void Cachesize(uint max);
		void Shrink();
		int get_Pages();
		ICachePage *Fetch(Pid id, bool createFlag);
		void Unpin(ICachePage *pg, bool reuseUnlikely);
		void Rekey(ICachePage *pg, Pid old, Pid new_);
		void Truncate(Pid limit);
		void Destroy(IPCache *p);
	};

	struct PgHdr1
	{
		ICachePage Page;
		Pid ID;					// Key value (page number)
		PgHdr1 *Next;			// Next in hash table chain
		PCache1 *Cache;			// Cache that currently owns this page
		PgHdr1 *LruNext;		// Next in LRU list of unpinned pages
		PgHdr1 *LruPrev;		// Previous in LRU list of unpinned pages
	};

	struct PgFreeslot
	{
		PgFreeslot *Next;		// Next free slot
	};

	struct PCacheGlobal
	{
		PGroup Group;			// The global PGroup for mode (2)
		// Variables related to SQLITE_CONFIG_PAGECACHE settings.  The szSlot, nSlot, pStart, pEnd, nReserve, and isInit values are all
		// fixed at sqlite3_initialize() time and do not require mutex protection. The nFreeSlot and pFree values do require mutex protection.
		bool IsInit;			// True if initialized
		int SizeSlot;			// Size of each free slot
		int Slots;				// The number of pcache slots
		int Reserves;			// Try to keep nFreeSlot above this
		void *Start, *End;		// Bounds of pagecache malloc range
		// Above requires no mutex.  Use mutex below for variable that follow.
		MutexEx Mutex;			// Mutex for accessing the following:
		PgFreeslot *Free;		// Free page blocks
		int FreeSlots;			// Number of unused pcache slots
		// The following value requires a mutex to change.  We skip the mutex on reading because (1) most platforms read a 32-bit integer atomically and
		// (2) even if an incorrect value is read, no great harm is done since this is really just an optimization.
		bool UnderPressure;		// True if low on PAGECACHE memory
	};

#pragma endregion

	static struct PCacheGlobal _pcache1;
	static bool _config_coreMutex = false;

#pragma region Page Allocation

	void BufferSetup(void *buffer, int size, int n)
	{
		if (_pcache1.IsInit)
		{
			size = SysEx_ROUNDDOWN8(size);
			_pcache1.SizeSlot = size;
			_pcache1.Slots = _pcache1.FreeSlots = n;
			_pcache1.Reserves = (n > 90 ? 10 : (n / 10 + 1));
			_pcache1.Start = buffer;
			_pcache1.Free = nullptr;
			_pcache1.UnderPressure = false;
			while (n--)
			{
				PgFreeslot *p = (PgFreeslot *)buffer;
				p->Next = _pcache1.Free;
				_pcache1.Free = p;
				buffer = (void *)&((char *)buffer)[size];
			}
			_pcache1.End = buffer;
		}
	}

	void *Alloc(int bytes)
	{
		_assert(MutexEx::NotHeld(_pcache1.Group.Mutex));
		StatusEx::StatusSet(StatusEx::STATUS::PAGECACHE_SIZE, bytes);
		void *p = nullptr;
		if (bytes <= _pcache1.SizeSlot)
		{
			MutexEx::Enter(_pcache1.Mutex);
			p = (PgHdr1 *)_pcache1.Free;
			if (p)
			{
				_pcache1.Free = _pcache1.Free->Next;
				_pcache1.FreeSlots--;
				_pcache1.UnderPressure = (_pcache1.FreeSlots < _pcache1.Reserves);
				_assert(_pcache1.FreeSlots >= 0);
				StatusEx::StatusAdd(StatusEx::STATUS::PAGECACHE_USED, 1);
			}
			MutexEx::Leave(_pcache1.Mutex);
		}
		if (!p)
		{
			// Memory is not available in the SQLITE_CONFIG_PAGECACHE pool.  Get it from sqlite3Malloc instead.
			p = SysEx::Alloc(bytes);
#ifndef DISABLE_PAGECACHE_OVERFLOW_STATS
			if (p)
			{
				int size = SysEx::AllocSize(p);
				MutexEx::Enter(_pcache1.Mutex);
				StatusEx::StatusAdd(StatusEx::STATUS::PAGECACHE_OVERFLOW, size);
				MutexEx::Leave(_pcache1.Mutex);
			}
#endif
			SysEx::MemdebugSetType(p, SysEx::MEMTYPE::PCACHE);
		}
		return p;
	}

	int Free(void *p)
	{
		int freed = 0;
		if (p == nullptr)
			return 0;
		if (p >= _pcache1.Start && p < _pcache1.End)
		{
			MutexEx::Enter(_pcache1.Mutex);
			StatusEx::StatusAdd(StatusEx::STATUS::PAGECACHE_USED, -1);
			PgFreeslot *slot = (PgFreeslot *)p;
			slot->Next = _pcache1.Free;
			_pcache1.Free = slot;
			_pcache1.FreeSlots++;
			_pcache1.UnderPressure = (_pcache1.FreeSlots < _pcache1.Reserves);
			_assert(_pcache1.FreeSlots <= _pcache1.Slots);
			MutexEx::Leave(_pcache1.Mutex);
		}
		else
		{
			_assert(SysEx::MemdebugHasType(p, SysEx::MEMTYPE::PCACHE));
			SysEx::MemdebugSetType(p, SysEx::MEMTYPE::HEAP);
			freed = SysEx::AllocSize(p);
#ifndef DISABLE_PAGECACHE_OVERFLOW_STATS
			MutexEx::Enter(_pcache1.Mutex);
			StatusEx::StatusAdd(StatusEx::STATUS::PAGECACHE_OVERFLOW, -freed);
			MutexEx::Leave(_pcache1.Mutex);
#endif
			SysEx::Free(p);
		}
		return freed;
	}

#ifdef ENABLE_MEMORY_MANAGEMENT
	static int MemSize(void *p)
	{
		if (p >= _pcache1.Start && p < _pcache1.End)
			return _pcache1.SizeSlot;
		_assert(SysEx::MemdebugHasType(p, SysEx::MEMTYPE::PCACHE));
		SysEx::MemdebugSetType(p, SysEx::MEMTYPE::HEAP);
		int size = SysEx::AllocSize(p);
		SysEx::MemdebugSetType(p, SysEx::MEMTYPE::PCACHE);
		return size;
	}
#endif

	static PgHdr1 *AllocPage(PCache1 *cache)
	{
		// The group mutex must be released before pcache1Alloc() is called. This is because it may call sqlite3_release_memory(), which assumes that this mutex is not held.
		_assert(MutexEx::Held(cache->Group->Mutex));
		MutexEx::Leave(cache->Group->Mutex);
		PgHdr1 *p = nullptr;
		void *pg;
#ifdef PCACHE_SEPARATE_HEADER
		pg = Alloc(cache->SizePage);
		p = (PgHdr1 *)SysEx::Alloc(sizeof(PgHdr1) + cache->SizeExtra);
		if (!pg || !p)
		{
			Free(pg);
			SysEx::Free(p);
			pg = nullptr;
		}
#else
		pg = Alloc(sizeof(PgHdr1) + cache->SizePage + cache->SizeExtra);
		p = (PgHdr1 *)&((uint8 *)pg)[cache->SizePage];
#endif
		MutexEx::Enter(cache->Group->Mutex);
		if (pg)
		{
			p->Page.Buffer = pg;
			p->Page.Extra = &p[1];
			if (cache->Purgeable)
				cache->Group->CurrentPages++;
			return p;
		}
		return nullptr;
	}

	static void FreePage(PgHdr1 *p)
	{
		if (SysEx_ALWAYS(p))
		{
			PCache1 *cache = p->Cache;
			_assert(MutexEx::Held(p->Cache->Group->Mutex));
			Free(p->Page.Buffer);
#ifdef PCACHE_SEPARATE_HEADER
			SysEx::Free(p);
#endif
			if (cache->Purgeable)
				cache->Group->CurrentPages--;
		}
	}

	__device__ static bool UnderMemoryPressure(PCache1 *cache)
	{
		return (_pcache1.Slots && (cache->SizePage + cache->SizeExtra) <= _pcache1.SizeSlot ? _pcache1.UnderPressure : SysEx::HeapNearlyFull());
	}

#pragma endregion

#pragma region General

	static int ResizeHash(PCache1 *p)
	{
		_assert(MutexEx::Held(p->Group->Mutex));
		uint newLength = __arrayLength(p->Hash) * 2;
		if (newLength < 256)
			newLength = 256;
		MutexEx::Leave(p->Group->Mutex);
		if (__arrayLength(p->Hash)) SysEx::BeginBenignAlloc();
		PgHdr1 **newHash = (PgHdr1 **)SysEx::Alloc(sizeof(PgHdr1 *) * newLength, true);
		if (__arrayLength(p->Hash)) SysEx::EndBenignAlloc();
		MutexEx::Enter(p->Group->Mutex);
		if (newHash)
		{
			for (uint i = 0; i < __arrayLength(p->Hash); i++)
			{
				PgHdr1 *page;
				PgHdr1 *next = p->Hash[i];
				while ((page = next) != 0)
				{
					uint h = (page->ID % newLength);
					next = page->Next;
					page->Next = newHash[h];
					newHash[h] = page;
				}
			}
			SysEx::Free(p->Hash);
			p->Hash = __arraySet(newHash, newLength);
		}
		return (p->Hash ? RC::OK : RC::NOMEM);
	}

	static void PinPage(PgHdr1 *page)
	{
		if (page == nullptr)
			return;
		PCache1 *cache = page->Cache;
		PGroup *group = cache->Group;
		_assert(MutexEx::Held(group->Mutex));
		if (page->LruNext || page == group->LruTail)
		{
			if (page->LruPrev)
				page->LruPrev->LruNext = page->LruNext;
			if (page->LruNext)
				page->LruNext->LruPrev = page->LruPrev;
			if (group->LruHead == page)
				group->LruHead = page->LruNext;
			if (group->LruTail == page)
				group->LruTail = page->LruPrev;
			page->LruNext = 0;
			page->LruPrev = 0;
			page->Cache->Recyclables--;
		}
	}

	static void RemoveFromHash(PgHdr1 *page)
	{
		PCache1 *cache = page->Cache;
		_assert(MutexEx::Held(cache->Group->Mutex));
		uint h = (page->ID % __arrayLength(cache->Hash));
		PgHdr1 **pp;
		for (pp = &cache->Hash[h]; (*pp) != page; pp = &(*pp)->Next);
		*pp = (*pp)->Next;
		cache->Pages--;
	}

	static void EnforceMaxPage(PGroup *group)
	{
		_assert(MutexEx::Held(group->Mutex));
		while (group->CurrentPages > group->MaxPages && group->LruTail)
		{
			PgHdr1 *p = group->LruTail;
			_assert(p->Cache->Group == group);
			PinPage(p);
			RemoveFromHash(p);
			FreePage(p);
		}
	}

	static void TruncateUnsafe(PCache1 *p, Pid limit)
	{
		ASSERTONLY(uint pages = 0;)
			_assert(MutexEx::Held(p->Group->Mutex));
		for (uint h = 0; h < __arrayLength(p->Hash); h++)
		{
			PgHdr1 **pp = &p->Hash[h]; 
			PgHdr1 *page;
			while ((page = *pp) != 0)
			{
				if (page->ID >= limit)
				{
					p->Pages--;
					*pp = page->Next;
					PinPage(page);
					FreePage(page);
				}
				else
				{
					pp = &page->Next;
					ASSERTONLY(pages++;)
				}
			}
		}
		_assert(p->Pages == pages);
	}

#pragma endregion

#pragma region Interface

	IPCache *newPCache1() { return (IPCache *)new PCache1(); }

	RC PCache1::Init()
	{
		_assert(!_pcache1.IsInit);
		_memset(&_pcache1, 0, sizeof(_pcache1));
		if (_config_coreMutex)
		{
			_pcache1.Group.Mutex = MutexEx::Alloc(MutexEx::MUTEX::STATIC_LRU);
			_pcache1.Mutex = MutexEx::Alloc(MutexEx::MUTEX::STATIC_PMEM);
		}
		_pcache1.Group.MaxPinned = 10;
		_pcache1.IsInit = true;
		return RC::OK;
	}

	void PCache1::Shutdown()
	{
		_assert(_pcache1.IsInit);
		_memset(&_pcache1, 0, sizeof(_pcache1));
	}

	IPCache *PCache1::Create(int sizePage, int sizeExtra, bool purgeable)
	{
		// The seperateCache variable is true if each PCache has its own private PGroup.  In other words, separateCache is true for mode (1) where no
		// mutexing is required.
		// *  Always use a unified cache (mode-2) if ENABLE_MEMORY_MANAGEMENT
		// *  Always use a unified cache in single-threaded applications
		// *  Otherwise (if multi-threaded and ENABLE_MEMORY_MANAGEMENT is off) use separate caches (mode-1)
#if defined(ENABLE_MEMORY_MANAGEMENT) || THREADSAFE == 0
		const bool separateCache = false;
#else
		bool separateCache = _config_coreMutex > 0;
#endif
		_assert((sizePage & (sizePage - 1)) == 0 && sizePage >= 512 && sizePage <= 65536);
		_assert(sizeExtra < 300);
		int size = sizeof(PCache1) + sizeof(PGroup) * (int)separateCache;
		PCache1 *cache = (PCache1 *)SysEx::Alloc(size, true);
		if (cache)
		{
			PGroup *group;
			if (separateCache)
			{
				group = (PGroup*)&cache[1];
				group->MaxPinned = 10;
			}
			else
				group = &_pcache1.Group;
			cache->Group = group;
			cache->SizePage = sizePage;
			cache->SizeExtra = sizeExtra;
			cache->Purgeable = purgeable;
			if (purgeable)
			{
				cache->Min = 10;
				MutexEx::Enter(group->Mutex);
				group->MinPages += cache->Min;
				group->MaxPinned = group->MaxPages + 10 - group->MinPages;
				MutexEx::Leave(group->Mutex);
			}
		}
		return (IPCache *)cache;
	}

	void PCache1::Cachesize(uint max)
	{
		if (Purgeable)
		{
			PGroup *group = Group;
			MutexEx::Enter(group->Mutex);
			group->MaxPages += (max - Max);
			group->MaxPinned = group->MaxPages + 10 - group->MinPages;
			Max = max;
			N90pct = Max * 9 / 10;
			EnforceMaxPage(group);
			MutexEx::Leave(group->Mutex);
		}
	}

	void PCache1::Shrink()
	{
		if (Purgeable)
		{
			PGroup *group = Group;
			MutexEx::Enter(group->Mutex);
			uint savedMaxPages = group->MaxPages;
			group->MaxPages = 0;
			EnforceMaxPage(group);
			group->MaxPages = savedMaxPages;
			MutexEx::Leave(group->Mutex);
		}
	}

	int PCache1::get_Pages()
	{
		MutexEx::Enter(Group->Mutex);
		int pages = Pages;
		MutexEx::Leave(Group->Mutex);
		return pages;
	}

	ICachePage *PCache1::Fetch(Pid id, bool createFlag)
	{
		_assert(Purgeable || !createFlag);
		_assert(Purgeable || Min == 0);
		_assert(!Purgeable || Min == 10);
		PGroup *group;
		MutexEx::Enter((group = Group)->Mutex);

		// Step 1: Search the hash table for an existing entry.
		PgHdr1 *page = nullptr;
		if (__arrayLength(Hash) > 0)
		{
			uint h = (id % __arrayLength(Hash));
			for (page = Hash[h]; page && page->ID != id; page = page->Next) ;
		}

		// Step 2: Abort if no existing page is found and createFlag is 0
		if (page || !createFlag)
		{
			PinPage(page);
			goto fetch_out;
		}

		// The pGroup local variable will normally be initialized by the pcache1EnterMutex() macro above.  But if SQLITE_MUTEX_OMIT is defined,
		// then pcache1EnterMutex() is a no-op, so we have to initialize the local variable here.  Delaying the initialization of pGroup is an
		// optimization:  The common case is to exit the module before reaching this point.
#ifdef MUTEX_OMIT
		group = cache->Group;
#endif

		// Step 3: Abort if createFlag is 1 but the cache is nearly full
		_assert(Pages >= Recyclables);
		uint pinned = Pages - Recyclables;	
		_assert(group->MaxPinned == group->MaxPages + 10 - group->MinPages);
		_assert(N90pct == Max * 9 / 10);
		if (createFlag && (pinned >= group->MaxPinned || pinned >= N90pct || UnderMemoryPressure(this)))
			goto fetch_out;
		if (Pages >= __arrayLength(Hash) && ResizeHash(this))
			goto fetch_out;

		// Step 4. Try to recycle a page.
		if (Purgeable && group->LruTail && ((Pages + 1 >= Max) || group->CurrentPages >= group->MaxPages || UnderMemoryPressure(this)))
		{
			page = group->LruTail;
			RemoveFromHash(page);
			PinPage(page);
			PCache1 *other = page->Cache;

			// We want to verify that szPage and szExtra are the same for pOther and pCache.  Assert that we can verify this by comparing sums.
			_assert((SizePage & (SizePage - 1)) == 0 && SizePage >= 512);
			_assert(SizeExtra < 512);
			_assert((other->SizePage & (other->SizePage - 1)) == 0 && other->SizePage >= 512);
			_assert(other->SizeExtra < 512);

			if (other->SizePage + other->SizeExtra != SizePage + SizeExtra)
			{
				FreePage(page);
				page = nullptr;
			}
			else
				group->CurrentPages -= (other->Purgeable - Purgeable);
		}

		// Step 5. If a usable page buffer has still not been found, attempt to allocate a new one. 
		if (!page)
		{
			if (createFlag) SysEx::BeginBenignAlloc();
			page = AllocPage(this);
			if (createFlag) SysEx::EndBenignAlloc();
		}
		if (page)
		{
			uint h = (id % __arrayLength(Hash));
			Pages++;
			page->ID = id;
			page->Next = Hash[h];
			page->Cache = this;
			page->LruPrev = nullptr;
			page->LruNext = nullptr;
			*(void **)page->Page.Extra = nullptr;
			Hash[h] = page;
		}

fetch_out:
		if (page && id > MaxID)
			MaxID = id;
		MutexEx::Leave(group->Mutex);
		return &page->Page;
	}

	void PCache1::Unpin(ICachePage *pg, bool reuseUnlikely)
	{
		PgHdr1 *page = (PgHdr1 *)pg;
		PGroup *group = Group;
		_assert(page->Cache == this);
		MutexEx::Enter(group->Mutex);
		// It is an error to call this function if the page is already part of the PGroup LRU list.
		_assert(page->LruPrev == nullptr && page->LruNext == nullptr);
		_assert(group->LruHead != page && group->LruTail != page);
		if (reuseUnlikely || group->CurrentPages > group->MaxPages)
		{
			RemoveFromHash(page);
			FreePage(page);
		}
		else
		{
			// Add the page to the PGroup LRU list.
			if (group->LruHead)
			{
				group->LruHead->LruPrev = page;
				page->LruNext = group->LruHead;
				group->LruHead = page;
			}
			else
			{
				group->LruTail = page;
				group->LruHead = page;
			}
			Recyclables++;
		}
		MutexEx::Leave(Group->Mutex);
	}

	void PCache1::Rekey(ICachePage *pg, Pid old, Pid new_)
	{
		PgHdr1 *page = (PgHdr1 *)pg;
		_assert(page->ID == old);
		_assert(page->Cache == this);
		MutexEx::Enter(Group->Mutex);
		uint h = (old % __arrayLength(Hash));
		PgHdr1 **pp = &Hash[h];
		while ((*pp) != page)
			pp = &(*pp)->Next;
		*pp = page->Next;
		h = (new_ % __arrayLength(Hash));
		page->ID = new_;
		page->Next = Hash[h];
		Hash[h] = page;
		if (new_ > MaxID)
			MaxID = new_;
		MutexEx::Leave(Group->Mutex);
	}

	void PCache1::Truncate(Pid limit)
	{
		MutexEx::Enter(Group->Mutex);
		if (limit <= MaxID)
		{
			TruncateUnsafe(this, limit);
			MaxID = limit - 1;
		}
		MutexEx::Leave(Group->Mutex);
	}

	void PCache1::Destroy(IPCache *p)
	{
		PCache1 *cache = (PCache1 *)p;
		PGroup *group = cache->Group;
		_assert(cache->Purgeable || (cache->Max == 0 && cache->Min == 0));
		MutexEx::Enter(group->Mutex);
		TruncateUnsafe(cache, 0);
		_assert(group->MaxPages >= cache->Max);
		group->MaxPages -= cache->Max;
		_assert(group->MinPages >= cache->Min);
		group->MinPages -= cache->Min;
		group->MaxPinned = group->MaxPages + 10 - group->MinPages;
		EnforceMaxPage(group);
		MutexEx::Leave(group->Mutex);
		SysEx::Free(cache->Hash);
		SysEx::Free(cache);
	}

#ifdef ENABLE_MEMORY_MANAGEMENT
	int PCache::ReleaseMemory(int required)
	{
		_assert(MutexEx::NotHeld(_pcache1.Group.Mutex));
		_assert(MutexEx::NotHeld(_pcache1.Mutex));
		int free = 0;
		if (_pcache1.Start == nullptr)
		{
			PgHdr1 *p;
			MutexEx::Enter(_pcache1.Group.Mutex);
			while ((required < 0 || free < required) && ((p = _pcache1.Group.LruTail) != nullptr))
			{
				free += MemSize(p->Page.Buffer);
#ifdef PCACHE_SEPARATE_HEADER
				free += MemSize(p);
#endif
				PinPage(p);
				RemoveFromHash(p);
				FreePage(p);
			}
			MutexEx::Leave(_pcache1.Group.Mutex);
		}
		return free;
	}
#endif

#pragma endregion

#pragma	region Tests
#ifdef TEST

	__device__ void PCache1_testStats(uint *current, uint *max, uint *min, uint *recyclables)
	{
		uint recyclables2 = 0;
		for (PgHdr1 *p = _pcache1.Group.LruHead; p; p = p->LruNext)
			recyclables2++;
		*current = _pcache1.Group.CurrentPages;
		*max = _pcache1.Group.MaxPages;
		*min = _pcache1.Group.MinPages;
		*recyclables = recyclables2;
	}

#endif
#pragma endregion
}
