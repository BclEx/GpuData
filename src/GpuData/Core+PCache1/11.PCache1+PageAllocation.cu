// pcache1.c
#include "Core+PCache1.cu.h"
using namespace Core;

namespace Core
{
	static void BufferSetup(void *buffer, int size, int n)
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

	static void *Alloc(int bytes)
	{
		_assert(MutexEx::NotHeld(_pcache1.Group.Mutex));
		StatusEx::StatusSet(StatusEx::STATUS_PAGECACHE_SIZE, bytes);
		PgHdr *p = nullptr;
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
				StatusEx::StatusAdd(StatusEx::STATUS_PAGECACHE_USED, 1);
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
				StatusEx::StatusAdd(StatusEx::STATUS_PAGECACHE_OVERFLOW, size);
				MutexEx::Leave(_pcache1.Mutex);
			}
#endif
			SysEx::MemdebugSetType(p, SysEx::MEMTYPE_PCACHE);
		}
		return p;
	}

	static int Free(void *p)
	{
		int freed = 0;
		if (p == nullptr)
			return 0;
		if (p >= _pcache1.Start && p < _pcache1.End)
		{
			MutexEx::Enter(_pcache1.Mutex);
			StatusEx::StatusAdd(StatusEx::STATUS_PAGECACHE_USED, -1);
			PgFreeslot *slot = (PgFreeslot *)p;
			slot->Next = pcache1.Free;
			_pcache1.Free = slot;
			_pcache1.FreeSlots++;
			_pcache1.UnderPressure = (_pcache1.FreeSlots < _pcache1.Reserves);
			_assert(pcache1.FreeSlots <= _pcache1.Slots);
			MutexEx::Leave(_pcache1.Mutex);
		}
		else
		{
			_assert(SysEx::MemdebugHasType(p, SysEx::MEMTYPE_PCACHE));
			SysEx::MemdebugSetType(p, SysEx::MEMTYPE_HEAP);
			freed = SysEx::MallocSize(p);
#ifndef DISABLE_PAGECACHE_OVERFLOW_STATS
			MutexEx::Enter(_pcache1.Mutex);
			StatusEx::StatusAdd(StatusEx::STATUS_PAGECACHE_OVERFLOW, -freed);
			Mutex::Leave(_pcache1.Mutex);
#endif
			SysEx::Free(p);
		}
		return freed;
	}

#ifdef ENABLE_MEMORY_MANAGEMENT
	static int MemSize(void *p)
	{
		if (p >= pcache1.Start && p < pcache1.End)
			return pcache1.SizeSlot;
		assert(SysEx::MemdebugHasType(p, SysEx::MEMTYPE_PCACHE) );
		SysEx::MemdebugSetType(p, SysEx::MEMTYPE_HEAP);
		int size = SysEx::MallocSize(p);
		SysEx::MemdebugSetType(p, SysEx::MEMTYPE_PCACHE);
		return size;
	}
#endif

	static PgHdr1 *AllocPage(PCache1 *t)
	{
		// The group mutex must be released before pcache1Alloc() is called. This is because it may call sqlite3_release_memory(), which assumes that this mutex is not held.
		assert(MutexEx::Held(t->Group->Mutex));
		PCache1::LeaveMutex(t->Group);
		PgHdr1 *p = nullptr;
		void *pPg;
#ifdef PCACHE_SEPARATE_HEADER
		pPg = Alloc(t->SizePage);
		p = SysEx::Malloc(sizeof(PgHdr1) + t->SizeExtra);
		if (!pPg || !p)
		{
			Free(pPg);
SysEx:Free(p);
			pPg = nullptr;
		}
#else
		pPg = Alloc(sizeof(PgHdr1) + t->SizePage + t->SizeExtra);
		p = (PgHdr1 *)&((u8 *)pPg)[t->SizePage];
#endif
		PCache1::EnterMutex(t->Group);
		if (pPg)
		{
			p->Page.Buffer = pPg;
			p->Page.Extra = &p[1];
			if (t->Purgeable)
				t->Group->CurrentPages++;
			return p;
		}
		return nullptr;
	}

	static void FreePage(PgHdr1 *p)
	{
		if (SysEx::ALWAYS(p))
		{
			PCache1 *cache = p->Cache;
			assert(MutexEx::Held(p->Cache->Group->Mutex));
			Free(p->Page.Buffer);
#ifdef PCACHE_SEPARATE_HEADER
			SysEx::Free(p);
#endif
			if (cache->Purgeable)
				cache->Group->CurrentPages--;
		}
	}

	/*
	** Malloc function used by SQLite to obtain space from the buffer configured
	** using sqlite3_config(SQLITE_CONFIG_PAGECACHE) option. If no such buffer
	** exists, this function falls back to sqlite3Malloc().
	*/
	void *sqlite3PageMalloc(int sz){
		return pcache1Alloc(sz);
	}

	/*
	** Free an allocated buffer obtained from sqlite3PageMalloc().
	*/
	void sqlite3PageFree(void *p){
		pcache1Free(p);
	}


	/*
	** Return true if it desirable to avoid allocating a new page cache
	** entry.
	**
	** If memory was allocated specifically to the page cache using
	** SQLITE_CONFIG_PAGECACHE but that memory has all been used, then
	** it is desirable to avoid allocating a new page cache entry because
	** presumably SQLITE_CONFIG_PAGECACHE was suppose to be sufficient
	** for all page cache needs and we should not need to spill the
	** allocation onto the heap.
	**
	** Or, the heap is used for all page cache memory but the heap is
	** under memory pressure, then again it is desirable to avoid
	** allocating a new page cache entry in order to avoid stressing
	** the heap even further.
	*/
	static int pcache1UnderMemoryPressure(PCache1 *pCache){
		if( pcache1.nSlot && (pCache->szPage+pCache->szExtra)<=pcache1.szSlot ){
			return pcache1.bUnderPressure;
		}else{
			return sqlite3HeapNearlyFull();
		}
	}

}
