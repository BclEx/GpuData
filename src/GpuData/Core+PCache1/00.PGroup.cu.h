// pcache1.c
namespace Core
{
	struct PGroup 
	{
		MutexEx Mutex;					// MUTEX_STATIC_LRU or NULL
		unsigned int MaxPages;			// Sum of nMax for purgeable caches
		unsigned int MinPages;			// Sum of nMin for purgeable caches
		unsigned int MaxPinned;         // nMaxpage + 10 - nMinPage
		unsigned int CurrentPage;		// Number of purgeable pages allocated
		PgHdr1 *LruHead, *LruTail;		// LRU list of unpinned pages
	};
}
