namespace Core
{
	struct PCache1
	{
		// Cache configuration parameters. Page size (szPage) and the purgeable flag (bPurgeable) are set when the cache is created. nMax may be 
		// modified at any time by a call to the pcache1Cachesize() method. The PGroup mutex must be held when accessing nMax.
		PGroup *Group;					// PGroup this cache belongs to
		int SizePage;                   // Size of allocated pages in bytes
		int SizeExtra;                  // Size of extra space in bytes
		bool Purgeable;					// True if cache is purgeable
		unsigned int Min;				// Minimum number of pages reserved
		unsigned int Max;				// Configured "cache_size" value
		unsigned int N90pct;			// nMax*9/10
		unsigned int MaxKey;			// Largest key seen since xTruncate()

		// Hash table of all pages. The following variables may only be accessed when the accessor is holding the PGroup mutex.
		unsigned int Recyclables;       // Number of pages in the LRU list
		unsigned int Pages;             // Total number of pages in apHash
		unsigned int _0; PgHdr1 **Hash;	// Hash table for fast lookup by key
	};

#define PCache1_EnterMutex(x) MutexEx_Enter((x)->Mutex)
#define PCache1_LeaveMutex(x) MutexEx_Leave((x)->Mutex)
}
