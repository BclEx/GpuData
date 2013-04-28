// pcache1.c
namespace Core
{
	struct PgHdr1
	{
		sqlite3_pcache_page Page;
		unsigned int Key;             // Key value (page number)
		PgHdr1 *Next;                 // Next in hash table chain
		PCache1 *Cache;               // Cache that currently owns this page
		PgHdr1 *LruNext;              // Next in LRU list of unpinned pages
		PgHdr1 *LruPrev;              // Previous in LRU list of unpinned pages
	};
}
