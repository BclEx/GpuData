namespace Core
{
	
/*
** Implementation of the sqlite3_pcache.xInit method.
*/
static int pcache1Init(void *NotUsed){
  UNUSED_PARAMETER(NotUsed);
  assert( pcache1.isInit==0 );
  memset(&pcache1, 0, sizeof(pcache1));
  if( sqlite3GlobalConfig.bCoreMutex ){
    pcache1.grp.mutex = sqlite3_mutex_alloc(SQLITE_MUTEX_STATIC_LRU);
    pcache1.mutex = sqlite3_mutex_alloc(SQLITE_MUTEX_STATIC_PMEM);
  }
  pcache1.grp.mxPinned = 10;
  pcache1.isInit = 1;
  return SQLITE_OK;
}

/*
** Implementation of the sqlite3_pcache.xShutdown method.
** Note that the static mutex allocated in xInit does 
** not need to be freed.
*/
static void pcache1Shutdown(void *NotUsed){
  UNUSED_PARAMETER(NotUsed);
  assert( pcache1.isInit!=0 );
  memset(&pcache1, 0, sizeof(pcache1));
}

/*
** Implementation of the sqlite3_pcache.xCreate method.
**
** Allocate a new cache.
*/
static sqlite3_pcache *pcache1Create(int szPage, int szExtra, int bPurgeable){
  PCache1 *pCache;      /* The newly created page cache */
  PGroup *pGroup;       /* The group the new page cache will belong to */
  int sz;               /* Bytes of memory required to allocate the new cache */

  /*
  ** The seperateCache variable is true if each PCache has its own private
  ** PGroup.  In other words, separateCache is true for mode (1) where no
  ** mutexing is required.
  **
  **   *  Always use a unified cache (mode-2) if ENABLE_MEMORY_MANAGEMENT
  **
  **   *  Always use a unified cache in single-threaded applications
  **
  **   *  Otherwise (if multi-threaded and ENABLE_MEMORY_MANAGEMENT is off)
  **      use separate caches (mode-1)
  */
#if defined(SQLITE_ENABLE_MEMORY_MANAGEMENT) || SQLITE_THREADSAFE==0
  const int separateCache = 0;
#else
  int separateCache = sqlite3GlobalConfig.bCoreMutex>0;
#endif

  assert( (szPage & (szPage-1))==0 && szPage>=512 && szPage<=65536 );
  assert( szExtra < 300 );

  sz = sizeof(PCache1) + sizeof(PGroup)*separateCache;
  pCache = (PCache1 *)sqlite3MallocZero(sz);
  if( pCache ){
    if( separateCache ){
      pGroup = (PGroup*)&pCache[1];
      pGroup->mxPinned = 10;
    }else{
      pGroup = &pcache1.grp;
    }
    pCache->pGroup = pGroup;
    pCache->szPage = szPage;
    pCache->szExtra = szExtra;
    pCache->bPurgeable = (bPurgeable ? 1 : 0);
    if( bPurgeable ){
      pCache->nMin = 10;
      pcache1EnterMutex(pGroup);
      pGroup->nMinPage += pCache->nMin;
      pGroup->mxPinned = pGroup->nMaxPage + 10 - pGroup->nMinPage;
      pcache1LeaveMutex(pGroup);
    }
  }
  return (sqlite3_pcache *)pCache;
}

/*
** Implementation of the sqlite3_pcache.xCachesize method. 
**
** Configure the cache_size limit for a cache.
*/
static void pcache1Cachesize(sqlite3_pcache *p, int nMax){
  PCache1 *pCache = (PCache1 *)p;
  if( pCache->bPurgeable ){
    PGroup *pGroup = pCache->pGroup;
    pcache1EnterMutex(pGroup);
    pGroup->nMaxPage += (nMax - pCache->nMax);
    pGroup->mxPinned = pGroup->nMaxPage + 10 - pGroup->nMinPage;
    pCache->nMax = nMax;
    pCache->n90pct = pCache->nMax*9/10;
    pcache1EnforceMaxPage(pGroup);
    pcache1LeaveMutex(pGroup);
  }
}

/*
** Implementation of the sqlite3_pcache.xShrink method. 
**
** Free up as much memory as possible.
*/
static void pcache1Shrink(sqlite3_pcache *p){
  PCache1 *pCache = (PCache1*)p;
  if( pCache->bPurgeable ){
    PGroup *pGroup = pCache->pGroup;
    int savedMaxPage;
    pcache1EnterMutex(pGroup);
    savedMaxPage = pGroup->nMaxPage;
    pGroup->nMaxPage = 0;
    pcache1EnforceMaxPage(pGroup);
    pGroup->nMaxPage = savedMaxPage;
    pcache1LeaveMutex(pGroup);
  }
}

/*
** Implementation of the sqlite3_pcache.xPagecount method. 
*/
static int pcache1Pagecount(sqlite3_pcache *p){
  int n;
  PCache1 *pCache = (PCache1*)p;
  pcache1EnterMutex(pCache->pGroup);
  n = pCache->nPage;
  pcache1LeaveMutex(pCache->pGroup);
  return n;
}

/*
** Implementation of the sqlite3_pcache.xFetch method. 
**
** Fetch a page by key value.
**
** Whether or not a new page may be allocated by this function depends on
** the value of the createFlag argument.  0 means do not allocate a new
** page.  1 means allocate a new page if space is easily available.  2 
** means to try really hard to allocate a new page.
**
** For a non-purgeable cache (a cache used as the storage for an in-memory
** database) there is really no difference between createFlag 1 and 2.  So
** the calling function (pcache.c) will never have a createFlag of 1 on
** a non-purgeable cache.
**
** There are three different approaches to obtaining space for a page,
** depending on the value of parameter createFlag (which may be 0, 1 or 2).
**
**   1. Regardless of the value of createFlag, the cache is searched for a 
**      copy of the requested page. If one is found, it is returned.
**
**   2. If createFlag==0 and the page is not already in the cache, NULL is
**      returned.
**
**   3. If createFlag is 1, and the page is not already in the cache, then
**      return NULL (do not allocate a new page) if any of the following
**      conditions are true:
**
**       (a) the number of pages pinned by the cache is greater than
**           PCache1.nMax, or
**
**       (b) the number of pages pinned by the cache is greater than
**           the sum of nMax for all purgeable caches, less the sum of 
**           nMin for all other purgeable caches, or
**
**   4. If none of the first three conditions apply and the cache is marked
**      as purgeable, and if one of the following is true:
**
**       (a) The number of pages allocated for the cache is already 
**           PCache1.nMax, or
**
**       (b) The number of pages allocated for all purgeable caches is
**           already equal to or greater than the sum of nMax for all
**           purgeable caches,
**
**       (c) The system is under memory pressure and wants to avoid
**           unnecessary pages cache entry allocations
**
**      then attempt to recycle a page from the LRU list. If it is the right
**      size, return the recycled buffer. Otherwise, free the buffer and
**      proceed to step 5. 
**
**   5. Otherwise, allocate and return a new page buffer.
*/
static sqlite3_pcache_page *pcache1Fetch(
  sqlite3_pcache *p, 
  unsigned int iKey, 
  int createFlag
){
  unsigned int nPinned;
  PCache1 *pCache = (PCache1 *)p;
  PGroup *pGroup;
  PgHdr1 *pPage = 0;

  assert( pCache->bPurgeable || createFlag!=1 );
  assert( pCache->bPurgeable || pCache->nMin==0 );
  assert( pCache->bPurgeable==0 || pCache->nMin==10 );
  assert( pCache->nMin==0 || pCache->bPurgeable );
  pcache1EnterMutex(pGroup = pCache->pGroup);

  /* Step 1: Search the hash table for an existing entry. */
  if( pCache->nHash>0 ){
    unsigned int h = iKey % pCache->nHash;
    for(pPage=pCache->apHash[h]; pPage&&pPage->iKey!=iKey; pPage=pPage->pNext);
  }

  /* Step 2: Abort if no existing page is found and createFlag is 0 */
  if( pPage || createFlag==0 ){
    pcache1PinPage(pPage);
    goto fetch_out;
  }

  /* The pGroup local variable will normally be initialized by the
  ** pcache1EnterMutex() macro above.  But if SQLITE_MUTEX_OMIT is defined,
  ** then pcache1EnterMutex() is a no-op, so we have to initialize the
  ** local variable here.  Delaying the initialization of pGroup is an
  ** optimization:  The common case is to exit the module before reaching
  ** this point.
  */
#ifdef SQLITE_MUTEX_OMIT
  pGroup = pCache->pGroup;
#endif

  /* Step 3: Abort if createFlag is 1 but the cache is nearly full */
  assert( pCache->nPage >= pCache->nRecyclable );
  nPinned = pCache->nPage - pCache->nRecyclable;
  assert( pGroup->mxPinned == pGroup->nMaxPage + 10 - pGroup->nMinPage );
  assert( pCache->n90pct == pCache->nMax*9/10 );
  if( createFlag==1 && (
        nPinned>=pGroup->mxPinned
     || nPinned>=pCache->n90pct
     || pcache1UnderMemoryPressure(pCache)
  )){
    goto fetch_out;
  }

  if( pCache->nPage>=pCache->nHash && pcache1ResizeHash(pCache) ){
    goto fetch_out;
  }

  /* Step 4. Try to recycle a page. */
  if( pCache->bPurgeable && pGroup->pLruTail && (
         (pCache->nPage+1>=pCache->nMax)
      || pGroup->nCurrentPage>=pGroup->nMaxPage
      || pcache1UnderMemoryPressure(pCache)
  )){
    PCache1 *pOther;
    pPage = pGroup->pLruTail;
    pcache1RemoveFromHash(pPage);
    pcache1PinPage(pPage);
    pOther = pPage->pCache;

    /* We want to verify that szPage and szExtra are the same for pOther
    ** and pCache.  Assert that we can verify this by comparing sums. */
    assert( (pCache->szPage & (pCache->szPage-1))==0 && pCache->szPage>=512 );
    assert( pCache->szExtra<512 );
    assert( (pOther->szPage & (pOther->szPage-1))==0 && pOther->szPage>=512 );
    assert( pOther->szExtra<512 );

    if( pOther->szPage+pOther->szExtra != pCache->szPage+pCache->szExtra ){
      pcache1FreePage(pPage);
      pPage = 0;
    }else{
      pGroup->nCurrentPage -= (pOther->bPurgeable - pCache->bPurgeable);
    }
  }

  /* Step 5. If a usable page buffer has still not been found, 
  ** attempt to allocate a new one. 
  */
  if( !pPage ){
    if( createFlag==1 ) sqlite3BeginBenignMalloc();
    pPage = pcache1AllocPage(pCache);
    if( createFlag==1 ) sqlite3EndBenignMalloc();
  }

  if( pPage ){
    unsigned int h = iKey % pCache->nHash;
    pCache->nPage++;
    pPage->iKey = iKey;
    pPage->pNext = pCache->apHash[h];
    pPage->pCache = pCache;
    pPage->pLruPrev = 0;
    pPage->pLruNext = 0;
    *(void **)pPage->page.pExtra = 0;
    pCache->apHash[h] = pPage;
  }

fetch_out:
  if( pPage && iKey>pCache->iMaxKey ){
    pCache->iMaxKey = iKey;
  }
  pcache1LeaveMutex(pGroup);
  return &pPage->page;
}


/*
** Implementation of the sqlite3_pcache.xUnpin method.
**
** Mark a page as unpinned (eligible for asynchronous recycling).
*/
static void pcache1Unpin(
  sqlite3_pcache *p, 
  sqlite3_pcache_page *pPg, 
  int reuseUnlikely
){
  PCache1 *pCache = (PCache1 *)p;
  PgHdr1 *pPage = (PgHdr1 *)pPg;
  PGroup *pGroup = pCache->pGroup;
 
  assert( pPage->pCache==pCache );
  pcache1EnterMutex(pGroup);

  /* It is an error to call this function if the page is already 
  ** part of the PGroup LRU list.
  */
  assert( pPage->pLruPrev==0 && pPage->pLruNext==0 );
  assert( pGroup->pLruHead!=pPage && pGroup->pLruTail!=pPage );

  if( reuseUnlikely || pGroup->nCurrentPage>pGroup->nMaxPage ){
    pcache1RemoveFromHash(pPage);
    pcache1FreePage(pPage);
  }else{
    /* Add the page to the PGroup LRU list. */
    if( pGroup->pLruHead ){
      pGroup->pLruHead->pLruPrev = pPage;
      pPage->pLruNext = pGroup->pLruHead;
      pGroup->pLruHead = pPage;
    }else{
      pGroup->pLruTail = pPage;
      pGroup->pLruHead = pPage;
    }
    pCache->nRecyclable++;
  }

  pcache1LeaveMutex(pCache->pGroup);
}

/*
** Implementation of the sqlite3_pcache.xRekey method. 
*/
static void pcache1Rekey(
  sqlite3_pcache *p,
  sqlite3_pcache_page *pPg,
  unsigned int iOld,
  unsigned int iNew
){
  PCache1 *pCache = (PCache1 *)p;
  PgHdr1 *pPage = (PgHdr1 *)pPg;
  PgHdr1 **pp;
  unsigned int h; 
  assert( pPage->iKey==iOld );
  assert( pPage->pCache==pCache );

  pcache1EnterMutex(pCache->pGroup);

  h = iOld%pCache->nHash;
  pp = &pCache->apHash[h];
  while( (*pp)!=pPage ){
    pp = &(*pp)->pNext;
  }
  *pp = pPage->pNext;

  h = iNew%pCache->nHash;
  pPage->iKey = iNew;
  pPage->pNext = pCache->apHash[h];
  pCache->apHash[h] = pPage;
  if( iNew>pCache->iMaxKey ){
    pCache->iMaxKey = iNew;
  }

  pcache1LeaveMutex(pCache->pGroup);
}

/*
** Implementation of the sqlite3_pcache.xTruncate method. 
**
** Discard all unpinned pages in the cache with a page number equal to
** or greater than parameter iLimit. Any pinned pages with a page number
** equal to or greater than iLimit are implicitly unpinned.
*/
static void pcache1Truncate(sqlite3_pcache *p, unsigned int iLimit){
  PCache1 *pCache = (PCache1 *)p;
  pcache1EnterMutex(pCache->pGroup);
  if( iLimit<=pCache->iMaxKey ){
    pcache1TruncateUnsafe(pCache, iLimit);
    pCache->iMaxKey = iLimit-1;
  }
  pcache1LeaveMutex(pCache->pGroup);
}

/*
** Implementation of the sqlite3_pcache.xDestroy method. 
**
** Destroy a cache allocated using pcache1Create().
*/
static void pcache1Destroy(sqlite3_pcache *p){
  PCache1 *pCache = (PCache1 *)p;
  PGroup *pGroup = pCache->pGroup;
  assert( pCache->bPurgeable || (pCache->nMax==0 && pCache->nMin==0) );
  pcache1EnterMutex(pGroup);
  pcache1TruncateUnsafe(pCache, 0);
  assert( pGroup->nMaxPage >= pCache->nMax );
  pGroup->nMaxPage -= pCache->nMax;
  assert( pGroup->nMinPage >= pCache->nMin );
  pGroup->nMinPage -= pCache->nMin;
  pGroup->mxPinned = pGroup->nMaxPage + 10 - pGroup->nMinPage;
  pcache1EnforceMaxPage(pGroup);
  pcache1LeaveMutex(pGroup);
  sqlite3_free(pCache->apHash);
  sqlite3_free(pCache);
}

/*
** This function is called during initialization (sqlite3_initialize()) to
** install the default pluggable cache module, assuming the user has not
** already provided an alternative.
*/
void sqlite3PCacheSetDefault(void){
  static const sqlite3_pcache_methods2 defaultMethods = {
    1,                       /* iVersion */
    0,                       /* pArg */
    pcache1Init,             /* xInit */
    pcache1Shutdown,         /* xShutdown */
    pcache1Create,           /* xCreate */
    pcache1Cachesize,        /* xCachesize */
    pcache1Pagecount,        /* xPagecount */
    pcache1Fetch,            /* xFetch */
    pcache1Unpin,            /* xUnpin */
    pcache1Rekey,            /* xRekey */
    pcache1Truncate,         /* xTruncate */
    pcache1Destroy,          /* xDestroy */
    pcache1Shrink            /* xShrink */
  };
  sqlite3_config(SQLITE_CONFIG_PCACHE2, &defaultMethods);
}

#ifdef SQLITE_ENABLE_MEMORY_MANAGEMENT
/*
** This function is called to free superfluous dynamically allocated memory
** held by the pager system. Memory in use by any SQLite pager allocated
** by the current thread may be sqlite3_free()ed.
**
** nReq is the number of bytes of memory required. Once this much has
** been released, the function returns. The return value is the total number 
** of bytes of memory released.
*/
int sqlite3PcacheReleaseMemory(int nReq){
  int nFree = 0;
  assert( sqlite3_mutex_notheld(pcache1.grp.mutex) );
  assert( sqlite3_mutex_notheld(pcache1.mutex) );
  if( pcache1.pStart==0 ){
    PgHdr1 *p;
    pcache1EnterMutex(&pcache1.grp);
    while( (nReq<0 || nFree<nReq) && ((p=pcache1.grp.pLruTail)!=0) ){
      nFree += pcache1MemSize(p->page.pBuf);
#ifdef SQLITE_PCACHE_SEPARATE_HEADER
      nFree += sqlite3MemSize(p);
#endif
      pcache1PinPage(p);
      pcache1RemoveFromHash(p);
      pcache1FreePage(p);
    }
    pcache1LeaveMutex(&pcache1.grp);
  }
  return nFree;
}
#endif /* SQLITE_ENABLE_MEMORY_MANAGEMENT */

#ifdef SQLITE_TEST
/*
** This function is used by test procedures to inspect the internal state
** of the global cache.
*/
void sqlite3PcacheStats(
  int *pnCurrent,      /* OUT: Total number of pages cached */
  int *pnMax,          /* OUT: Global maximum cache size */
  int *pnMin,          /* OUT: Sum of PCache1.nMin for purgeable caches */
  int *pnRecyclable    /* OUT: Total number of pages available for recycling */
){
  PgHdr1 *p;
  int nRecyclable = 0;
  for(p=pcache1.grp.pLruHead; p; p=p->pLruNext){
    nRecyclable++;
  }
  *pnCurrent = pcache1.grp.nCurrentPage;
  *pnMax = (int)pcache1.grp.nMaxPage;
  *pnMin = (int)pcache1.grp.nMinPage;
  *pnRecyclable = nRecyclable;
}
#endif

}
