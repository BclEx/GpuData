// btree.c
#include "Core+Btree.cu.h"
#include "BtreeInt.cu.h"

namespace Core
{
#if _DEBUG
	bool BtreeTrace = true;
#define TRACE(X, ...) if (BtreeTrace) { printf(X, __VA_ARGS__); }
#else
#define TRACE(X, ...)
#endif

#pragma region Struct

	static const char _magicHeader[] = FILE_HEADER;

#define get2byteNotZero(X)  (((((int)get2byte(X))-1)&0xffff)+1)

#define BTALLOC_ANY   0           // Allocate any page
#define BTALLOC_EXACT 1           // Allocate exact page if possible
#define BTALLOC_LE    2           // Allocate any page <= the parameter

#ifndef OMIT_AUTOVACUUM
#define IfNotOmitAV(expr) (expr)
#else
#define IfNotOmitAV(expr) 0
#endif

#ifndef OMIT_SHARED_CACHE
	BtShared *sqlite3SharedCacheList = nullptr;
#endif

#ifndef OMIT_SHARED_CACHE
	bool _sharedCacheEnabled = enable;
	int sqlite3_enable_shared_cache(int enable)
	{
		_sharedCacheEnabled = enable;
		return RC::OK;
	}
#endif

#ifdef OMIT_SHARED_CACHE
#define querySharedCacheTableLock(a,b,c) RC::OK
#define setSharedCacheTableLock(a,b,c) RC::OK
#define clearAllSharedCacheTableLocks(a)
#define downgradeAllSharedCacheTableLocks(a)
#define hasSharedCacheTableLock(a,b,c,d) 1
#define hasReadConflicts(a, b) 0
#endif

#pragma endregion

#pragma region Shared Code1
#ifndef OMIT_SHARED_CACHE

#ifdef _DEBUG

	static bool hasSharedCacheTableLock(Btree *btree, Pid root, bool isIndex, LOCK lockType)
	{
		// If this database is not shareable, or if the client is reading and has the read-uncommitted flag set, then no lock is required. 
		// Return true immediately.
		if (!btree->Sharable || (lockType == LOCK::READ && (btree->Ctx->Flags & Context::FLAG::ReadUncommitted)))
			return true;

		// If the client is reading  or writing an index and the schema is not loaded, then it is too difficult to actually check to see if
		// the correct locks are held.  So do not bother - just return true. This case does not come up very often anyhow.
		Schema *schema = (Schema *)btree->Bt->Schema;
		if (isIndex && (!schema || (schema->Flags & DB_SchemaLoaded) == 0))
			return true;

		// Figure out the root-page that the lock should be held on. For table b-trees, this is just the root page of the b-tree being read or
		// written. For index b-trees, it is the root page of the associated table.
		Pid table = 0;
		if (isIndex)
			for (HashElem *p = sqliteHashFirst(&schema->IdxHash); p; p = sqliteHashNext(p))
			{
				Index *idx = (Index *)sqliteHashData(p);
				if (idx->TID == (int)root)
					table = idx->Table->TID;
			}
		else
			table = root;

		// Search for the required lock. Either a write-lock on root-page iTab, a write-lock on the schema table, or (if the client is reading) a
		// read-lock on iTab will suffice. Return 1 if any of these are found.
		for (BtLock *lock = btree->Bt->Lock; lock; lock = lock->Next)
			if (lock->Btree == btree && 
				(lock->Table == table || (lock->Lock == LOCK::WRITE && lock->Table == 1)) &&
				lock->Lock >= lockType)
				return true;

		// Failed to find the required lock.
		return false;
	}

	static bool hasReadConflicts(Btree *btree, Pid root)
	{
		for (BtCursor *p = btree->Bt->Cursor; p; p = p->Next)
			if (p->IDRoot == root &&
				p->Btree != btree &&
				(p->Btree->Ctx->Flags & Context::FLAG::ReadUncommitted) == 0)
				return true;
		return false;
	}

#endif

	static int querySharedCacheTableLock(Btree *p, Pid table, LOCK lock)
	{
		_assert(sqlite3BtreeHoldsMutex(p));
		_assert(lock == LOCK::READ || lock == LOCK::WRITE);
		_assert(p->DB != nullptr);
		_assert(!(p->DB->Flags & SQLITE_ReadUncommitted) || lock == LOCK::WRITE || table == 1);

		// If requesting a write-lock, then the Btree must have an open write transaction on this file. And, obviously, for this to be so there 
		// must be an open write transaction on the file itself.
		BtShared *bt = p->Bt;
		_assert(lock == LOCK::READ || (p == bt->Writer && p->InTrans == TRANS::WRITE));
		_assert(lock == LOCK::READ || bt->InTransaction == TRANS::WRITE);

		// This routine is a no-op if the shared-cache is not enabled
		if (!p->Sharable)
			return RC::OK;

		// If some other connection is holding an exclusive lock, the requested lock may not be obtained.
		if (bt->Writer != p && (bt->BtsFlags & BTS::EXCLUSIVE) != 0)
		{
			sqlite3ConnectionBlocked(p->DB, bt->Writer->DB);
			return RC::LOCKED_SHAREDCACHE;
		}

		for (BtLock *iter = bt->Lock; iter; iter = iter->Next)
		{
			// The condition (pIter->eLock!=eLock) in the following if(...) statement is a simplification of:
			//
			//   (eLock==WRITE_LOCK || pIter->eLock==WRITE_LOCK)
			//
			// since we know that if eLock==WRITE_LOCK, then no other connection may hold a WRITE_LOCK on any table in this file (since there can
			// only be a single writer).
			_assert(iter->Lock == LOCK::READ || iter->Lock == LOCK::WRITE);
			_assert(lock == LOCK::READ || iter->Btree == p || iter->Lock == LOCK::READ);
			if (iter->Btree != p && iter->Table == table && iter->Lock != lock)
			{
				sqlite3ConnectionBlocked(p->DB, iter->Btree->DB);
				if (lock == LOCK::WRITE)
				{
					_assert(p == bt->Writer);
					bt->BtsFlags |= BTS::PENDING;
				}
				return RC::LOCKED_SHAREDCACHE;
			}
		}
		return RC::OK;
	}

	static int setSharedCacheTableLock(Btree *p, Pid table, LOCK lock)
	{
		_assert(sqlite3BtreeHoldsMutex(p));
		_assert(lock == LOCK::READ || lock == LOCK::WRITE);
		_assert(p->DB != nullptr);

		// A connection with the read-uncommitted flag set will never try to obtain a read-lock using this function. The only read-lock obtained
		// by a connection in read-uncommitted mode is on the sqlite_master table, and that lock is obtained in BtreeBeginTrans().
		_assert((p->DB->Flags & SQLITE_ReadUncommitted) == 0 || lock == LOCK::WRITE);

		// This function should only be called on a sharable b-tree after it has been determined that no other b-tree holds a conflicting lock.
		_assert(p->Sharable);
		_assert(querySharedCacheTableLock(p, table, lock) == RC::OK);

		// First search the list for an existing lock on this table.
		BtShared *bt = p->Bt;
		BtLock *newLock = nullptr;
		for (BtLock *iter = bt->Lock; iter; iter = iter->Next)
			if (iter->Table == table && iter->Btree == p)
			{
				pLock = iter;
				break;
			}

			// If the above search did not find a BtLock struct associating Btree p with table iTable, allocate one and link it into the list.
			if (!newLock)
			{
				newLock = (BtLock *)SysEx::Alloc(sizeof(BtLock), true);
				if (!newLock)
					return RC::NOMEM;
				newLock->Table = table;
				newLock->Btree = p;
				newLock->Next = bt->Lock;
				bt->Lock = pLock;
			}

			// Set the BtLock.eLock variable to the maximum of the current lock and the requested lock. This means if a write-lock was already held
			// and a read-lock requested, we don't incorrectly downgrade the lock.
			_assert(LOCK::WRITE > LOCK::READ);
			if (lock > newLock->Lock)
				newLock->Lock = lock;

			return RC::OK;
	}

	static void clearAllSharedCacheTableLocks(Btree *p){
		BtShared *pBt = p->pBt;
		BtLock **ppIter = &pBt->pLock;

		assert( sqlite3BtreeHoldsMutex(p) );
		assert( p->sharable || 0==*ppIter );
		assert( p->inTrans>0 );

		while( *ppIter ){
			BtLock *pLock = *ppIter;
			assert( (pBt->btsFlags & BTS_EXCLUSIVE)==0 || pBt->pWriter==pLock->pBtree );
			assert( pLock->pBtree->inTrans>=pLock->eLock );
			if( pLock->pBtree==p ){
				*ppIter = pLock->pNext;
				assert( pLock->iTable!=1 || pLock==&p->lock );
				if( pLock->iTable!=1 ){
					sqlite3_free(pLock);
				}
			}else{
				ppIter = &pLock->pNext;
			}
		}

		assert( (pBt->btsFlags & BTS_PENDING)==0 || pBt->pWriter );
		if( pBt->pWriter==p ){
			pBt->pWriter = 0;
			pBt->btsFlags &= ~(BTS_EXCLUSIVE|BTS_PENDING);
		}else if( pBt->nTransaction==2 ){
			/* This function is called when Btree p is concluding its 
			** transaction. If there currently exists a writer, and p is not
			** that writer, then the number of locks held by connections other
			** than the writer must be about to drop to zero. In this case
			** set the BTS_PENDING flag to 0.
			**
			** If there is not currently a writer, then BTS_PENDING must
			** be zero already. So this next line is harmless in that case.
			*/
			pBt->btsFlags &= ~BTS_PENDING;
		}
	}

	static void downgradeAllSharedCacheTableLocks(Btree *p){
		BtShared *pBt = p->pBt;
		if( pBt->pWriter==p ){
			BtLock *pLock;
			pBt->pWriter = 0;
			pBt->btsFlags &= ~(BTS_EXCLUSIVE|BTS_PENDING);
			for(pLock=pBt->pLock; pLock; pLock=pLock->pNext){
				assert( pLock->eLock==READ_LOCK || pLock->pBtree==p );
				pLock->eLock = READ_LOCK;
			}
		}
	}

#endif
#pragma endregion

#pragma region Name1

	static void releasePage(MemPage *page);

#ifdef _DEBUG
	static int cursorHoldsMutex(BtCursor *p)
	{
		return MutexEx::Held(p->pBt->mutex);
	}
#endif

#ifndef OMIT_INCRBLOB
	static void invalidateOverflowCache(BtCursor *pCur){
		assert( cursorHoldsMutex(pCur) );
		sqlite3_free(pCur->aOverflow);
		pCur->aOverflow = 0;
	}

	static void invalidateAllOverflowCache(BtShared *pBt){
		BtCursor *p;
		assert( sqlite3_mutex_held(pBt->mutex) );
		for(p=pBt->pCursor; p; p=p->pNext){
			invalidateOverflowCache(p);
		}
	}

	static void invalidateIncrblobCursors(
		Btree *pBtree,          /* The database file to check */
		i64 iRow,               /* The rowid that might be changing */
		int isClearTable        /* True if all rows are being deleted */
		){
			BtCursor *p;
			BtShared *pBt = pBtree->pBt;
			assert( sqlite3BtreeHoldsMutex(pBtree) );
			for(p=pBt->pCursor; p; p=p->pNext){
				if( p->isIncrblobHandle && (isClearTable || p->info.nKey==iRow) ){
					p->eState = CURSOR_INVALID;
				}
			}
	}
#else
#define invalidateOverflowCache(x)
#define invalidateAllOverflowCache(x)
#define invalidateIncrblobCursors(x,y,z)
#endif

#pragma endregion

#pragma region Name1

	static int btreeSetHasContent(BtShared *pBt, Pgno pgno){
		int rc = SQLITE_OK;
		if( !pBt->pHasContent ){
			assert( pgno<=pBt->nPage );
			pBt->pHasContent = sqlite3BitvecCreate(pBt->nPage);
			if( !pBt->pHasContent ){
				rc = SQLITE_NOMEM;
			}
		}
		if( rc==SQLITE_OK && pgno<=sqlite3BitvecSize(pBt->pHasContent) ){
			rc = sqlite3BitvecSet(pBt->pHasContent, pgno);
		}
		return rc;
	}

	static int btreeGetHasContent(BtShared *pBt, Pgno pgno){
		Bitvec *p = pBt->pHasContent;
		return (p && (pgno>sqlite3BitvecSize(p) || sqlite3BitvecTest(p, pgno)));
	}

	static void btreeClearHasContent(BtShared *pBt){
		sqlite3BitvecDestroy(pBt->pHasContent);
		pBt->pHasContent = 0;
	}

	static void btreeReleaseAllCursorPages(BtCursor *pCur){
		int i;
		for(i=0; i<=pCur->iPage; i++){
			releasePage(pCur->apPage[i]);
			pCur->apPage[i] = 0;
		}
		pCur->iPage = -1;
	}

	static int saveCursorPosition(BtCursor *pCur){
		int rc;

		assert( CURSOR_VALID==pCur->eState );
		assert( 0==pCur->pKey );
		assert( cursorHoldsMutex(pCur) );

		rc = sqlite3BtreeKeySize(pCur, &pCur->nKey);
		assert( rc==SQLITE_OK );  /* KeySize() cannot fail */

		// If this is an intKey table, then the above call to BtreeKeySize() stores the integer key in pCur->nKey. In this case this value is
		// all that is required. Otherwise, if pCur is not open on an intKey table, then malloc space for and store the pCur->nKey bytes of key data.
		if( 0==pCur->apPage[0]->intKey ){
			void *pKey = sqlite3Malloc( (int)pCur->nKey );
			if( pKey ){
				rc = sqlite3BtreeKey(pCur, 0, (int)pCur->nKey, pKey);
				if( rc==SQLITE_OK ){
					pCur->pKey = pKey;
				}else{
					sqlite3_free(pKey);
				}
			}else{
				rc = SQLITE_NOMEM;
			}
		}
		assert( !pCur->apPage[0]->intKey || !pCur->pKey );

		if( rc==SQLITE_OK ){
			btreeReleaseAllCursorPages(pCur);
			pCur->eState = CURSOR_REQUIRESEEK;
		}

		invalidateOverflowCache(pCur);
		return rc;
	}

	static int saveAllCursors(BtShared *pBt, Pgno iRoot, BtCursor *pExcept){
		BtCursor *p;
		assert( sqlite3_mutex_held(pBt->mutex) );
		assert( pExcept==0 || pExcept->pBt==pBt );
		for(p=pBt->pCursor; p; p=p->pNext){
			if( p!=pExcept && (0==iRoot || p->pgnoRoot==iRoot) ){
				if( p->eState==CURSOR_VALID ){
					int rc = saveCursorPosition(p);
					if( SQLITE_OK!=rc ){
						return rc;
					}
				}else{
					testcase( p->iPage>0 );
					btreeReleaseAllCursorPages(p);
				}
			}
		}
		return SQLITE_OK;
	}

	void sqlite3BtreeClearCursor(BtCursor *pCur){
		assert( cursorHoldsMutex(pCur) );
		sqlite3_free(pCur->pKey);
		pCur->pKey = 0;
		pCur->eState = CURSOR_INVALID;
	}

	static int btreeMoveto(
		BtCursor *pCur,     /* Cursor open on the btree to be searched */
		const void *pKey,   /* Packed key if the btree is an index */
		i64 nKey,           /* Integer key for tables.  Size of pKey for indices */
		int bias,           /* Bias search to the high end */
		int *pRes           /* Write search results here */
		){
			int rc;                    /* Status code */
			UnpackedRecord *pIdxKey;   /* Unpacked index key */
			char aSpace[150];          /* Temp space for pIdxKey - to avoid a malloc */
			char *pFree = 0;

			if( pKey ){
				assert( nKey==(i64)(int)nKey );
				pIdxKey = sqlite3VdbeAllocUnpackedRecord(
					pCur->pKeyInfo, aSpace, sizeof(aSpace), &pFree
					);
				if( pIdxKey==0 ) return SQLITE_NOMEM;
				sqlite3VdbeRecordUnpack(pCur->pKeyInfo, (int)nKey, pKey, pIdxKey);
			}else{
				pIdxKey = 0;
			}
			rc = sqlite3BtreeMovetoUnpacked(pCur, pIdxKey, nKey, bias, pRes);
			if( pFree ){
				sqlite3DbFree(pCur->pKeyInfo->db, pFree);
			}
			return rc;
	}

	static int btreeRestoreCursorPosition(BtCursor *pCur){
		int rc;
		assert( cursorHoldsMutex(pCur) );
		assert( pCur->eState>=CURSOR_REQUIRESEEK );
		if( pCur->eState==CURSOR_FAULT ){
			return pCur->skipNext;
		}
		pCur->eState = CURSOR_INVALID;
		rc = btreeMoveto(pCur, pCur->pKey, pCur->nKey, 0, &pCur->skipNext);
		if( rc==SQLITE_OK ){
			sqlite3_free(pCur->pKey);
			pCur->pKey = 0;
			assert( pCur->eState==CURSOR_VALID || pCur->eState==CURSOR_INVALID );
		}
		return rc;
	}

#define restoreCursorPosition(p) \
	(p->eState>=CURSOR_REQUIRESEEK ? \
	btreeRestoreCursorPosition(p) : \
	SQLITE_OK)
	int sqlite3BtreeCursorHasMoved(BtCursor *pCur, int *pHasMoved){
		int rc;

		rc = restoreCursorPosition(pCur);
		if( rc ){
			*pHasMoved = 1;
			return rc;
		}
		if( pCur->eState!=CURSOR_VALID || pCur->skipNext!=0 ){
			*pHasMoved = 1;
		}else{
			*pHasMoved = 0;
		}
		return SQLITE_OK;
	}

#pragma endregion

#pragma region Name1

#ifndef OMIT_AUTOVACUUM
	static Pgno ptrmapPageno(BtShared *pBt, Pgno pgno){
		int nPagesPerMapPage;
		Pgno iPtrMap, ret;
		assert( sqlite3_mutex_held(pBt->mutex) );
		if( pgno<2 ) return 0;
		nPagesPerMapPage = (pBt->usableSize/5)+1;
		iPtrMap = (pgno-2)/nPagesPerMapPage;
		ret = (iPtrMap*nPagesPerMapPage) + 2; 
		if( ret==PENDING_BYTE_PAGE(pBt) ){
			ret++;
		}
		return ret;
	}

	static void ptrmapPut(BtShared *pBt, Pgno key, u8 eType, Pgno parent, int *pRC){
		DbPage *pDbPage;  /* The pointer map page */
		u8 *pPtrmap;      /* The pointer map data */
		Pgno iPtrmap;     /* The pointer map page number */
		int offset;       /* Offset in pointer map page */
		int rc;           /* Return code from subfunctions */

		if( *pRC ) return;

		assert( sqlite3_mutex_held(pBt->mutex) );
		/* The master-journal page number must never be used as a pointer map page */
		assert( 0==PTRMAP_ISPAGE(pBt, PENDING_BYTE_PAGE(pBt)) );

		assert( pBt->autoVacuum );
		if( key==0 ){
			*pRC = SQLITE_CORRUPT_BKPT;
			return;
		}
		iPtrmap = PTRMAP_PAGENO(pBt, key);
		rc = sqlite3PagerGet(pBt->pPager, iPtrmap, &pDbPage);
		if( rc!=SQLITE_OK ){
			*pRC = rc;
			return;
		}
		offset = PTRMAP_PTROFFSET(iPtrmap, key);
		if( offset<0 ){
			*pRC = SQLITE_CORRUPT_BKPT;
			goto ptrmap_exit;
		}
		assert( offset <= (int)pBt->usableSize-5 );
		pPtrmap = (u8 *)sqlite3PagerGetData(pDbPage);

		if( eType!=pPtrmap[offset] || get4byte(&pPtrmap[offset+1])!=parent ){
			TRACE(("PTRMAP_UPDATE: %d->(%d,%d)\n", key, eType, parent));
			*pRC= rc = sqlite3PagerWrite(pDbPage);
			if( rc==SQLITE_OK ){
				pPtrmap[offset] = eType;
				put4byte(&pPtrmap[offset+1], parent);
			}
		}

ptrmap_exit:
		sqlite3PagerUnref(pDbPage);
	}

	static int ptrmapGet(BtShared *pBt, Pgno key, u8 *pEType, Pgno *pPgno){
		DbPage *pDbPage;   /* The pointer map page */
		int iPtrmap;       /* Pointer map page index */
		u8 *pPtrmap;       /* Pointer map page data */
		int offset;        /* Offset of entry in pointer map */
		int rc;

		assert( sqlite3_mutex_held(pBt->mutex) );

		iPtrmap = PTRMAP_PAGENO(pBt, key);
		rc = sqlite3PagerGet(pBt->pPager, iPtrmap, &pDbPage);
		if( rc!=0 ){
			return rc;
		}
		pPtrmap = (u8 *)sqlite3PagerGetData(pDbPage);

		offset = PTRMAP_PTROFFSET(iPtrmap, key);
		if( offset<0 ){
			sqlite3PagerUnref(pDbPage);
			return SQLITE_CORRUPT_BKPT;
		}
		assert( offset <= (int)pBt->usableSize-5 );
		assert( pEType!=0 );
		*pEType = pPtrmap[offset];
		if( pPgno ) *pPgno = get4byte(&pPtrmap[offset+1]);

		sqlite3PagerUnref(pDbPage);
		if( *pEType<1 || *pEType>5 ) return SQLITE_CORRUPT_BKPT;
		return SQLITE_OK;
	}

#else
#define ptrmapPut(w,x,y,z,rc)
#define ptrmapGet(w,x,y,z) SQLITE_OK
#define ptrmapPutOvflPtr(x, y, rc)
#endif

#define findCell(P,I) \
	((P)->aData + ((P)->maskPage & get2byte(&(P)->aCellIdx[2*(I)])))
#define findCellv2(D,M,O,I) (D+(M&get2byte(D+(O+2*(I)))))

	static u8 *findOverflowCell(MemPage *pPage, int iCell){
		int i;
		assert( sqlite3_mutex_held(pPage->pBt->mutex) );
		for(i=pPage->nOverflow-1; i>=0; i--){
			int k;
			k = pPage->aiOvfl[i];
			if( k<=iCell ){
				if( k==iCell ){
					return pPage->apOvfl[i];
				}
				iCell--;
			}
		}
		return findCell(pPage, iCell);
	}

	static void btreeParseCellPtr(
		MemPage *pPage,         /* Page containing the cell */
		u8 *pCell,              /* Pointer to the cell text. */
		CellInfo *pInfo         /* Fill in this structure */
		){
			u16 n;                  /* Number bytes in cell content header */
			u32 nPayload;           /* Number of bytes of cell payload */

			assert( sqlite3_mutex_held(pPage->pBt->mutex) );

			pInfo->pCell = pCell;
			assert( pPage->leaf==0 || pPage->leaf==1 );
			n = pPage->childPtrSize;
			assert( n==4-4*pPage->leaf );
			if( pPage->intKey ){
				if( pPage->hasData ){
					n += getVarint32(&pCell[n], nPayload);
				}else{
					nPayload = 0;
				}
				n += getVarint(&pCell[n], (u64*)&pInfo->nKey);
				pInfo->nData = nPayload;
			}else{
				pInfo->nData = 0;
				n += getVarint32(&pCell[n], nPayload);
				pInfo->nKey = nPayload;
			}
			pInfo->nPayload = nPayload;
			pInfo->nHeader = n;
			testcase( nPayload==pPage->maxLocal );
			testcase( nPayload==pPage->maxLocal+1 );
			if( likely(nPayload<=pPage->maxLocal) ){
				/* This is the (easy) common case where the entire payload fits
				** on the local page.  No overflow is required.
				*/
				if( (pInfo->nSize = (u16)(n+nPayload))<4 ) pInfo->nSize = 4;
				pInfo->nLocal = (u16)nPayload;
				pInfo->iOverflow = 0;
			}else{
				/* If the payload will not fit completely on the local page, we have
				** to decide how much to store locally and how much to spill onto
				** overflow pages.  The strategy is to minimize the amount of unused
				** space on overflow pages while keeping the amount of local storage
				** in between minLocal and maxLocal.
				**
				** Warning:  changing the way overflow payload is distributed in any
				** way will result in an incompatible file format.
				*/
				int minLocal;  /* Minimum amount of payload held locally */
				int maxLocal;  /* Maximum amount of payload held locally */
				int surplus;   /* Overflow payload available for local storage */

				minLocal = pPage->minLocal;
				maxLocal = pPage->maxLocal;
				surplus = minLocal + (nPayload - minLocal)%(pPage->pBt->usableSize - 4);
				testcase( surplus==maxLocal );
				testcase( surplus==maxLocal+1 );
				if( surplus <= maxLocal ){
					pInfo->nLocal = (u16)surplus;
				}else{
					pInfo->nLocal = (u16)minLocal;
				}
				pInfo->iOverflow = (u16)(pInfo->nLocal + n);
				pInfo->nSize = pInfo->iOverflow + 4;
			}
	}
#define parseCell(pPage, iCell, pInfo) \
	btreeParseCellPtr((pPage), findCell((pPage), (iCell)), (pInfo))
	static void btreeParseCell(
		MemPage *pPage,         /* Page containing the cell */
		int iCell,              /* The cell index.  First cell is 0 */
		CellInfo *pInfo         /* Fill in this structure */
		){
			parseCell(pPage, iCell, pInfo);
	}

	static u16 cellSizePtr(MemPage *pPage, u8 *pCell){
		u8 *pIter = &pCell[pPage->childPtrSize];
		u32 nSize;

#ifdef SQLITE_DEBUG
		// The value returned by this function should always be the same as the (CellInfo.nSize) value found by doing a full parse of the
		// cell. If SQLITE_DEBUG is defined, an assert() at the bottom of this function verifies that this invariant is not violated.
		CellInfo debuginfo;
		btreeParseCellPtr(pPage, pCell, &debuginfo);
#endif

		if( pPage->intKey ){
			u8 *pEnd;
			if( pPage->hasData ){
				pIter += getVarint32(pIter, nSize);
			}else{
				nSize = 0;
			}

			// pIter now points at the 64-bit integer key value, a variable length integer. The following block moves pIter to point at the first byte
			// past the end of the key value. */
			pEnd = &pIter[9];
			while( (*pIter++)&0x80 && pIter<pEnd );
		}else{
			pIter += getVarint32(pIter, nSize);
		}

		testcase( nSize==pPage->maxLocal );
		testcase( nSize==pPage->maxLocal+1 );
		if( nSize>pPage->maxLocal ){
			int minLocal = pPage->minLocal;
			nSize = minLocal + (nSize - minLocal) % (pPage->pBt->usableSize - 4);
			testcase( nSize==pPage->maxLocal );
			testcase( nSize==pPage->maxLocal+1 );
			if( nSize>pPage->maxLocal ){
				nSize = minLocal;
			}
			nSize += 4;
		}
		nSize += (u32)(pIter - pCell);

		// The minimum size of any cell is 4 bytes.
		if( nSize<4 ){
			nSize = 4;
		}

		assert( nSize==debuginfo.nSize );
		return (u16)nSize;
	}

#pragma endregion

#pragma region Name1

#ifdef _DEBUG
	static u16 cellSize(MemPage *pPage, int iCell)
	{
		return cellSizePtr(pPage, findCell(pPage, iCell));
	}
#endif

#ifndef OMIT_AUTOVACUUM
	static void ptrmapPutOvflPtr(MemPage *pPage, u8 *pCell, int *pRC){
		CellInfo info;
		if( *pRC ) return;
		assert( pCell!=0 );
		btreeParseCellPtr(pPage, pCell, &info);
		assert( (info.nData+(pPage->intKey?0:info.nKey))==info.nPayload );
		if( info.iOverflow ){
			Pgno ovfl = get4byte(&pCell[info.iOverflow]);
			ptrmapPut(pPage->pBt, ovfl, PTRMAP_OVERFLOW1, pPage->pgno, pRC);
		}
	}
#endif


	static int defragmentPage(MemPage *pPage){
		int i;                     /* Loop counter */
		int pc;                    /* Address of a i-th cell */
		int hdr;                   /* Offset to the page header */
		int size;                  /* Size of a cell */
		int usableSize;            /* Number of usable bytes on a page */
		int cellOffset;            /* Offset to the cell pointer array */
		int cbrk;                  /* Offset to the cell content area */
		int nCell;                 /* Number of cells on the page */
		unsigned char *data;       /* The page data */
		unsigned char *temp;       /* Temp area for cell content */
		int iCellFirst;            /* First allowable cell index */
		int iCellLast;             /* Last possible cell index */


		assert( sqlite3PagerIswriteable(pPage->pDbPage) );
		assert( pPage->pBt!=0 );
		assert( pPage->pBt->usableSize <= SQLITE_MAX_PAGE_SIZE );
		assert( pPage->nOverflow==0 );
		assert( sqlite3_mutex_held(pPage->pBt->mutex) );
		temp = sqlite3PagerTempSpace(pPage->pBt->pPager);
		data = pPage->aData;
		hdr = pPage->hdrOffset;
		cellOffset = pPage->cellOffset;
		nCell = pPage->nCell;
		assert( nCell==get2byte(&data[hdr+3]) );
		usableSize = pPage->pBt->usableSize;
		cbrk = get2byte(&data[hdr+5]);
		memcpy(&temp[cbrk], &data[cbrk], usableSize - cbrk);
		cbrk = usableSize;
		iCellFirst = cellOffset + 2*nCell;
		iCellLast = usableSize - 4;
		for(i=0; i<nCell; i++){
			u8 *pAddr;     // The i-th cell pointer
			pAddr = &data[cellOffset + i*2];
			pc = get2byte(pAddr);
			testcase( pc==iCellFirst );
			testcase( pc==iCellLast );
#if !defined(ENABLE_OVERSIZE_CELL_CHECK)
			// These conditions have already been verified in btreeInitPage() if SQLITE_ENABLE_OVERSIZE_CELL_CHECK is defined 
			if( pc<iCellFirst || pc>iCellLast ){
				return SQLITE_CORRUPT_BKPT;
			}
#endif
			assert( pc>=iCellFirst && pc<=iCellLast );
			size = cellSizePtr(pPage, &temp[pc]);
			cbrk -= size;
#if defined(SQLITE_ENABLE_OVERSIZE_CELL_CHECK)
			if( cbrk<iCellFirst ){
				return SQLITE_CORRUPT_BKPT;
			}
#else
			if( cbrk<iCellFirst || pc+size>usableSize ){
				return SQLITE_CORRUPT_BKPT;
			}
#endif
			assert( cbrk+size<=usableSize && cbrk>=iCellFirst );
			testcase( cbrk+size==usableSize );
			testcase( pc+size==usableSize );
			memcpy(&data[cbrk], &temp[pc], size);
			put2byte(pAddr, cbrk);
		}
		assert( cbrk>=iCellFirst );
		put2byte(&data[hdr+5], cbrk);
		data[hdr+1] = 0;
		data[hdr+2] = 0;
		data[hdr+7] = 0;
		memset(&data[iCellFirst], 0, cbrk-iCellFirst);
		assert( sqlite3PagerIswriteable(pPage->pDbPage) );
		if( cbrk-iCellFirst!=pPage->nFree ){
			return SQLITE_CORRUPT_BKPT;
		}
		return SQLITE_OK;
	}

	static int allocateSpace(MemPage *pPage, int nByte, int *pIdx){
		const int hdr = pPage->hdrOffset;    /* Local cache of pPage->hdrOffset */
		u8 * const data = pPage->aData;      /* Local cache of pPage->aData */
		int nFrag;                           /* Number of fragmented bytes on pPage */
		int top;                             /* First byte of cell content area */
		int gap;        /* First byte of gap between cell pointers and cell content */
		int rc;         /* Integer return code */
		int usableSize; /* Usable size of the page */

		assert( sqlite3PagerIswriteable(pPage->pDbPage) );
		assert( pPage->pBt );
		assert( sqlite3_mutex_held(pPage->pBt->mutex) );
		assert( nByte>=0 );  /* Minimum cell size is 4 */
		assert( pPage->nFree>=nByte );
		assert( pPage->nOverflow==0 );
		usableSize = pPage->pBt->usableSize;
		assert( nByte < usableSize-8 );

		nFrag = data[hdr+7];
		assert( pPage->cellOffset == hdr + 12 - 4*pPage->leaf );
		gap = pPage->cellOffset + 2*pPage->nCell;
		top = get2byteNotZero(&data[hdr+5]);
		if( gap>top ) return SQLITE_CORRUPT_BKPT;
		testcase( gap+2==top );
		testcase( gap+1==top );
		testcase( gap==top );

		if( nFrag>=60 ){
			/* Always defragment highly fragmented pages */
			rc = defragmentPage(pPage);
			if( rc ) return rc;
			top = get2byteNotZero(&data[hdr+5]);
		}else if( gap+2<=top ){
			/* Search the freelist looking for a free slot big enough to satisfy 
			** the request. The allocation is made from the first free slot in 
			** the list that is large enough to accomadate it.
			*/
			int pc, addr;
			for(addr=hdr+1; (pc = get2byte(&data[addr]))>0; addr=pc){
				int size;            /* Size of the free slot */
				if( pc>usableSize-4 || pc<addr+4 ){
					return SQLITE_CORRUPT_BKPT;
				}
				size = get2byte(&data[pc+2]);
				if( size>=nByte ){
					int x = size - nByte;
					testcase( x==4 );
					testcase( x==3 );
					if( x<4 ){
						/* Remove the slot from the free-list. Update the number of
						** fragmented bytes within the page. */
						memcpy(&data[addr], &data[pc], 2);
						data[hdr+7] = (u8)(nFrag + x);
					}else if( size+pc > usableSize ){
						return SQLITE_CORRUPT_BKPT;
					}else{
						/* The slot remains on the free-list. Reduce its size to account
						** for the portion used by the new allocation. */
						put2byte(&data[pc+2], x);
					}
					*pIdx = pc + x;
					return SQLITE_OK;
				}
			}
		}

		// Check to make sure there is enough space in the gap to satisfy the allocation.  If not, defragment.
		testcase( gap+2+nByte==top );
		if( gap+2+nByte>top ){
			rc = defragmentPage(pPage);
			if( rc ) return rc;
			top = get2byteNotZero(&data[hdr+5]);
			assert( gap+nByte<=top );
		}


		// Allocate memory from the gap in between the cell pointer array and the cell content area.  The btreeInitPage() call has already
		// validated the freelist.  Given that the freelist is valid, there is no way that the allocation can extend off the end of the page.
		// The assert() below verifies the previous sentence.
		top -= nByte;
		put2byte(&data[hdr+5], top);
		assert( top+nByte <= (int)pPage->pBt->usableSize );
		*pIdx = top;
		return SQLITE_OK;
	}

	static int freeSpace(MemPage *pPage, int start, int size){
		int addr, pbegin, hdr;
		int iLast;                        /* Largest possible freeblock offset */
		unsigned char *data = pPage->aData;

		assert( pPage->pBt!=0 );
		assert( sqlite3PagerIswriteable(pPage->pDbPage) );
		assert( start>=pPage->hdrOffset+6+pPage->childPtrSize );
		assert( (start + size) <= (int)pPage->pBt->usableSize );
		assert( sqlite3_mutex_held(pPage->pBt->mutex) );
		assert( size>=0 );   /* Minimum cell size is 4 */

		if( pPage->pBt->btsFlags & BTS_SECURE_DELETE ){
			/* Overwrite deleted information with zeros when the secure_delete
			** option is enabled */
			memset(&data[start], 0, size);
		}

		/* Add the space back into the linked list of freeblocks.  Note that
		** even though the freeblock list was checked by btreeInitPage(),
		** btreeInitPage() did not detect overlapping cells or
		** freeblocks that overlapped cells.   Nor does it detect when the
		** cell content area exceeds the value in the page header.  If these
		** situations arise, then subsequent insert operations might corrupt
		** the freelist.  So we do need to check for corruption while scanning
		** the freelist.
		*/
		hdr = pPage->hdrOffset;
		addr = hdr + 1;
		iLast = pPage->pBt->usableSize - 4;
		assert( start<=iLast );
		while( (pbegin = get2byte(&data[addr]))<start && pbegin>0 ){
			if( pbegin<addr+4 ){
				return SQLITE_CORRUPT_BKPT;
			}
			addr = pbegin;
		}
		if( pbegin>iLast ){
			return SQLITE_CORRUPT_BKPT;
		}
		assert( pbegin>addr || pbegin==0 );
		put2byte(&data[addr], start);
		put2byte(&data[start], pbegin);
		put2byte(&data[start+2], size);
		pPage->nFree = pPage->nFree + (u16)size;

		/* Coalesce adjacent free blocks */
		addr = hdr + 1;
		while( (pbegin = get2byte(&data[addr]))>0 ){
			int pnext, psize, x;
			assert( pbegin>addr );
			assert( pbegin <= (int)pPage->pBt->usableSize-4 );
			pnext = get2byte(&data[pbegin]);
			psize = get2byte(&data[pbegin+2]);
			if( pbegin + psize + 3 >= pnext && pnext>0 ){
				int frag = pnext - (pbegin+psize);
				if( (frag<0) || (frag>(int)data[hdr+7]) ){
					return SQLITE_CORRUPT_BKPT;
				}
				data[hdr+7] -= (u8)frag;
				x = get2byte(&data[pnext]);
				put2byte(&data[pbegin], x);
				x = pnext + get2byte(&data[pnext+2]) - pbegin;
				put2byte(&data[pbegin+2], x);
			}else{
				addr = pbegin;
			}
		}

		/* If the cell content area begins with a freeblock, remove it. */
		if( data[hdr+1]==data[hdr+5] && data[hdr+2]==data[hdr+6] ){
			int top;
			pbegin = get2byte(&data[hdr+1]);
			memcpy(&data[hdr+1], &data[pbegin], 2);
			top = get2byte(&data[hdr+5]) + get2byte(&data[pbegin+2]);
			put2byte(&data[hdr+5], top);
		}
		assert( sqlite3PagerIswriteable(pPage->pDbPage) );
		return SQLITE_OK;
	}

	static int decodeFlags(MemPage *pPage, int flagByte){
		BtShared *pBt;     /* A copy of pPage->pBt */

		assert( pPage->hdrOffset==(pPage->pgno==1 ? 100 : 0) );
		assert( sqlite3_mutex_held(pPage->pBt->mutex) );
		pPage->leaf = (u8)(flagByte>>3);  assert( PTF_LEAF == 1<<3 );
		flagByte &= ~PTF_LEAF;
		pPage->childPtrSize = 4-4*pPage->leaf;
		pBt = pPage->pBt;
		if( flagByte==(PTF_LEAFDATA | PTF_INTKEY) ){
			pPage->intKey = 1;
			pPage->hasData = pPage->leaf;
			pPage->maxLocal = pBt->maxLeaf;
			pPage->minLocal = pBt->minLeaf;
		}else if( flagByte==PTF_ZERODATA ){
			pPage->intKey = 0;
			pPage->hasData = 0;
			pPage->maxLocal = pBt->maxLocal;
			pPage->minLocal = pBt->minLocal;
		}else{
			return SQLITE_CORRUPT_BKPT;
		}
		pPage->max1bytePayload = pBt->max1bytePayload;
		return SQLITE_OK;
	}

	static int btreeInitPage(MemPage *pPage){

		assert( pPage->pBt!=0 );
		assert( sqlite3_mutex_held(pPage->pBt->mutex) );
		assert( pPage->pgno==sqlite3PagerPagenumber(pPage->pDbPage) );
		assert( pPage == sqlite3PagerGetExtra(pPage->pDbPage) );
		assert( pPage->aData == sqlite3PagerGetData(pPage->pDbPage) );

		if( !pPage->isInit ){
			u16 pc;            /* Address of a freeblock within pPage->aData[] */
			u8 hdr;            /* Offset to beginning of page header */
			u8 *data;          /* Equal to pPage->aData */
			BtShared *pBt;        /* The main btree structure */
			int usableSize;    /* Amount of usable space on each page */
			u16 cellOffset;    /* Offset from start of page to first cell pointer */
			int nFree;         /* Number of unused bytes on the page */
			int top;           /* First byte of the cell content area */
			int iCellFirst;    /* First allowable cell or freeblock offset */
			int iCellLast;     /* Last possible cell or freeblock offset */

			pBt = pPage->pBt;

			hdr = pPage->hdrOffset;
			data = pPage->aData;
			if( decodeFlags(pPage, data[hdr]) ) return SQLITE_CORRUPT_BKPT;
			assert( pBt->pageSize>=512 && pBt->pageSize<=65536 );
			pPage->maskPage = (u16)(pBt->pageSize - 1);
			pPage->nOverflow = 0;
			usableSize = pBt->usableSize;
			pPage->cellOffset = cellOffset = hdr + 12 - 4*pPage->leaf;
			pPage->aDataEnd = &data[usableSize];
			pPage->aCellIdx = &data[cellOffset];
			top = get2byteNotZero(&data[hdr+5]);
			pPage->nCell = get2byte(&data[hdr+3]);
			if( pPage->nCell>MX_CELL(pBt) ){
				/* To many cells for a single page.  The page must be corrupt */
				return SQLITE_CORRUPT_BKPT;
			}
			testcase( pPage->nCell==MX_CELL(pBt) );

			// A malformed database page might cause us to read past the end of page when parsing a cell.  
			//
			// The following block of code checks early to see if a cell extends past the end of a page boundary and causes SQLITE_CORRUPT to be 
			// returned if it does.
			iCellFirst = cellOffset + 2*pPage->nCell;
			iCellLast = usableSize - 4;
#if defined(ENABLE_OVERSIZE_CELL_CHECK)
			{
				int i;            /* Index into the cell pointer array */
				int sz;           /* Size of a cell */

				if( !pPage->leaf ) iCellLast--;
				for(i=0; i<pPage->nCell; i++){
					pc = get2byte(&data[cellOffset+i*2]);
					testcase( pc==iCellFirst );
					testcase( pc==iCellLast );
					if( pc<iCellFirst || pc>iCellLast ){
						return SQLITE_CORRUPT_BKPT;
					}
					sz = cellSizePtr(pPage, &data[pc]);
					testcase( pc+sz==usableSize );
					if( pc+sz>usableSize ){
						return SQLITE_CORRUPT_BKPT;
					}
				}
				if( !pPage->leaf ) iCellLast++;
			}  
#endif

			/* Compute the total free space on the page */
			pc = get2byte(&data[hdr+1]);
			nFree = data[hdr+7] + top;
			while( pc>0 ){
				u16 next, size;
				if( pc<iCellFirst || pc>iCellLast ){
					/* Start of free block is off the page */
					return SQLITE_CORRUPT_BKPT; 
				}
				next = get2byte(&data[pc]);
				size = get2byte(&data[pc+2]);
				if( (next>0 && next<=pc+size+3) || pc+size>usableSize ){
					/* Free blocks must be in ascending order. And the last byte of
					** the free-block must lie on the database page.  */
					return SQLITE_CORRUPT_BKPT; 
				}
				nFree = nFree + size;
				pc = next;
			}

			// At this point, nFree contains the sum of the offset to the start of the cell-content area plus the number of free bytes within
			// the cell-content area. If this is greater than the usable-size of the page, then the page must be corrupted. This check also
			// serves to verify that the offset to the start of the cell-content area, according to the page header, lies within the page.
			if( nFree>usableSize ){
				return SQLITE_CORRUPT_BKPT; 
			}
			pPage->nFree = (u16)(nFree - iCellFirst);
			pPage->isInit = 1;
		}
		return SQLITE_OK;
	}

	static void zeroPage(MemPage *pPage, int flags){
		unsigned char *data = pPage->aData;
		BtShared *pBt = pPage->pBt;
		u8 hdr = pPage->hdrOffset;
		u16 first;

		assert( sqlite3PagerPagenumber(pPage->pDbPage)==pPage->pgno );
		assert( sqlite3PagerGetExtra(pPage->pDbPage) == (void*)pPage );
		assert( sqlite3PagerGetData(pPage->pDbPage) == data );
		assert( sqlite3PagerIswriteable(pPage->pDbPage) );
		assert( sqlite3_mutex_held(pBt->mutex) );
		if( pBt->btsFlags & BTS_SECURE_DELETE ){
			memset(&data[hdr], 0, pBt->usableSize - hdr);
		}
		data[hdr] = (char)flags;
		first = hdr + 8 + 4*((flags&PTF_LEAF)==0 ?1:0);
		memset(&data[hdr+1], 0, 4);
		data[hdr+7] = 0;
		put2byte(&data[hdr+5], pBt->usableSize);
		pPage->nFree = (u16)(pBt->usableSize - first);
		decodeFlags(pPage, flags);
		pPage->hdrOffset = hdr;
		pPage->cellOffset = first;
		pPage->aDataEnd = &data[pBt->usableSize];
		pPage->aCellIdx = &data[first];
		pPage->nOverflow = 0;
		assert( pBt->pageSize>=512 && pBt->pageSize<=65536 );
		pPage->maskPage = (u16)(pBt->pageSize - 1);
		pPage->nCell = 0;
		pPage->isInit = 1;
	}

#pragma endregion

#pragma region Name1

	static MemPage *btreePageFromDbPage(DbPage *pDbPage, Pgno pgno, BtShared *pBt){
		MemPage *pPage = (MemPage*)sqlite3PagerGetExtra(pDbPage);
		pPage->aData = sqlite3PagerGetData(pDbPage);
		pPage->pDbPage = pDbPage;
		pPage->pBt = pBt;
		pPage->pgno = pgno;
		pPage->hdrOffset = pPage->pgno==1 ? 100 : 0;
		return pPage; 
	}

	static int btreeGetPage(
		BtShared *pBt,       /* The btree */
		Pgno pgno,           /* Number of the page to fetch */
		MemPage **ppPage,    /* Return the page in this parameter */
		int noContent        /* Do not load page content if true */
		){
			int rc;
			DbPage *pDbPage;

			assert( sqlite3_mutex_held(pBt->mutex) );
			rc = sqlite3PagerAcquire(pBt->pPager, pgno, (DbPage**)&pDbPage, noContent);
			if( rc ) return rc;
			*ppPage = btreePageFromDbPage(pDbPage, pgno, pBt);
			return SQLITE_OK;
	}

	static MemPage *btreePageLookup(BtShared *pBt, Pgno pgno){
		DbPage *pDbPage;
		assert( sqlite3_mutex_held(pBt->mutex) );
		pDbPage = sqlite3PagerLookup(pBt->pPager, pgno);
		if( pDbPage ){
			return btreePageFromDbPage(pDbPage, pgno, pBt);
		}
		return 0;
	}

	static Pgno btreePagecount(BtShared *pBt){
		return pBt->nPage;
	}
	u32 sqlite3BtreeLastPage(Btree *p){
		assert( sqlite3BtreeHoldsMutex(p) );
		assert( ((p->pBt->nPage)&0x8000000)==0 );
		return (int)btreePagecount(p->pBt);
	}

	static int getAndInitPage(
		BtShared *pBt,          /* The database file */
		Pgno pgno,           /* Number of the page to get */
		MemPage **ppPage     /* Write the page pointer here */
		){
			int rc;
			assert( sqlite3_mutex_held(pBt->mutex) );

			if( pgno>btreePagecount(pBt) ){
				rc = SQLITE_CORRUPT_BKPT;
			}else{
				rc = btreeGetPage(pBt, pgno, ppPage, 0);
				if( rc==SQLITE_OK ){
					rc = btreeInitPage(*ppPage);
					if( rc!=SQLITE_OK ){
						releasePage(*ppPage);
					}
				}
			}

			testcase( pgno==0 );
			assert( pgno!=0 || rc==SQLITE_CORRUPT );
			return rc;
	}

	static void releasePage(MemPage *pPage){
		if( pPage ){
			assert( pPage->aData );
			assert( pPage->pBt );
			assert( sqlite3PagerGetExtra(pPage->pDbPage) == (void*)pPage );
			assert( sqlite3PagerGetData(pPage->pDbPage)==pPage->aData );
			assert( sqlite3_mutex_held(pPage->pBt->mutex) );
			sqlite3PagerUnref(pPage->pDbPage);
		}
	}

	static void pageReinit(DbPage *pData){
		MemPage *pPage;
		pPage = (MemPage *)sqlite3PagerGetExtra(pData);
		assert( sqlite3PagerPageRefcount(pData)>0 );
		if( pPage->isInit ){
			assert( sqlite3_mutex_held(pPage->pBt->mutex) );
			pPage->isInit = 0;
			if( sqlite3PagerPageRefcount(pData)>1 ){
				/* pPage might not be a btree page;  it might be an overflow page
				** or ptrmap page or a free page.  In those cases, the following
				** call to btreeInitPage() will likely return SQLITE_CORRUPT.
				** But no harm is done by this.  And it is very important that
				** btreeInitPage() be called on every btree page so we make
				** the call for every page that comes in for re-initing. */
				btreeInitPage(pPage);
			}
		}
	}

#pragma endregion

#pragma region Name1

	static int btreeInvokeBusyHandler(void *pArg){
		BtShared *pBt = (BtShared*)pArg;
		assert( pBt->db );
		assert( sqlite3_mutex_held(pBt->db->mutex) );
		return sqlite3InvokeBusyHandler(&pBt->db->busyHandler);
	}

	int sqlite3BtreeOpen(
		sqlite3_vfs *pVfs,      /* VFS to use for this b-tree */
		const char *zFilename,  /* Name of the file containing the BTree database */
		sqlite3 *db,            /* Associated database handle */
		Btree **ppBtree,        /* Pointer to new Btree object written here */
		int flags,              /* Options */
		int vfsFlags            /* Flags passed through to sqlite3_vfs.xOpen() */
		){
			BtShared *pBt = 0;             /* Shared part of btree structure */
			Btree *p;                      /* Handle to return */
			sqlite3_mutex *mutexOpen = 0;  /* Prevents a race condition. Ticket #3537 */
			int rc = SQLITE_OK;            /* Result code from this function */
			u8 nReserve;                   /* Byte of unused space on each page */
			unsigned char zDbHeader[100];  /* Database header content */

			/* True if opening an ephemeral, temporary database */
			const int isTempDb = zFilename==0 || zFilename[0]==0;

			/* Set the variable isMemdb to true for an in-memory database, or 
			** false for a file-based database.
			*/
#ifdef OMIT_MEMORYDB
			const int isMemdb = 0;
#else
			const int isMemdb = (zFilename && strcmp(zFilename, ":memory:")==0)
				|| (isTempDb && sqlite3TempInMemory(db))
				|| (vfsFlags & SQLITE_OPEN_MEMORY)!=0;
#endif

			assert( db!=0 );
			assert( pVfs!=0 );
			assert( sqlite3_mutex_held(db->mutex) );
			assert( (flags&0xff)==flags );   /* flags fit in 8 bits */

			/* Only a BTREE_SINGLE database can be BTREE_UNORDERED */
			assert( (flags & BTREE_UNORDERED)==0 || (flags & BTREE_SINGLE)!=0 );

			/* A BTREE_SINGLE database is always a temporary and/or ephemeral */
			assert( (flags & BTREE_SINGLE)==0 || isTempDb );

			if( isMemdb ){
				flags |= BTREE_MEMORY;
			}
			if( (vfsFlags & SQLITE_OPEN_MAIN_DB)!=0 && (isMemdb || isTempDb) ){
				vfsFlags = (vfsFlags & ~SQLITE_OPEN_MAIN_DB) | SQLITE_OPEN_TEMP_DB;
			}
			p = sqlite3MallocZero(sizeof(Btree));
			if( !p ){
				return SQLITE_NOMEM;
			}
			p->inTrans = TRANS_NONE;
			p->db = db;
#ifndef OMIT_SHARED_CACHE
			p->lock.pBtree = p;
			p->lock.iTable = 1;
#endif

#if !defined(OMIT_SHARED_CACHE) && !defined(OMIT_DISKIO)
			/*
			** If this Btree is a candidate for shared cache, try to find an
			** existing BtShared object that we can share with
			*/
			if( isTempDb==0 && (isMemdb==0 || (vfsFlags&SQLITE_OPEN_URI)!=0) ){
				if( vfsFlags & SQLITE_OPEN_SHAREDCACHE ){
					int nFullPathname = pVfs->mxPathname+1;
					char *zFullPathname = sqlite3Malloc(nFullPathname);
					MUTEX_LOGIC( sqlite3_mutex *mutexShared; )
						p->sharable = 1;
					if( !zFullPathname ){
						sqlite3_free(p);
						return SQLITE_NOMEM;
					}
					if( isMemdb ){
						memcpy(zFullPathname, zFilename, sqlite3Strlen30(zFilename)+1);
					}else{
						rc = sqlite3OsFullPathname(pVfs, zFilename,
							nFullPathname, zFullPathname);
						if( rc ){
							sqlite3_free(zFullPathname);
							sqlite3_free(p);
							return rc;
						}
					}
#if THREADSAFE
					mutexOpen = sqlite3MutexAlloc(SQLITE_MUTEX_STATIC_OPEN);
					sqlite3_mutex_enter(mutexOpen);
					mutexShared = sqlite3MutexAlloc(SQLITE_MUTEX_STATIC_MASTER);
					sqlite3_mutex_enter(mutexShared);
#endif
					for(pBt=GLOBAL(BtShared*,sqlite3SharedCacheList); pBt; pBt=pBt->pNext){
						assert( pBt->nRef>0 );
						if( 0==strcmp(zFullPathname, sqlite3PagerFilename(pBt->pPager, 0))
							&& sqlite3PagerVfs(pBt->pPager)==pVfs ){
								int iDb;
								for(iDb=db->nDb-1; iDb>=0; iDb--){
									Btree *pExisting = db->aDb[iDb].pBt;
									if( pExisting && pExisting->pBt==pBt ){
										sqlite3_mutex_leave(mutexShared);
										sqlite3_mutex_leave(mutexOpen);
										sqlite3_free(zFullPathname);
										sqlite3_free(p);
										return SQLITE_CONSTRAINT;
									}
								}
								p->pBt = pBt;
								pBt->nRef++;
								break;
						}
					}
					sqlite3_mutex_leave(mutexShared);
					sqlite3_free(zFullPathname);
				}
#ifdef _DEBUG
				else{
					/* In debug mode, we mark all persistent databases as sharable
					** even when they are not.  This exercises the locking code and
					** gives more opportunity for asserts(sqlite3_mutex_held())
					** statements to find locking problems.
					*/
					p->sharable = 1;
				}
#endif
			}
#endif
			if( pBt==0 ){
				/*
				** The following asserts make sure that structures used by the btree are
				** the right size.  This is to guard against size changes that result
				** when compiling on a different architecture.
				*/
				assert( sizeof(i64)==8 || sizeof(i64)==4 );
				assert( sizeof(u64)==8 || sizeof(u64)==4 );
				assert( sizeof(u32)==4 );
				assert( sizeof(u16)==2 );
				assert( sizeof(Pgno)==4 );

				pBt = sqlite3MallocZero( sizeof(*pBt) );
				if( pBt==0 ){
					rc = SQLITE_NOMEM;
					goto btree_open_out;
				}
				rc = sqlite3PagerOpen(pVfs, &pBt->pPager, zFilename,
					EXTRA_SIZE, flags, vfsFlags, pageReinit);
				if( rc==SQLITE_OK ){
					rc = sqlite3PagerReadFileheader(pBt->pPager,sizeof(zDbHeader),zDbHeader);
				}
				if( rc!=SQLITE_OK ){
					goto btree_open_out;
				}
				pBt->openFlags = (u8)flags;
				pBt->db = db;
				sqlite3PagerSetBusyhandler(pBt->pPager, btreeInvokeBusyHandler, pBt);
				p->pBt = pBt;

				pBt->pCursor = 0;
				pBt->pPage1 = 0;
				if( sqlite3PagerIsreadonly(pBt->pPager) ) pBt->btsFlags |= BTS_READ_ONLY;
#ifdef SECURE_DELETE
				pBt->btsFlags |= BTS_SECURE_DELETE;
#endif
				pBt->pageSize = (zDbHeader[16]<<8) | (zDbHeader[17]<<16);
				if( pBt->pageSize<512 || pBt->pageSize>SQLITE_MAX_PAGE_SIZE
					|| ((pBt->pageSize-1)&pBt->pageSize)!=0 ){
						pBt->pageSize = 0;
#ifndef OMIT_AUTOVACUUM
						/* If the magic name ":memory:" will create an in-memory database, then
						** leave the autoVacuum mode at 0 (do not auto-vacuum), even if
						** SQLITE_DEFAULT_AUTOVACUUM is true. On the other hand, if
						** SQLITE_OMIT_MEMORYDB has been defined, then ":memory:" is just a
						** regular file-name. In this case the auto-vacuum applies as per normal.
						*/
						if( zFilename && !isMemdb ){
							pBt->autoVacuum = (SQLITE_DEFAULT_AUTOVACUUM ? 1 : 0);
							pBt->incrVacuum = (SQLITE_DEFAULT_AUTOVACUUM==2 ? 1 : 0);
						}
#endif
						nReserve = 0;
				}else{
					nReserve = zDbHeader[20];
					pBt->btsFlags |= BTS_PAGESIZE_FIXED;
#ifndef OMIT_AUTOVACUUM
					pBt->autoVacuum = (get4byte(&zDbHeader[36 + 4*4])?1:0);
					pBt->incrVacuum = (get4byte(&zDbHeader[36 + 7*4])?1:0);
#endif
				}
				rc = sqlite3PagerSetPagesize(pBt->pPager, &pBt->pageSize, nReserve);
				if( rc ) goto btree_open_out;
				pBt->usableSize = pBt->pageSize - nReserve;
				assert( (pBt->pageSize & 7)==0 );  /* 8-byte alignment of pageSize */

#if !defined(OMIT_SHARED_CACHE) && !defined(OMIT_DISKIO)
				/* Add the new BtShared object to the linked list sharable BtShareds.
				*/
				if( p->sharable ){
					MUTEX_LOGIC( sqlite3_mutex *mutexShared; )
						pBt->nRef = 1;
					MUTEX_LOGIC( mutexShared = sqlite3MutexAlloc(SQLITE_MUTEX_STATIC_MASTER);)
						if( SQLITE_THREADSAFE && sqlite3GlobalConfig.bCoreMutex ){
							pBt->mutex = sqlite3MutexAlloc(SQLITE_MUTEX_FAST);
							if( pBt->mutex==0 ){
								rc = SQLITE_NOMEM;
								db->mallocFailed = 0;
								goto btree_open_out;
							}
						}
						sqlite3_mutex_enter(mutexShared);
						pBt->pNext = GLOBAL(BtShared*,sqlite3SharedCacheList);
						GLOBAL(BtShared*,sqlite3SharedCacheList) = pBt;
						sqlite3_mutex_leave(mutexShared);
				}
#endif
			}

#if !defined(OMIT_SHARED_CACHE) && !defined(OMIT_DISKIO)
			/* If the new Btree uses a sharable pBtShared, then link the new
			** Btree into the list of all sharable Btrees for the same connection.
			** The list is kept in ascending order by pBt address.
			*/
			if( p->sharable ){
				int i;
				Btree *pSib;
				for(i=0; i<db->nDb; i++){
					if( (pSib = db->aDb[i].pBt)!=0 && pSib->sharable ){
						while( pSib->pPrev ){ pSib = pSib->pPrev; }
						if( p->pBt<pSib->pBt ){
							p->pNext = pSib;
							p->pPrev = 0;
							pSib->pPrev = p;
						}else{
							while( pSib->pNext && pSib->pNext->pBt<p->pBt ){
								pSib = pSib->pNext;
							}
							p->pNext = pSib->pNext;
							p->pPrev = pSib;
							if( p->pNext ){
								p->pNext->pPrev = p;
							}
							pSib->pNext = p;
						}
						break;
					}
				}
			}
#endif
			*ppBtree = p;

btree_open_out:
			if( rc!=SQLITE_OK ){
				if( pBt && pBt->pPager ){
					sqlite3PagerClose(pBt->pPager);
				}
				sqlite3_free(pBt);
				sqlite3_free(p);
				*ppBtree = 0;
			}else{
				/* If the B-Tree was successfully opened, set the pager-cache size to the
				** default value. Except, when opening on an existing shared pager-cache,
				** do not change the pager-cache size.
				*/
				if( sqlite3BtreeSchema(p, 0, 0)==0 ){
					sqlite3PagerSetCachesize(p->pBt->pPager, SQLITE_DEFAULT_CACHE_SIZE);
				}
			}
			if( mutexOpen ){
				assert( sqlite3_mutex_held(mutexOpen) );
				sqlite3_mutex_leave(mutexOpen);
			}
			return rc;
	}

	static int removeFromSharingList(BtShared *pBt){
#ifndef SQLITE_OMIT_SHARED_CACHE
		MUTEX_LOGIC( sqlite3_mutex *pMaster; )
			BtShared *pList;
		int removed = 0;

		assert( sqlite3_mutex_notheld(pBt->mutex) );
		MUTEX_LOGIC( pMaster = sqlite3MutexAlloc(SQLITE_MUTEX_STATIC_MASTER); )
			sqlite3_mutex_enter(pMaster);
		pBt->nRef--;
		if( pBt->nRef<=0 ){
			if( GLOBAL(BtShared*,sqlite3SharedCacheList)==pBt ){
				GLOBAL(BtShared*,sqlite3SharedCacheList) = pBt->pNext;
			}else{
				pList = GLOBAL(BtShared*,sqlite3SharedCacheList);
				while( ALWAYS(pList) && pList->pNext!=pBt ){
					pList=pList->pNext;
				}
				if( ALWAYS(pList) ){
					pList->pNext = pBt->pNext;
				}
			}
			if( SQLITE_THREADSAFE ){
				sqlite3_mutex_free(pBt->mutex);
			}
			removed = 1;
		}
		sqlite3_mutex_leave(pMaster);
		return removed;
#else
		return 1;
#endif
	}

	static void allocateTempSpace(BtShared *pBt){
		if( !pBt->pTmpSpace ){
			pBt->pTmpSpace = sqlite3PageMalloc( pBt->pageSize );
		}
	}

	static void freeTempSpace(BtShared *pBt){
		sqlite3PageFree( pBt->pTmpSpace);
		pBt->pTmpSpace = 0;
	}

	int sqlite3BtreeClose(Btree *p){
		BtShared *pBt = p->pBt;
		BtCursor *pCur;

		/* Close all cursors opened via this handle.  */
		assert( sqlite3_mutex_held(p->db->mutex) );
		sqlite3BtreeEnter(p);
		pCur = pBt->pCursor;
		while( pCur ){
			BtCursor *pTmp = pCur;
			pCur = pCur->pNext;
			if( pTmp->pBtree==p ){
				sqlite3BtreeCloseCursor(pTmp);
			}
		}

		/* Rollback any active transaction and free the handle structure.
		** The call to sqlite3BtreeRollback() drops any table-locks held by
		** this handle.
		*/
		sqlite3BtreeRollback(p, SQLITE_OK);
		sqlite3BtreeLeave(p);

		/* If there are still other outstanding references to the shared-btree
		** structure, return now. The remainder of this procedure cleans 
		** up the shared-btree.
		*/
		assert( p->wantToLock==0 && p->locked==0 );
		if( !p->sharable || removeFromSharingList(pBt) ){
			/* The pBt is no longer on the sharing list, so we can access
			** it without having to hold the mutex.
			**
			** Clean out and delete the BtShared object.
			*/
			assert( !pBt->pCursor );
			sqlite3PagerClose(pBt->pPager);
			if( pBt->xFreeSchema && pBt->pSchema ){
				pBt->xFreeSchema(pBt->pSchema);
			}
			sqlite3DbFree(0, pBt->pSchema);
			freeTempSpace(pBt);
			sqlite3_free(pBt);
		}

#ifndef OMIT_SHARED_CACHE
		assert( p->wantToLock==0 );
		assert( p->locked==0 );
		if( p->pPrev ) p->pPrev->pNext = p->pNext;
		if( p->pNext ) p->pNext->pPrev = p->pPrev;
#endif

		sqlite3_free(p);
		return SQLITE_OK;
	}

#pragma endregion

#pragma region Name1

	int sqlite3BtreeSetCacheSize(Btree *p, int mxPage){
		BtShared *pBt = p->pBt;
		assert( sqlite3_mutex_held(p->db->mutex) );
		sqlite3BtreeEnter(p);
		sqlite3PagerSetCachesize(pBt->pPager, mxPage);
		sqlite3BtreeLeave(p);
		return SQLITE_OK;
	}

#ifndef SQLITE_OMIT_PAGER_PRAGMAS
	int sqlite3BtreeSetSafetyLevel(
		Btree *p,              /* The btree to set the safety level on */
		int level,             /* PRAGMA synchronous.  1=OFF, 2=NORMAL, 3=FULL */
		int fullSync,          /* PRAGMA fullfsync. */
		int ckptFullSync       /* PRAGMA checkpoint_fullfync */
		){
			BtShared *pBt = p->pBt;
			assert( sqlite3_mutex_held(p->db->mutex) );
			assert( level>=1 && level<=3 );
			sqlite3BtreeEnter(p);
			sqlite3PagerSetSafetyLevel(pBt->pPager, level, fullSync, ckptFullSync);
			sqlite3BtreeLeave(p);
			return SQLITE_OK;
	}
#endif

	int sqlite3BtreeSyncDisabled(Btree *p){
		BtShared *pBt = p->pBt;
		int rc;
		assert( sqlite3_mutex_held(p->db->mutex) );  
		sqlite3BtreeEnter(p);
		assert( pBt && pBt->pPager );
		rc = sqlite3PagerNosync(pBt->pPager);
		sqlite3BtreeLeave(p);
		return rc;
	}

	int sqlite3BtreeSetPageSize(Btree *p, int pageSize, int nReserve, int iFix){
		int rc = SQLITE_OK;
		BtShared *pBt = p->pBt;
		assert( nReserve>=-1 && nReserve<=255 );
		sqlite3BtreeEnter(p);
		if( pBt->btsFlags & BTS_PAGESIZE_FIXED ){
			sqlite3BtreeLeave(p);
			return SQLITE_READONLY;
		}
		if( nReserve<0 ){
			nReserve = pBt->pageSize - pBt->usableSize;
		}
		assert( nReserve>=0 && nReserve<=255 );
		if( pageSize>=512 && pageSize<=SQLITE_MAX_PAGE_SIZE &&
			((pageSize-1)&pageSize)==0 ){
				assert( (pageSize & 7)==0 );
				assert( !pBt->pPage1 && !pBt->pCursor );
				pBt->pageSize = (u32)pageSize;
				freeTempSpace(pBt);
		}
		rc = sqlite3PagerSetPagesize(pBt->pPager, &pBt->pageSize, nReserve);
		pBt->usableSize = pBt->pageSize - (u16)nReserve;
		if( iFix ) pBt->btsFlags |= BTS_PAGESIZE_FIXED;
		sqlite3BtreeLeave(p);
		return rc;
	}

	int sqlite3BtreeGetPageSize(Btree *p){
		return p->pBt->pageSize;
	}

#if defined(HAS_CODEC) || defined(_DEBUG)
	int sqlite3BtreeGetReserveNoMutex(Btree *p){
		assert( sqlite3_mutex_held(p->pBt->mutex) );
		return p->pBt->pageSize - p->pBt->usableSize;
	}
#endif

#if !defined(SQLITE_OMIT_PAGER_PRAGMAS) || !defined(SQLITE_OMIT_VACUUM)
	int sqlite3BtreeGetReserve(Btree *p){
		int n;
		sqlite3BtreeEnter(p);
		n = p->pBt->pageSize - p->pBt->usableSize;
		sqlite3BtreeLeave(p);
		return n;
	}

	int sqlite3BtreeMaxPageCount(Btree *p, int mxPage){
		int n;
		sqlite3BtreeEnter(p);
		n = sqlite3PagerMaxPageCount(p->pBt->pPager, mxPage);
		sqlite3BtreeLeave(p);
		return n;
	}

	int sqlite3BtreeSecureDelete(Btree *p, int newFlag){
		int b;
		if( p==0 ) return 0;
		sqlite3BtreeEnter(p);
		if( newFlag>=0 ){
			p->pBt->btsFlags &= ~BTS_SECURE_DELETE;
			if( newFlag ) p->pBt->btsFlags |= BTS_SECURE_DELETE;
		} 
		b = (p->pBt->btsFlags & BTS_SECURE_DELETE)!=0;
		sqlite3BtreeLeave(p);
		return b;
	}
#endif

	int sqlite3BtreeSetAutoVacuum(Btree *p, int autoVacuum){
#ifdef SQLITE_OMIT_AUTOVACUUM
		return SQLITE_READONLY;
#else
		BtShared *pBt = p->pBt;
		int rc = SQLITE_OK;
		u8 av = (u8)autoVacuum;

		sqlite3BtreeEnter(p);
		if( (pBt->btsFlags & BTS_PAGESIZE_FIXED)!=0 && (av ?1:0)!=pBt->autoVacuum ){
			rc = SQLITE_READONLY;
		}else{
			pBt->autoVacuum = av ?1:0;
			pBt->incrVacuum = av==2 ?1:0;
		}
		sqlite3BtreeLeave(p);
		return rc;
#endif
	}

	int sqlite3BtreeGetAutoVacuum(Btree *p){
#ifdef SQLITE_OMIT_AUTOVACUUM
		return BTREE_AUTOVACUUM_NONE;
#else
		int rc;
		sqlite3BtreeEnter(p);
		rc = (
			(!p->pBt->autoVacuum)?BTREE_AUTOVACUUM_NONE:
			(!p->pBt->incrVacuum)?BTREE_AUTOVACUUM_FULL:
			BTREE_AUTOVACUUM_INCR
			);
		sqlite3BtreeLeave(p);
		return rc;
#endif
	}

#pragma endregion

#pragma region Name1

	static int lockBtree(BtShared *pBt){
		int rc;              /* Result code from subfunctions */
		MemPage *pPage1;     /* Page 1 of the database file */
		int nPage;           /* Number of pages in the database */
		int nPageFile = 0;   /* Number of pages in the database file */
		int nPageHeader;     /* Number of pages in the database according to hdr */

		assert( sqlite3_mutex_held(pBt->mutex) );
		assert( pBt->pPage1==0 );
		rc = sqlite3PagerSharedLock(pBt->pPager);
		if( rc!=SQLITE_OK ) return rc;
		rc = btreeGetPage(pBt, 1, &pPage1, 0);
		if( rc!=SQLITE_OK ) return rc;

		/* Do some checking to help insure the file we opened really is
		** a valid database file. 
		*/
		nPage = nPageHeader = get4byte(28+(u8*)pPage1->aData);
		sqlite3PagerPagecount(pBt->pPager, &nPageFile);
		if( nPage==0 || memcmp(24+(u8*)pPage1->aData, 92+(u8*)pPage1->aData,4)!=0 ){
			nPage = nPageFile;
		}
		if( nPage>0 ){
			u32 pageSize;
			u32 usableSize;
			u8 *page1 = pPage1->aData;
			rc = SQLITE_NOTADB;
			if( memcmp(page1, zMagicHeader, 16)!=0 ){
				goto page1_init_failed;
			}

#ifdef SQLITE_OMIT_WAL
			if( page1[18]>1 ){
				pBt->btsFlags |= BTS_READ_ONLY;
			}
			if( page1[19]>1 ){
				goto page1_init_failed;
			}
#else
			if( page1[18]>2 ){
				pBt->btsFlags |= BTS_READ_ONLY;
			}
			if( page1[19]>2 ){
				goto page1_init_failed;
			}

			/* If the write version is set to 2, this database should be accessed
			** in WAL mode. If the log is not already open, open it now. Then 
			** return SQLITE_OK and return without populating BtShared.pPage1.
			** The caller detects this and calls this function again. This is
			** required as the version of page 1 currently in the page1 buffer
			** may not be the latest version - there may be a newer one in the log
			** file.
			*/
			if( page1[19]==2 && (pBt->btsFlags & BTS_NO_WAL)==0 ){
				int isOpen = 0;
				rc = sqlite3PagerOpenWal(pBt->pPager, &isOpen);
				if( rc!=SQLITE_OK ){
					goto page1_init_failed;
				}else if( isOpen==0 ){
					releasePage(pPage1);
					return SQLITE_OK;
				}
				rc = SQLITE_NOTADB;
			}
#endif

			/* The maximum embedded fraction must be exactly 25%.  And the minimum
			** embedded fraction must be 12.5% for both leaf-data and non-leaf-data.
			** The original design allowed these amounts to vary, but as of
			** version 3.6.0, we require them to be fixed.
			*/
			if( memcmp(&page1[21], "\100\040\040",3)!=0 ){
				goto page1_init_failed;
			}
			pageSize = (page1[16]<<8) | (page1[17]<<16);
			if( ((pageSize-1)&pageSize)!=0
				|| pageSize>SQLITE_MAX_PAGE_SIZE 
				|| pageSize<=256 
				){
					goto page1_init_failed;
			}
			assert( (pageSize & 7)==0 );
			usableSize = pageSize - page1[20];
			if( (u32)pageSize!=pBt->pageSize ){
				/* After reading the first page of the database assuming a page size
				** of BtShared.pageSize, we have discovered that the page-size is
				** actually pageSize. Unlock the database, leave pBt->pPage1 at
				** zero and return SQLITE_OK. The caller will call this function
				** again with the correct page-size.
				*/
				releasePage(pPage1);
				pBt->usableSize = usableSize;
				pBt->pageSize = pageSize;
				freeTempSpace(pBt);
				rc = sqlite3PagerSetPagesize(pBt->pPager, &pBt->pageSize,
					pageSize-usableSize);
				return rc;
			}
			if( (pBt->db->flags & SQLITE_RecoveryMode)==0 && nPage>nPageFile ){
				rc = SQLITE_CORRUPT_BKPT;
				goto page1_init_failed;
			}
			if( usableSize<480 ){
				goto page1_init_failed;
			}
			pBt->pageSize = pageSize;
			pBt->usableSize = usableSize;
#ifndef SQLITE_OMIT_AUTOVACUUM
			pBt->autoVacuum = (get4byte(&page1[36 + 4*4])?1:0);
			pBt->incrVacuum = (get4byte(&page1[36 + 7*4])?1:0);
#endif
		}

		/* maxLocal is the maximum amount of payload to store locally for
		** a cell.  Make sure it is small enough so that at least minFanout
		** cells can will fit on one page.  We assume a 10-byte page header.
		** Besides the payload, the cell must store:
		**     2-byte pointer to the cell
		**     4-byte child pointer
		**     9-byte nKey value
		**     4-byte nData value
		**     4-byte overflow page pointer
		** So a cell consists of a 2-byte pointer, a header which is as much as
		** 17 bytes long, 0 to N bytes of payload, and an optional 4 byte overflow
		** page pointer.
		*/
		pBt->maxLocal = (u16)((pBt->usableSize-12)*64/255 - 23);
		pBt->minLocal = (u16)((pBt->usableSize-12)*32/255 - 23);
		pBt->maxLeaf = (u16)(pBt->usableSize - 35);
		pBt->minLeaf = (u16)((pBt->usableSize-12)*32/255 - 23);
		if( pBt->maxLocal>127 ){
			pBt->max1bytePayload = 127;
		}else{
			pBt->max1bytePayload = (u8)pBt->maxLocal;
		}
		assert( pBt->maxLeaf + 23 <= MX_CELL_SIZE(pBt) );
		pBt->pPage1 = pPage1;
		pBt->nPage = nPage;
		return SQLITE_OK;

page1_init_failed:
		releasePage(pPage1);
		pBt->pPage1 = 0;
		return rc;
	}

	static void unlockBtreeIfUnused(BtShared *pBt){
		assert( sqlite3_mutex_held(pBt->mutex) );
		assert( pBt->pCursor==0 || pBt->inTransaction>TRANS_NONE );
		if( pBt->inTransaction==TRANS_NONE && pBt->pPage1!=0 ){
			assert( pBt->pPage1->aData );
			assert( sqlite3PagerRefcount(pBt->pPager)==1 );
			assert( pBt->pPage1->aData );
			releasePage(pBt->pPage1);
			pBt->pPage1 = 0;
		}
	}

#pragma endregion

#pragma region Name1

	static int newDatabase(BtShared *pBt){
		MemPage *pP1;
		unsigned char *data;
		int rc;

		assert( sqlite3_mutex_held(pBt->mutex) );
		if( pBt->nPage>0 ){
			return SQLITE_OK;
		}
		pP1 = pBt->pPage1;
		assert( pP1!=0 );
		data = pP1->aData;
		rc = sqlite3PagerWrite(pP1->pDbPage);
		if( rc ) return rc;
		memcpy(data, zMagicHeader, sizeof(zMagicHeader));
		assert( sizeof(zMagicHeader)==16 );
		data[16] = (u8)((pBt->pageSize>>8)&0xff);
		data[17] = (u8)((pBt->pageSize>>16)&0xff);
		data[18] = 1;
		data[19] = 1;
		assert( pBt->usableSize<=pBt->pageSize && pBt->usableSize+255>=pBt->pageSize);
		data[20] = (u8)(pBt->pageSize - pBt->usableSize);
		data[21] = 64;
		data[22] = 32;
		data[23] = 32;
		memset(&data[24], 0, 100-24);
		zeroPage(pP1, PTF_INTKEY|PTF_LEAF|PTF_LEAFDATA );
		pBt->btsFlags |= BTS_PAGESIZE_FIXED;
#ifndef SQLITE_OMIT_AUTOVACUUM
		assert( pBt->autoVacuum==1 || pBt->autoVacuum==0 );
		assert( pBt->incrVacuum==1 || pBt->incrVacuum==0 );
		put4byte(&data[36 + 4*4], pBt->autoVacuum);
		put4byte(&data[36 + 7*4], pBt->incrVacuum);
#endif
		pBt->nPage = 1;
		data[31] = 1;
		return SQLITE_OK;
	}

	int sqlite3BtreeNewDb(Btree *p){
		int rc;
		sqlite3BtreeEnter(p);
		p->pBt->nPage = 0;
		rc = newDatabase(p->pBt);
		sqlite3BtreeLeave(p);
		return rc;
	}

#pragma endregion

#pragma region Name1

	int sqlite3BtreeBeginTrans(Btree *p, int wrflag){
		sqlite3 *pBlock = 0;
		BtShared *pBt = p->pBt;
		int rc = SQLITE_OK;

		sqlite3BtreeEnter(p);
		btreeIntegrity(p);

		/* If the btree is already in a write-transaction, or it
		** is already in a read-transaction and a read-transaction
		** is requested, this is a no-op.
		*/
		if( p->inTrans==TRANS_WRITE || (p->inTrans==TRANS_READ && !wrflag) ){
			goto trans_begun;
		}
		assert( IfNotOmitAV(pBt->bDoTruncate)==0 );

		/* Write transactions are not possible on a read-only database */
		if( (pBt->btsFlags & BTS_READ_ONLY)!=0 && wrflag ){
			rc = SQLITE_READONLY;
			goto trans_begun;
		}

#ifndef SQLITE_OMIT_SHARED_CACHE
		/* If another database handle has already opened a write transaction 
		** on this shared-btree structure and a second write transaction is
		** requested, return SQLITE_LOCKED.
		*/
		if( (wrflag && pBt->inTransaction==TRANS_WRITE)
			|| (pBt->btsFlags & BTS_PENDING)!=0
			){
				pBlock = pBt->pWriter->db;
		}else if( wrflag>1 ){
			BtLock *pIter;
			for(pIter=pBt->pLock; pIter; pIter=pIter->pNext){
				if( pIter->pBtree!=p ){
					pBlock = pIter->pBtree->db;
					break;
				}
			}
		}
		if( pBlock ){
			sqlite3ConnectionBlocked(p->db, pBlock);
			rc = SQLITE_LOCKED_SHAREDCACHE;
			goto trans_begun;
		}
#endif

		/* Any read-only or read-write transaction implies a read-lock on 
		** page 1. So if some other shared-cache client already has a write-lock 
		** on page 1, the transaction cannot be opened. */
		rc = querySharedCacheTableLock(p, MASTER_ROOT, READ_LOCK);
		if( SQLITE_OK!=rc ) goto trans_begun;

		pBt->btsFlags &= ~BTS_INITIALLY_EMPTY;
		if( pBt->nPage==0 ) pBt->btsFlags |= BTS_INITIALLY_EMPTY;
		do {
			/* Call lockBtree() until either pBt->pPage1 is populated or
			** lockBtree() returns something other than SQLITE_OK. lockBtree()
			** may return SQLITE_OK but leave pBt->pPage1 set to 0 if after
			** reading page 1 it discovers that the page-size of the database 
			** file is not pBt->pageSize. In this case lockBtree() will update
			** pBt->pageSize to the page-size of the file on disk.
			*/
			while( pBt->pPage1==0 && SQLITE_OK==(rc = lockBtree(pBt)) );

			if( rc==SQLITE_OK && wrflag ){
				if( (pBt->btsFlags & BTS_READ_ONLY)!=0 ){
					rc = SQLITE_READONLY;
				}else{
					rc = sqlite3PagerBegin(pBt->pPager,wrflag>1,sqlite3TempInMemory(p->db));
					if( rc==SQLITE_OK ){
						rc = newDatabase(pBt);
					}
				}
			}

			if( rc!=SQLITE_OK ){
				unlockBtreeIfUnused(pBt);
			}
		}while( (rc&0xFF)==SQLITE_BUSY && pBt->inTransaction==TRANS_NONE &&
			btreeInvokeBusyHandler(pBt) );

		if( rc==SQLITE_OK ){
			if( p->inTrans==TRANS_NONE ){
				pBt->nTransaction++;
#ifndef SQLITE_OMIT_SHARED_CACHE
				if( p->sharable ){
					assert( p->lock.pBtree==p && p->lock.iTable==1 );
					p->lock.eLock = READ_LOCK;
					p->lock.pNext = pBt->pLock;
					pBt->pLock = &p->lock;
				}
#endif
			}
			p->inTrans = (wrflag?TRANS_WRITE:TRANS_READ);
			if( p->inTrans>pBt->inTransaction ){
				pBt->inTransaction = p->inTrans;
			}
			if( wrflag ){
				MemPage *pPage1 = pBt->pPage1;
#ifndef SQLITE_OMIT_SHARED_CACHE
				assert( !pBt->pWriter );
				pBt->pWriter = p;
				pBt->btsFlags &= ~BTS_EXCLUSIVE;
				if( wrflag>1 ) pBt->btsFlags |= BTS_EXCLUSIVE;
#endif

				/* If the db-size header field is incorrect (as it may be if an old
				** client has been writing the database file), update it now. Doing
				** this sooner rather than later means the database size can safely 
				** re-read the database size from page 1 if a savepoint or transaction
				** rollback occurs within the transaction.
				*/
				if( pBt->nPage!=get4byte(&pPage1->aData[28]) ){
					rc = sqlite3PagerWrite(pPage1->pDbPage);
					if( rc==SQLITE_OK ){
						put4byte(&pPage1->aData[28], pBt->nPage);
					}
				}
			}
		}


trans_begun:
		if( rc==SQLITE_OK && wrflag ){
			/* This call makes sure that the pager has the correct number of
			** open savepoints. If the second parameter is greater than 0 and
			** the sub-journal is not already open, then it will be opened here.
			*/
			rc = sqlite3PagerOpenSavepoint(pBt->pPager, p->db->nSavepoint);
		}

		btreeIntegrity(p);
		sqlite3BtreeLeave(p);
		return rc;
	}

#pragma endregion

#pragma region OMIT_AUTOVACUUM
#ifndef OMIT_AUTOVACUUM

	static int setChildPtrmaps(MemPage *pPage){
		int i;                             /* Counter variable */
		int nCell;                         /* Number of cells in page pPage */
		int rc;                            /* Return code */
		BtShared *pBt = pPage->pBt;
		u8 isInitOrig = pPage->isInit;
		Pgno pgno = pPage->pgno;

		assert( sqlite3_mutex_held(pPage->pBt->mutex) );
		rc = btreeInitPage(pPage);
		if( rc!=SQLITE_OK ){
			goto set_child_ptrmaps_out;
		}
		nCell = pPage->nCell;

		for(i=0; i<nCell; i++){
			u8 *pCell = findCell(pPage, i);

			ptrmapPutOvflPtr(pPage, pCell, &rc);

			if( !pPage->leaf ){
				Pgno childPgno = get4byte(pCell);
				ptrmapPut(pBt, childPgno, PTRMAP_BTREE, pgno, &rc);
			}
		}

		if( !pPage->leaf ){
			Pgno childPgno = get4byte(&pPage->aData[pPage->hdrOffset+8]);
			ptrmapPut(pBt, childPgno, PTRMAP_BTREE, pgno, &rc);
		}

set_child_ptrmaps_out:
		pPage->isInit = isInitOrig;
		return rc;
	}

	static int modifyPagePointer(MemPage *pPage, Pgno iFrom, Pgno iTo, u8 eType){
		assert( sqlite3_mutex_held(pPage->pBt->mutex) );
		assert( sqlite3PagerIswriteable(pPage->pDbPage) );
		if( eType==PTRMAP_OVERFLOW2 ){
			/* The pointer is always the first 4 bytes of the page in this case.  */
			if( get4byte(pPage->aData)!=iFrom ){
				return SQLITE_CORRUPT_BKPT;
			}
			put4byte(pPage->aData, iTo);
		}else{
			u8 isInitOrig = pPage->isInit;
			int i;
			int nCell;

			btreeInitPage(pPage);
			nCell = pPage->nCell;

			for(i=0; i<nCell; i++){
				u8 *pCell = findCell(pPage, i);
				if( eType==PTRMAP_OVERFLOW1 ){
					CellInfo info;
					btreeParseCellPtr(pPage, pCell, &info);
					if( info.iOverflow
						&& pCell+info.iOverflow+3<=pPage->aData+pPage->maskPage
						&& iFrom==get4byte(&pCell[info.iOverflow])
						){
							put4byte(&pCell[info.iOverflow], iTo);
							break;
					}
				}else{
					if( get4byte(pCell)==iFrom ){
						put4byte(pCell, iTo);
						break;
					}
				}
			}

			if( i==nCell ){
				if( eType!=PTRMAP_BTREE || 
					get4byte(&pPage->aData[pPage->hdrOffset+8])!=iFrom ){
						return SQLITE_CORRUPT_BKPT;
				}
				put4byte(&pPage->aData[pPage->hdrOffset+8], iTo);
			}

			pPage->isInit = isInitOrig;
		}
		return SQLITE_OK;
	}

	static int relocatePage(
		BtShared *pBt,           /* Btree */
		MemPage *pDbPage,        /* Open page to move */
		u8 eType,                /* Pointer map 'type' entry for pDbPage */
		Pgno iPtrPage,           /* Pointer map 'page-no' entry for pDbPage */
		Pgno iFreePage,          /* The location to move pDbPage to */
		int isCommit             /* isCommit flag passed to sqlite3PagerMovepage */
		){
			MemPage *pPtrPage;   /* The page that contains a pointer to pDbPage */
			Pgno iDbPage = pDbPage->pgno;
			Pager *pPager = pBt->pPager;
			int rc;

			assert( eType==PTRMAP_OVERFLOW2 || eType==PTRMAP_OVERFLOW1 || 
				eType==PTRMAP_BTREE || eType==PTRMAP_ROOTPAGE );
			assert( sqlite3_mutex_held(pBt->mutex) );
			assert( pDbPage->pBt==pBt );

			/* Move page iDbPage from its current location to page number iFreePage */
			TRACE(("AUTOVACUUM: Moving %d to free page %d (ptr page %d type %d)\n", 
				iDbPage, iFreePage, iPtrPage, eType));
			rc = sqlite3PagerMovepage(pPager, pDbPage->pDbPage, iFreePage, isCommit);
			if( rc!=SQLITE_OK ){
				return rc;
			}
			pDbPage->pgno = iFreePage;

			/* If pDbPage was a btree-page, then it may have child pages and/or cells
			** that point to overflow pages. The pointer map entries for all these
			** pages need to be changed.
			**
			** If pDbPage is an overflow page, then the first 4 bytes may store a
			** pointer to a subsequent overflow page. If this is the case, then
			** the pointer map needs to be updated for the subsequent overflow page.
			*/
			if( eType==PTRMAP_BTREE || eType==PTRMAP_ROOTPAGE ){
				rc = setChildPtrmaps(pDbPage);
				if( rc!=SQLITE_OK ){
					return rc;
				}
			}else{
				Pgno nextOvfl = get4byte(pDbPage->aData);
				if( nextOvfl!=0 ){
					ptrmapPut(pBt, nextOvfl, PTRMAP_OVERFLOW2, iFreePage, &rc);
					if( rc!=SQLITE_OK ){
						return rc;
					}
				}
			}

			/* Fix the database pointer on page iPtrPage that pointed at iDbPage so
			** that it points at iFreePage. Also fix the pointer map entry for
			** iPtrPage.
			*/
			if( eType!=PTRMAP_ROOTPAGE ){
				rc = btreeGetPage(pBt, iPtrPage, &pPtrPage, 0);
				if( rc!=SQLITE_OK ){
					return rc;
				}
				rc = sqlite3PagerWrite(pPtrPage->pDbPage);
				if( rc!=SQLITE_OK ){
					releasePage(pPtrPage);
					return rc;
				}
				rc = modifyPagePointer(pPtrPage, iDbPage, iFreePage, eType);
				releasePage(pPtrPage);
				if( rc==SQLITE_OK ){
					ptrmapPut(pBt, iFreePage, eType, iPtrPage, &rc);
				}
			}
			return rc;
	}

	static int allocateBtreePage(BtShared *, MemPage **, Pgno *, Pgno, u8);
	static int incrVacuumStep(BtShared *pBt, Pgno nFin, Pgno iLastPg, int bCommit){
		Pgno nFreeList;           /* Number of pages still on the free-list */
		int rc;

		assert( sqlite3_mutex_held(pBt->mutex) );
		assert( iLastPg>nFin );

		if( !PTRMAP_ISPAGE(pBt, iLastPg) && iLastPg!=PENDING_BYTE_PAGE(pBt) ){
			u8 eType;
			Pgno iPtrPage;

			nFreeList = get4byte(&pBt->pPage1->aData[36]);
			if( nFreeList==0 ){
				return SQLITE_DONE;
			}

			rc = ptrmapGet(pBt, iLastPg, &eType, &iPtrPage);
			if( rc!=SQLITE_OK ){
				return rc;
			}
			if( eType==PTRMAP_ROOTPAGE ){
				return SQLITE_CORRUPT_BKPT;
			}

			if( eType==PTRMAP_FREEPAGE ){
				if( bCommit==0 ){
					/* Remove the page from the files free-list. This is not required
					** if bCommit is non-zero. In that case, the free-list will be
					** truncated to zero after this function returns, so it doesn't 
					** matter if it still contains some garbage entries.
					*/
					Pgno iFreePg;
					MemPage *pFreePg;
					rc = allocateBtreePage(pBt, &pFreePg, &iFreePg, iLastPg, BTALLOC_EXACT);
					if( rc!=SQLITE_OK ){
						return rc;
					}
					assert( iFreePg==iLastPg );
					releasePage(pFreePg);
				}
			} else {
				Pgno iFreePg;             /* Index of free page to move pLastPg to */
				MemPage *pLastPg;
				u8 eMode = BTALLOC_ANY;   /* Mode parameter for allocateBtreePage() */
				Pgno iNear = 0;           /* nearby parameter for allocateBtreePage() */

				rc = btreeGetPage(pBt, iLastPg, &pLastPg, 0);
				if( rc!=SQLITE_OK ){
					return rc;
				}

				/* If bCommit is zero, this loop runs exactly once and page pLastPg
				** is swapped with the first free page pulled off the free list.
				**
				** On the other hand, if bCommit is greater than zero, then keep
				** looping until a free-page located within the first nFin pages
				** of the file is found.
				*/
				if( bCommit==0 ){
					eMode = BTALLOC_LE;
					iNear = nFin;
				}
				do {
					MemPage *pFreePg;
					rc = allocateBtreePage(pBt, &pFreePg, &iFreePg, iNear, eMode);
					if( rc!=SQLITE_OK ){
						releasePage(pLastPg);
						return rc;
					}
					releasePage(pFreePg);
				}while( bCommit && iFreePg>nFin );
				assert( iFreePg<iLastPg );

				rc = relocatePage(pBt, pLastPg, eType, iPtrPage, iFreePg, bCommit);
				releasePage(pLastPg);
				if( rc!=SQLITE_OK ){
					return rc;
				}
			}
		}

		if( bCommit==0 ){
			do {
				iLastPg--;
			}while( iLastPg==PENDING_BYTE_PAGE(pBt) || PTRMAP_ISPAGE(pBt, iLastPg) );
			pBt->bDoTruncate = 1;
			pBt->nPage = iLastPg;
		}
		return SQLITE_OK;
	}

	static Pgno finalDbSize(BtShared *pBt, Pgno nOrig, Pgno nFree){
		int nEntry;                     /* Number of entries on one ptrmap page */
		Pgno nPtrmap;                   /* Number of PtrMap pages to be freed */
		Pgno nFin;                      /* Return value */

		nEntry = pBt->usableSize/5;
		nPtrmap = (nFree-nOrig+PTRMAP_PAGENO(pBt, nOrig)+nEntry)/nEntry;
		nFin = nOrig - nFree - nPtrmap;
		if( nOrig>PENDING_BYTE_PAGE(pBt) && nFin<PENDING_BYTE_PAGE(pBt) ){
			nFin--;
		}
		while( PTRMAP_ISPAGE(pBt, nFin) || nFin==PENDING_BYTE_PAGE(pBt) ){
			nFin--;
		}

		return nFin;
	}

	int sqlite3BtreeIncrVacuum(Btree *p){
		int rc;
		BtShared *pBt = p->pBt;

		sqlite3BtreeEnter(p);
		assert( pBt->inTransaction==TRANS_WRITE && p->inTrans==TRANS_WRITE );
		if( !pBt->autoVacuum ){
			rc = SQLITE_DONE;
		}else{
			Pgno nOrig = btreePagecount(pBt);
			Pgno nFree = get4byte(&pBt->pPage1->aData[36]);
			Pgno nFin = finalDbSize(pBt, nOrig, nFree);

			if( nOrig<nFin ){
				rc = SQLITE_CORRUPT_BKPT;
			}else if( nFree>0 ){
				invalidateAllOverflowCache(pBt);
				rc = incrVacuumStep(pBt, nFin, nOrig, 0);
				if( rc==SQLITE_OK ){
					rc = sqlite3PagerWrite(pBt->pPage1->pDbPage);
					put4byte(&pBt->pPage1->aData[28], pBt->nPage);
				}
			}else{
				rc = SQLITE_DONE;
			}
		}
		sqlite3BtreeLeave(p);
		return rc;
	}

	static int autoVacuumCommit(BtShared *pBt){
		int rc = SQLITE_OK;
		Pager *pPager = pBt->pPager;
		VVA_ONLY( int nRef = sqlite3PagerRefcount(pPager) );

		assert( sqlite3_mutex_held(pBt->mutex) );
		invalidateAllOverflowCache(pBt);
		assert(pBt->autoVacuum);
		if( !pBt->incrVacuum ){
			Pgno nFin;         /* Number of pages in database after autovacuuming */
			Pgno nFree;        /* Number of pages on the freelist initially */
			Pgno iFree;        /* The next page to be freed */
			Pgno nOrig;        /* Database size before freeing */

			nOrig = btreePagecount(pBt);
			if( PTRMAP_ISPAGE(pBt, nOrig) || nOrig==PENDING_BYTE_PAGE(pBt) ){
				/* It is not possible to create a database for which the final page
				** is either a pointer-map page or the pending-byte page. If one
				** is encountered, this indicates corruption.
				*/
				return SQLITE_CORRUPT_BKPT;
			}

			nFree = get4byte(&pBt->pPage1->aData[36]);
			nFin = finalDbSize(pBt, nOrig, nFree);
			if( nFin>nOrig ) return SQLITE_CORRUPT_BKPT;

			for(iFree=nOrig; iFree>nFin && rc==SQLITE_OK; iFree--){
				rc = incrVacuumStep(pBt, nFin, iFree, 1);
			}
			if( (rc==SQLITE_DONE || rc==SQLITE_OK) && nFree>0 ){
				rc = sqlite3PagerWrite(pBt->pPage1->pDbPage);
				put4byte(&pBt->pPage1->aData[32], 0);
				put4byte(&pBt->pPage1->aData[36], 0);
				put4byte(&pBt->pPage1->aData[28], nFin);
				pBt->bDoTruncate = 1;
				pBt->nPage = nFin;
			}
			if( rc!=SQLITE_OK ){
				sqlite3PagerRollback(pPager);
			}
		}

		assert( nRef==sqlite3PagerRefcount(pPager) );
		return rc;
	}

#else
#define setChildPtrmaps(x) RC::OK
#endif
#pragma endregion

#pragma region Commit

	int sqlite3BtreeCommitPhaseOne(Btree *p, const char *zMaster){
		int rc = SQLITE_OK;
		if( p->inTrans==TRANS_WRITE ){
			BtShared *pBt = p->pBt;
			sqlite3BtreeEnter(p);
#ifndef SQLITE_OMIT_AUTOVACUUM
			if( pBt->autoVacuum ){
				rc = autoVacuumCommit(pBt);
				if( rc!=SQLITE_OK ){
					sqlite3BtreeLeave(p);
					return rc;
				}
			}
			if( pBt->bDoTruncate ){
				sqlite3PagerTruncateImage(pBt->pPager, pBt->nPage);
			}
#endif
			rc = sqlite3PagerCommitPhaseOne(pBt->pPager, zMaster, 0);
			sqlite3BtreeLeave(p);
		}
		return rc;
	}

	static void btreeEndTransaction(Btree *p){
		BtShared *pBt = p->pBt;
		assert( sqlite3BtreeHoldsMutex(p) );

#ifndef SQLITE_OMIT_AUTOVACUUM
		pBt->bDoTruncate = 0;
#endif
		btreeClearHasContent(pBt);
		if( p->inTrans>TRANS_NONE && p->db->activeVdbeCnt>1 ){
			/* If there are other active statements that belong to this database
			** handle, downgrade to a read-only transaction. The other statements
			** may still be reading from the database.  */
			downgradeAllSharedCacheTableLocks(p);
			p->inTrans = TRANS_READ;
		}else{
			/* If the handle had any kind of transaction open, decrement the 
			** transaction count of the shared btree. If the transaction count 
			** reaches 0, set the shared state to TRANS_NONE. The unlockBtreeIfUnused()
			** call below will unlock the pager.  */
			if( p->inTrans!=TRANS_NONE ){
				clearAllSharedCacheTableLocks(p);
				pBt->nTransaction--;
				if( 0==pBt->nTransaction ){
					pBt->inTransaction = TRANS_NONE;
				}
			}

			/* Set the current transaction state to TRANS_NONE and unlock the 
			** pager if this call closed the only read or write transaction.  */
			p->inTrans = TRANS_NONE;
			unlockBtreeIfUnused(pBt);
		}

		btreeIntegrity(p);
	}

	int sqlite3BtreeCommitPhaseTwo(Btree *p, int bCleanup){

		if( p->inTrans==TRANS_NONE ) return SQLITE_OK;
		sqlite3BtreeEnter(p);
		btreeIntegrity(p);

		/* If the handle has a write-transaction open, commit the shared-btrees 
		** transaction and set the shared state to TRANS_READ.
		*/
		if( p->inTrans==TRANS_WRITE ){
			int rc;
			BtShared *pBt = p->pBt;
			assert( pBt->inTransaction==TRANS_WRITE );
			assert( pBt->nTransaction>0 );
			rc = sqlite3PagerCommitPhaseTwo(pBt->pPager);
			if( rc!=SQLITE_OK && bCleanup==0 ){
				sqlite3BtreeLeave(p);
				return rc;
			}
			pBt->inTransaction = TRANS_READ;
		}

		btreeEndTransaction(p);
		sqlite3BtreeLeave(p);
		return SQLITE_OK;
	}

	int sqlite3BtreeCommit(Btree *p){
		int rc;
		sqlite3BtreeEnter(p);
		rc = sqlite3BtreeCommitPhaseOne(p, 0);
		if( rc==SQLITE_OK ){
			rc = sqlite3BtreeCommitPhaseTwo(p, 0);
		}
		sqlite3BtreeLeave(p);
		return rc;
	}

#pragma endregion

#pragma region Cursors

#ifndef _DEBUG
	static int countWriteCursors(BtShared *pBt){
		BtCursor *pCur;
		int r = 0;
		for(pCur=pBt->pCursor; pCur; pCur=pCur->pNext){
			if( pCur->wrFlag && pCur->eState!=CURSOR_FAULT ) r++; 
		}
		return r;
	}
#endif

	void sqlite3BtreeTripAllCursors(Btree *pBtree, int errCode){
		BtCursor *p;
		if( pBtree==0 ) return;
		sqlite3BtreeEnter(pBtree);
		for(p=pBtree->pBt->pCursor; p; p=p->pNext){
			int i;
			sqlite3BtreeClearCursor(p);
			p->eState = CURSOR_FAULT;
			p->skipNext = errCode;
			for(i=0; i<=p->iPage; i++){
				releasePage(p->apPage[i]);
				p->apPage[i] = 0;
			}
		}
		sqlite3BtreeLeave(pBtree);
	}

	int sqlite3BtreeRollback(Btree *p, int tripCode){
		int rc;
		BtShared *pBt = p->pBt;
		MemPage *pPage1;

		sqlite3BtreeEnter(p);
		if( tripCode==SQLITE_OK ){
			rc = tripCode = saveAllCursors(pBt, 0, 0);
		}else{
			rc = SQLITE_OK;
		}
		if( tripCode ){
			sqlite3BtreeTripAllCursors(p, tripCode);
		}
		btreeIntegrity(p);

		if( p->inTrans==TRANS_WRITE ){
			int rc2;

			assert( TRANS_WRITE==pBt->inTransaction );
			rc2 = sqlite3PagerRollback(pBt->pPager);
			if( rc2!=SQLITE_OK ){
				rc = rc2;
			}

			/* The rollback may have destroyed the pPage1->aData value.  So
			** call btreeGetPage() on page 1 again to make
			** sure pPage1->aData is set correctly. */
			if( btreeGetPage(pBt, 1, &pPage1, 0)==SQLITE_OK ){
				int nPage = get4byte(28+(u8*)pPage1->aData);
				testcase( nPage==0 );
				if( nPage==0 ) sqlite3PagerPagecount(pBt->pPager, &nPage);
				testcase( pBt->nPage!=nPage );
				pBt->nPage = nPage;
				releasePage(pPage1);
			}
			assert( countWriteCursors(pBt)==0 );
			pBt->inTransaction = TRANS_READ;
		}

		btreeEndTransaction(p);
		sqlite3BtreeLeave(p);
		return rc;
	}

	int sqlite3BtreeBeginStmt(Btree *p, int iStatement){
		int rc;
		BtShared *pBt = p->pBt;
		sqlite3BtreeEnter(p);
		assert( p->inTrans==TRANS_WRITE );
		assert( (pBt->btsFlags & BTS_READ_ONLY)==0 );
		assert( iStatement>0 );
		assert( iStatement>p->db->nSavepoint );
		assert( pBt->inTransaction==TRANS_WRITE );
		/* At the pager level, a statement transaction is a savepoint with
		** an index greater than all savepoints created explicitly using
		** SQL statements. It is illegal to open, release or rollback any
		** such savepoints while the statement transaction savepoint is active.
		*/
		rc = sqlite3PagerOpenSavepoint(pBt->pPager, iStatement);
		sqlite3BtreeLeave(p);
		return rc;
	}

	int sqlite3BtreeSavepoint(Btree *p, int op, int iSavepoint){
		int rc = SQLITE_OK;
		if( p && p->inTrans==TRANS_WRITE ){
			BtShared *pBt = p->pBt;
			assert( op==SAVEPOINT_RELEASE || op==SAVEPOINT_ROLLBACK );
			assert( iSavepoint>=0 || (iSavepoint==-1 && op==SAVEPOINT_ROLLBACK) );
			sqlite3BtreeEnter(p);
			rc = sqlite3PagerSavepoint(pBt->pPager, op, iSavepoint);
			if( rc==SQLITE_OK ){
				if( iSavepoint<0 && (pBt->btsFlags & BTS_INITIALLY_EMPTY)!=0 ){
					pBt->nPage = 0;
				}
				rc = newDatabase(pBt);
				pBt->nPage = get4byte(28 + pBt->pPage1->aData);

				/* The database size was written into the offset 28 of the header
				** when the transaction started, so we know that the value at offset
				** 28 is nonzero. */
				assert( pBt->nPage>0 );
			}
			sqlite3BtreeLeave(p);
		}
		return rc;
	}

#pragma endregion

#pragma region Cursor

	static int btreeCursor(
		Btree *p,                              /* The btree */
		int iTable,                            /* Root page of table to open */
		int wrFlag,                            /* 1 to write. 0 read-only */
	struct KeyInfo *pKeyInfo,              /* First arg to comparison function */
		BtCursor *pCur                         /* Space for new cursor */
		){
			BtShared *pBt = p->pBt;                /* Shared b-tree handle */

			assert( sqlite3BtreeHoldsMutex(p) );
			assert( wrFlag==0 || wrFlag==1 );

			/* The following assert statements verify that if this is a sharable 
			** b-tree database, the connection is holding the required table locks, 
			** and that no other connection has any open cursor that conflicts with 
			** this lock.  */
			assert( hasSharedCacheTableLock(p, iTable, pKeyInfo!=0, wrFlag+1) );
			assert( wrFlag==0 || !hasReadConflicts(p, iTable) );

			/* Assert that the caller has opened the required transaction. */
			assert( p->inTrans>TRANS_NONE );
			assert( wrFlag==0 || p->inTrans==TRANS_WRITE );
			assert( pBt->pPage1 && pBt->pPage1->aData );

			if( NEVER(wrFlag && (pBt->btsFlags & BTS_READ_ONLY)!=0) ){
				return SQLITE_READONLY;
			}
			if( iTable==1 && btreePagecount(pBt)==0 ){
				assert( wrFlag==0 );
				iTable = 0;
			}

			/* Now that no other errors can occur, finish filling in the BtCursor
			** variables and link the cursor into the BtShared list.  */
			pCur->pgnoRoot = (Pgno)iTable;
			pCur->iPage = -1;
			pCur->pKeyInfo = pKeyInfo;
			pCur->pBtree = p;
			pCur->pBt = pBt;
			pCur->wrFlag = (u8)wrFlag;
			pCur->pNext = pBt->pCursor;
			if( pCur->pNext ){
				pCur->pNext->pPrev = pCur;
			}
			pBt->pCursor = pCur;
			pCur->eState = CURSOR_INVALID;
			pCur->cachedRowid = 0;
			return SQLITE_OK;
	}
	int sqlite3BtreeCursor(
		Btree *p,                                   /* The btree */
		int iTable,                                 /* Root page of table to open */
		int wrFlag,                                 /* 1 to write. 0 read-only */
	struct KeyInfo *pKeyInfo,                   /* First arg to xCompare() */
		BtCursor *pCur                              /* Write new cursor here */
		){
			int rc;
			sqlite3BtreeEnter(p);
			rc = btreeCursor(p, iTable, wrFlag, pKeyInfo, pCur);
			sqlite3BtreeLeave(p);
			return rc;
	}

	int sqlite3BtreeCursorSize(void){
		return ROUND8(sizeof(BtCursor));
	}

	void sqlite3BtreeCursorZero(BtCursor *p){
		memset(p, 0, offsetof(BtCursor, iPage));
	}

	void sqlite3BtreeSetCachedRowid(BtCursor *pCur, sqlite3_int64 iRowid){
		BtCursor *p;
		for(p=pCur->pBt->pCursor; p; p=p->pNext){
			if( p->pgnoRoot==pCur->pgnoRoot ) p->cachedRowid = iRowid;
		}
		assert( pCur->cachedRowid==iRowid );
	}

	sqlite3_int64 sqlite3BtreeGetCachedRowid(BtCursor *pCur){
		return pCur->cachedRowid;
	}

	int sqlite3BtreeCloseCursor(BtCursor *pCur){
		Btree *pBtree = pCur->pBtree;
		if( pBtree ){
			int i;
			BtShared *pBt = pCur->pBt;
			sqlite3BtreeEnter(pBtree);
			sqlite3BtreeClearCursor(pCur);
			if( pCur->pPrev ){
				pCur->pPrev->pNext = pCur->pNext;
			}else{
				pBt->pCursor = pCur->pNext;
			}
			if( pCur->pNext ){
				pCur->pNext->pPrev = pCur->pPrev;
			}
			for(i=0; i<=pCur->iPage; i++){
				releasePage(pCur->apPage[i]);
			}
			unlockBtreeIfUnused(pBt);
			invalidateOverflowCache(pCur);
			/* sqlite3_free(pCur); */
			sqlite3BtreeLeave(pBtree);
		}
		return SQLITE_OK;
	}

#ifndef _DEBUG
	static void assertCellInfo(BtCursor *pCur){
		CellInfo info;
		int iPage = pCur->iPage;
		memset(&info, 0, sizeof(info));
		btreeParseCell(pCur->apPage[iPage], pCur->aiIdx[iPage], &info);
		assert( memcmp(&info, &pCur->info, sizeof(info))==0 );
	}
#else
#define assertCellInfo(x)
#endif
#ifdef _MSC_VER
	/* Use a real function in MSVC to work around bugs in that compiler. */
	static void getCellInfo(BtCursor *pCur){
		if( pCur->info.nSize==0 ){
			int iPage = pCur->iPage;
			btreeParseCell(pCur->apPage[iPage],pCur->aiIdx[iPage],&pCur->info);
			pCur->validNKey = 1;
		}else{
			assertCellInfo(pCur);
		}
	}
#else /* if not _MSC_VER */
	/* Use a macro in all other compilers so that the function is inlined */
#define getCellInfo(pCur)                                                      \
	if( pCur->info.nSize==0 ){                                                   \
	int iPage = pCur->iPage;                                                   \
	btreeParseCell(pCur->apPage[iPage],pCur->aiIdx[iPage],&pCur->info); \
	pCur->validNKey = 1;                                                       \
	}else{                                                                       \
	assertCellInfo(pCur);                                                      \
	}
#endif /* _MSC_VER */

#ifndef _DEBUG  // The next routine used only within assert() statements
	int sqlite3BtreeCursorIsValid(BtCursor *pCur){
		return pCur && pCur->eState==CURSOR_VALID;
	}
#endif

	int sqlite3BtreeKeySize(BtCursor *pCur, i64 *pSize){
		assert( cursorHoldsMutex(pCur) );
		assert( pCur->eState==CURSOR_INVALID || pCur->eState==CURSOR_VALID );
		if( pCur->eState!=CURSOR_VALID ){
			*pSize = 0;
		}else{
			getCellInfo(pCur);
			*pSize = pCur->info.nKey;
		}
		return SQLITE_OK;
	}

	int sqlite3BtreeDataSize(BtCursor *pCur, u32 *pSize){
		assert( cursorHoldsMutex(pCur) );
		assert( pCur->eState==CURSOR_VALID );
		getCellInfo(pCur);
		*pSize = pCur->info.nData;
		return SQLITE_OK;
	}

#pragma endregion

#pragma region Overflow

	static int getOverflowPage(
		BtShared *pBt,               /* The database file */
		Pgno ovfl,                   /* Current overflow page number */
		MemPage **ppPage,            /* OUT: MemPage handle (may be NULL) */
		Pgno *pPgnoNext              /* OUT: Next overflow page number */
		){
			Pgno next = 0;
			MemPage *pPage = 0;
			int rc = SQLITE_OK;

			assert( sqlite3_mutex_held(pBt->mutex) );
			assert(pPgnoNext);

#ifndef OMIT_AUTOVACUUM
			/* Try to find the next page in the overflow list using the
			** autovacuum pointer-map pages. Guess that the next page in 
			** the overflow list is page number (ovfl+1). If that guess turns 
			** out to be wrong, fall back to loading the data of page 
			** number ovfl to determine the next page number.
			*/
			if( pBt->autoVacuum ){
				Pgno pgno;
				Pgno iGuess = ovfl+1;
				u8 eType;

				while( PTRMAP_ISPAGE(pBt, iGuess) || iGuess==PENDING_BYTE_PAGE(pBt) ){
					iGuess++;
				}

				if( iGuess<=btreePagecount(pBt) ){
					rc = ptrmapGet(pBt, iGuess, &eType, &pgno);
					if( rc==SQLITE_OK && eType==PTRMAP_OVERFLOW2 && pgno==ovfl ){
						next = iGuess;
						rc = SQLITE_DONE;
					}
				}
			}
#endif

			assert( next==0 || rc==SQLITE_DONE );
			if( rc==SQLITE_OK ){
				rc = btreeGetPage(pBt, ovfl, &pPage, 0);
				assert( rc==SQLITE_OK || pPage==0 );
				if( rc==SQLITE_OK ){
					next = get4byte(pPage->aData);
				}
			}

			*pPgnoNext = next;
			if( ppPage ){
				*ppPage = pPage;
			}else{
				releasePage(pPage);
			}
			return (rc==SQLITE_DONE ? SQLITE_OK : rc);
	}

	static int copyPayload(
		void *pPayload,           /* Pointer to page data */
		void *pBuf,               /* Pointer to buffer */
		int nByte,                /* Number of bytes to copy */
		int eOp,                  /* 0 -> copy from page, 1 -> copy to page */
		DbPage *pDbPage           /* Page containing pPayload */
		){
			if( eOp ){
				/* Copy data from buffer to page (a write operation) */
				int rc = sqlite3PagerWrite(pDbPage);
				if( rc!=SQLITE_OK ){
					return rc;
				}
				memcpy(pPayload, pBuf, nByte);
			}else{
				/* Copy data from page to buffer (a read operation) */
				memcpy(pBuf, pPayload, nByte);
			}
			return SQLITE_OK;
	}

	static int accessPayload(
		BtCursor *pCur,      /* Cursor pointing to entry to read from */
		u32 offset,          /* Begin reading this far into payload */
		u32 amt,             /* Read this many bytes */
		unsigned char *pBuf, /* Write the bytes into this buffer */ 
		int eOp              /* zero to read. non-zero to write. */
		){
			unsigned char *aPayload;
			int rc = SQLITE_OK;
			u32 nKey;
			int iIdx = 0;
			MemPage *pPage = pCur->apPage[pCur->iPage]; /* Btree page of current entry */
			BtShared *pBt = pCur->pBt;                  /* Btree this cursor belongs to */

			assert( pPage );
			assert( pCur->eState==CURSOR_VALID );
			assert( pCur->aiIdx[pCur->iPage]<pPage->nCell );
			assert( cursorHoldsMutex(pCur) );

			getCellInfo(pCur);
			aPayload = pCur->info.pCell + pCur->info.nHeader;
			nKey = (pPage->intKey ? 0 : (int)pCur->info.nKey);

			if( NEVER(offset+amt > nKey+pCur->info.nData) 
				|| &aPayload[pCur->info.nLocal] > &pPage->aData[pBt->usableSize]
			){
				/* Trying to read or write past the end of the data is an error */
				return SQLITE_CORRUPT_BKPT;
			}

			/* Check if data must be read/written to/from the btree page itself. */
			if( offset<pCur->info.nLocal ){
				int a = amt;
				if( a+offset>pCur->info.nLocal ){
					a = pCur->info.nLocal - offset;
				}
				rc = copyPayload(&aPayload[offset], pBuf, a, eOp, pPage->pDbPage);
				offset = 0;
				pBuf += a;
				amt -= a;
			}else{
				offset -= pCur->info.nLocal;
			}

			if( rc==SQLITE_OK && amt>0 ){
				const u32 ovflSize = pBt->usableSize - 4;  /* Bytes content per ovfl page */
				Pgno nextPage;

				nextPage = get4byte(&aPayload[pCur->info.nLocal]);

#ifndef OMIT_INCRBLOB
				/* If the isIncrblobHandle flag is set and the BtCursor.aOverflow[]
				** has not been allocated, allocate it now. The array is sized at
				** one entry for each overflow page in the overflow chain. The
				** page number of the first overflow page is stored in aOverflow[0],
				** etc. A value of 0 in the aOverflow[] array means "not yet known"
				** (the cache is lazily populated).
				*/
				if( pCur->isIncrblobHandle && !pCur->aOverflow ){
					int nOvfl = (pCur->info.nPayload-pCur->info.nLocal+ovflSize-1)/ovflSize;
					pCur->aOverflow = (Pgno *)sqlite3MallocZero(sizeof(Pgno)*nOvfl);
					/* nOvfl is always positive.  If it were zero, fetchPayload would have
					** been used instead of this routine. */
					if( ALWAYS(nOvfl) && !pCur->aOverflow ){
						rc = SQLITE_NOMEM;
					}
				}

				/* If the overflow page-list cache has been allocated and the
				** entry for the first required overflow page is valid, skip
				** directly to it.
				*/
				if( pCur->aOverflow && pCur->aOverflow[offset/ovflSize] ){
					iIdx = (offset/ovflSize);
					nextPage = pCur->aOverflow[iIdx];
					offset = (offset%ovflSize);
				}
#endif

				for( ; rc==SQLITE_OK && amt>0 && nextPage; iIdx++){

#ifndef OMIT_INCRBLOB
					/* If required, populate the overflow page-list cache. */
					if( pCur->aOverflow ){
						assert(!pCur->aOverflow[iIdx] || pCur->aOverflow[iIdx]==nextPage);
						pCur->aOverflow[iIdx] = nextPage;
					}
#endif

					if( offset>=ovflSize ){
						/* The only reason to read this page is to obtain the page
						** number for the next page in the overflow chain. The page
						** data is not required. So first try to lookup the overflow
						** page-list cache, if any, then fall back to the getOverflowPage()
						** function.
						*/
#ifndef OMIT_INCRBLOB
						if( pCur->aOverflow && pCur->aOverflow[iIdx+1] ){
							nextPage = pCur->aOverflow[iIdx+1];
						} else 
#endif
							rc = getOverflowPage(pBt, nextPage, 0, &nextPage);
						offset -= ovflSize;
					}else{
						/* Need to read this page properly. It contains some of the
						** range of data that is being read (eOp==0) or written (eOp!=0).
						*/
#ifdef DIRECT_OVERFLOW_READ
						sqlite3_file *fd;
#endif
						int a = amt;
						if( a + offset > ovflSize ){
							a = ovflSize - offset;
						}

#ifdef DIRECT_OVERFLOW_READ
						/* If all the following are true:
						**
						**   1) this is a read operation, and 
						**   2) data is required from the start of this overflow page, and
						**   3) the database is file-backed, and
						**   4) there is no open write-transaction, and
						**   5) the database is not a WAL database,
						**
						** then data can be read directly from the database file into the
						** output buffer, bypassing the page-cache altogether. This speeds
						** up loading large records that span many overflow pages.
						*/
						if( eOp==0                                             /* (1) */
							&& offset==0                                          /* (2) */
							&& pBt->inTransaction==TRANS_READ                     /* (4) */
							&& (fd = sqlite3PagerFile(pBt->pPager))->pMethods     /* (3) */
							&& pBt->pPage1->aData[19]==0x01                       /* (5) */
							){
								u8 aSave[4];
								u8 *aWrite = &pBuf[-4];
								memcpy(aSave, aWrite, 4);
								rc = sqlite3OsRead(fd, aWrite, a+4, (i64)pBt->pageSize*(nextPage-1));
								nextPage = get4byte(aWrite);
								memcpy(aWrite, aSave, 4);
						}else
#endif

						{
							DbPage *pDbPage;
							rc = sqlite3PagerGet(pBt->pPager, nextPage, &pDbPage);
							if( rc==SQLITE_OK ){
								aPayload = sqlite3PagerGetData(pDbPage);
								nextPage = get4byte(aPayload);
								rc = copyPayload(&aPayload[offset+4], pBuf, a, eOp, pDbPage);
								sqlite3PagerUnref(pDbPage);
								offset = 0;
							}
						}
						amt -= a;
						pBuf += a;
					}
				}
			}

			if( rc==SQLITE_OK && amt>0 ){
				return SQLITE_CORRUPT_BKPT;
			}
			return rc;
	}

	int sqlite3BtreeKey(BtCursor *pCur, u32 offset, u32 amt, void *pBuf){
		assert( cursorHoldsMutex(pCur) );
		assert( pCur->eState==CURSOR_VALID );
		assert( pCur->iPage>=0 && pCur->apPage[pCur->iPage] );
		assert( pCur->aiIdx[pCur->iPage]<pCur->apPage[pCur->iPage]->nCell );
		return accessPayload(pCur, offset, amt, (unsigned char*)pBuf, 0);
	}

	int sqlite3BtreeData(BtCursor *pCur, u32 offset, u32 amt, void *pBuf){
		int rc;

#ifndef OMIT_INCRBLOB
		if ( pCur->eState==CURSOR_INVALID ){
			return SQLITE_ABORT;
		}
#endif

		assert( cursorHoldsMutex(pCur) );
		rc = restoreCursorPosition(pCur);
		if( rc==SQLITE_OK ){
			assert( pCur->eState==CURSOR_VALID );
			assert( pCur->iPage>=0 && pCur->apPage[pCur->iPage] );
			assert( pCur->aiIdx[pCur->iPage]<pCur->apPage[pCur->iPage]->nCell );
			rc = accessPayload(pCur, offset, amt, pBuf, 0);
		}
		return rc;
	}

	static const unsigned char *fetchPayload(
		BtCursor *pCur,      /* Cursor pointing to entry to read from */
		int *pAmt,           /* Write the number of available bytes here */
		int skipKey          /* read beginning at data if this is true */
		){
			unsigned char *aPayload;
			MemPage *pPage;
			u32 nKey;
			u32 nLocal;

			assert( pCur!=0 && pCur->iPage>=0 && pCur->apPage[pCur->iPage]);
			assert( pCur->eState==CURSOR_VALID );
			assert( cursorHoldsMutex(pCur) );
			pPage = pCur->apPage[pCur->iPage];
			assert( pCur->aiIdx[pCur->iPage]<pPage->nCell );
			if( NEVER(pCur->info.nSize==0) ){
				btreeParseCell(pCur->apPage[pCur->iPage], pCur->aiIdx[pCur->iPage],
					&pCur->info);
			}
			aPayload = pCur->info.pCell;
			aPayload += pCur->info.nHeader;
			if( pPage->intKey ){
				nKey = 0;
			}else{
				nKey = (int)pCur->info.nKey;
			}
			if( skipKey ){
				aPayload += nKey;
				nLocal = pCur->info.nLocal - nKey;
			}else{
				nLocal = pCur->info.nLocal;
				assert( nLocal<=nKey );
			}
			*pAmt = nLocal;
			return aPayload;
	}

	const void *sqlite3BtreeKeyFetch(BtCursor *pCur, int *pAmt){
		const void *p = 0;
		assert( sqlite3_mutex_held(pCur->pBtree->db->mutex) );
		assert( cursorHoldsMutex(pCur) );
		if( ALWAYS(pCur->eState==CURSOR_VALID) ){
			p = (const void*)fetchPayload(pCur, pAmt, 0);
		}
		return p;
	}
	const void *sqlite3BtreeDataFetch(BtCursor *pCur, int *pAmt){
		const void *p = 0;
		assert( sqlite3_mutex_held(pCur->pBtree->db->mutex) );
		assert( cursorHoldsMutex(pCur) );
		if( ALWAYS(pCur->eState==CURSOR_VALID) ){
			p = (const void*)fetchPayload(pCur, pAmt, 1);
		}
		return p;
	}

#pragma endregion

#pragma region Move

	static int moveToChild(BtCursor *pCur, u32 newPgno){
		int rc;
		int i = pCur->iPage;
		MemPage *pNewPage;
		BtShared *pBt = pCur->pBt;

		assert( cursorHoldsMutex(pCur) );
		assert( pCur->eState==CURSOR_VALID );
		assert( pCur->iPage<BTCURSOR_MAX_DEPTH );
		if( pCur->iPage>=(BTCURSOR_MAX_DEPTH-1) ){
			return SQLITE_CORRUPT_BKPT;
		}
		rc = getAndInitPage(pBt, newPgno, &pNewPage);
		if( rc ) return rc;
		pCur->apPage[i+1] = pNewPage;
		pCur->aiIdx[i+1] = 0;
		pCur->iPage++;

		pCur->info.nSize = 0;
		pCur->validNKey = 0;
		if( pNewPage->nCell<1 || pNewPage->intKey!=pCur->apPage[i]->intKey ){
			return SQLITE_CORRUPT_BKPT;
		}
		return SQLITE_OK;
	}

#if 0
	static void assertParentIndex(MemPage *pParent, int iIdx, Pgno iChild){
		assert( iIdx<=pParent->nCell );
		if( iIdx==pParent->nCell ){
			assert( get4byte(&pParent->aData[pParent->hdrOffset+8])==iChild );
		}else{
			assert( get4byte(findCell(pParent, iIdx))==iChild );
		}
	}
#else
#define assertParentIndex(x,y,z) 
#endif

	static void moveToParent(BtCursor *pCur){
		assert( cursorHoldsMutex(pCur) );
		assert( pCur->eState==CURSOR_VALID );
		assert( pCur->iPage>0 );
		assert( pCur->apPage[pCur->iPage] );

		/* UPDATE: It is actually possible for the condition tested by the assert
		** below to be untrue if the database file is corrupt. This can occur if
		** one cursor has modified page pParent while a reference to it is held 
		** by a second cursor. Which can only happen if a single page is linked
		** into more than one b-tree structure in a corrupt database.  */
#if 0
		assertParentIndex(
			pCur->apPage[pCur->iPage-1], 
			pCur->aiIdx[pCur->iPage-1], 
			pCur->apPage[pCur->iPage]->pgno
			);
#endif
		testcase( pCur->aiIdx[pCur->iPage-1] > pCur->apPage[pCur->iPage-1]->nCell );

		releasePage(pCur->apPage[pCur->iPage]);
		pCur->iPage--;
		pCur->info.nSize = 0;
		pCur->validNKey = 0;
	}

	static int moveToRoot(BtCursor *pCur){
		MemPage *pRoot;
		int rc = SQLITE_OK;
		Btree *p = pCur->pBtree;
		BtShared *pBt = p->pBt;

		assert( cursorHoldsMutex(pCur) );
		assert( CURSOR_INVALID < CURSOR_REQUIRESEEK );
		assert( CURSOR_VALID   < CURSOR_REQUIRESEEK );
		assert( CURSOR_FAULT   > CURSOR_REQUIRESEEK );
		if( pCur->eState>=CURSOR_REQUIRESEEK ){
			if( pCur->eState==CURSOR_FAULT ){
				assert( pCur->skipNext!=SQLITE_OK );
				return pCur->skipNext;
			}
			sqlite3BtreeClearCursor(pCur);
		}

		if( pCur->iPage>=0 ){
			int i;
			for(i=1; i<=pCur->iPage; i++){
				releasePage(pCur->apPage[i]);
			}
			pCur->iPage = 0;
		}else if( pCur->pgnoRoot==0 ){
			pCur->eState = CURSOR_INVALID;
			return SQLITE_OK;
		}else{
			rc = getAndInitPage(pBt, pCur->pgnoRoot, &pCur->apPage[0]);
			if( rc!=SQLITE_OK ){
				pCur->eState = CURSOR_INVALID;
				return rc;
			}
			pCur->iPage = 0;

			/* If pCur->pKeyInfo is not NULL, then the caller that opened this cursor
			** expected to open it on an index b-tree. Otherwise, if pKeyInfo is
			** NULL, the caller expects a table b-tree. If this is not the case,
			** return an SQLITE_CORRUPT error.  */
			assert( pCur->apPage[0]->intKey==1 || pCur->apPage[0]->intKey==0 );
			if( (pCur->pKeyInfo==0)!=pCur->apPage[0]->intKey ){
				return SQLITE_CORRUPT_BKPT;
			}
		}

		/* Assert that the root page is of the correct type. This must be the
		** case as the call to this function that loaded the root-page (either
		** this call or a previous invocation) would have detected corruption 
		** if the assumption were not true, and it is not possible for the flags 
		** byte to have been modified while this cursor is holding a reference
		** to the page.  */
		pRoot = pCur->apPage[0];
		assert( pRoot->pgno==pCur->pgnoRoot );
		assert( pRoot->isInit && (pCur->pKeyInfo==0)==pRoot->intKey );

		pCur->aiIdx[0] = 0;
		pCur->info.nSize = 0;
		pCur->atLast = 0;
		pCur->validNKey = 0;

		if( pRoot->nCell==0 && !pRoot->leaf ){
			Pgno subpage;
			if( pRoot->pgno!=1 ) return SQLITE_CORRUPT_BKPT;
			subpage = get4byte(&pRoot->aData[pRoot->hdrOffset+8]);
			pCur->eState = CURSOR_VALID;
			rc = moveToChild(pCur, subpage);
		}else{
			pCur->eState = ((pRoot->nCell>0)?CURSOR_VALID:CURSOR_INVALID);
		}
		return rc;
	}

	static int moveToLeftmost(BtCursor *pCur){
		Pgno pgno;
		int rc = SQLITE_OK;
		MemPage *pPage;

		assert( cursorHoldsMutex(pCur) );
		assert( pCur->eState==CURSOR_VALID );
		while( rc==SQLITE_OK && !(pPage = pCur->apPage[pCur->iPage])->leaf ){
			assert( pCur->aiIdx[pCur->iPage]<pPage->nCell );
			pgno = get4byte(findCell(pPage, pCur->aiIdx[pCur->iPage]));
			rc = moveToChild(pCur, pgno);
		}
		return rc;
	}

	static int moveToRightmost(BtCursor *pCur){
		Pgno pgno;
		int rc = SQLITE_OK;
		MemPage *pPage = 0;

		assert( cursorHoldsMutex(pCur) );
		assert( pCur->eState==CURSOR_VALID );
		while( rc==SQLITE_OK && !(pPage = pCur->apPage[pCur->iPage])->leaf ){
			pgno = get4byte(&pPage->aData[pPage->hdrOffset+8]);
			pCur->aiIdx[pCur->iPage] = pPage->nCell;
			rc = moveToChild(pCur, pgno);
		}
		if( rc==SQLITE_OK ){
			pCur->aiIdx[pCur->iPage] = pPage->nCell-1;
			pCur->info.nSize = 0;
			pCur->validNKey = 0;
		}
		return rc;
	}

	int sqlite3BtreeFirst(BtCursor *pCur, int *pRes){
		int rc;

		assert( cursorHoldsMutex(pCur) );
		assert( sqlite3_mutex_held(pCur->pBtree->db->mutex) );
		rc = moveToRoot(pCur);
		if( rc==SQLITE_OK ){
			if( pCur->eState==CURSOR_INVALID ){
				assert( pCur->pgnoRoot==0 || pCur->apPage[pCur->iPage]->nCell==0 );
				*pRes = 1;
			}else{
				assert( pCur->apPage[pCur->iPage]->nCell>0 );
				*pRes = 0;
				rc = moveToLeftmost(pCur);
			}
		}
		return rc;
	}

	int sqlite3BtreeLast(BtCursor *pCur, int *pRes){
		int rc;

		assert( cursorHoldsMutex(pCur) );
		assert( sqlite3_mutex_held(pCur->pBtree->db->mutex) );

		/* If the cursor already points to the last entry, this is a no-op. */
		if( CURSOR_VALID==pCur->eState && pCur->atLast ){
#ifdef _DEBUG
			/* This block serves to assert() that the cursor really does point 
			** to the last entry in the b-tree. */
			int ii;
			for(ii=0; ii<pCur->iPage; ii++){
				assert( pCur->aiIdx[ii]==pCur->apPage[ii]->nCell );
			}
			assert( pCur->aiIdx[pCur->iPage]==pCur->apPage[pCur->iPage]->nCell-1 );
			assert( pCur->apPage[pCur->iPage]->leaf );
#endif
			return SQLITE_OK;
		}

		rc = moveToRoot(pCur);
		if( rc==SQLITE_OK ){
			if( CURSOR_INVALID==pCur->eState ){
				assert( pCur->pgnoRoot==0 || pCur->apPage[pCur->iPage]->nCell==0 );
				*pRes = 1;
			}else{
				assert( pCur->eState==CURSOR_VALID );
				*pRes = 0;
				rc = moveToRightmost(pCur);
				pCur->atLast = rc==SQLITE_OK ?1:0;
			}
		}
		return rc;
	}

	int sqlite3BtreeMovetoUnpacked(
		BtCursor *pCur,          /* The cursor to be moved */
		UnpackedRecord *pIdxKey, /* Unpacked index key */
		i64 intKey,              /* The table key */
		int biasRight,           /* If true, bias the search to the high end */
		int *pRes                /* Write search results here */
		){
			int rc;

			assert( cursorHoldsMutex(pCur) );
			assert( sqlite3_mutex_held(pCur->pBtree->db->mutex) );
			assert( pRes );
			assert( (pIdxKey==0)==(pCur->pKeyInfo==0) );

			/* If the cursor is already positioned at the point we are trying
			** to move to, then just return without doing any work */
			if( pCur->eState==CURSOR_VALID && pCur->validNKey 
				&& pCur->apPage[0]->intKey 
				){
					if( pCur->info.nKey==intKey ){
						*pRes = 0;
						return SQLITE_OK;
					}
					if( pCur->atLast && pCur->info.nKey<intKey ){
						*pRes = -1;
						return SQLITE_OK;
					}
			}

			rc = moveToRoot(pCur);
			if( rc ){
				return rc;
			}
			assert( pCur->pgnoRoot==0 || pCur->apPage[pCur->iPage] );
			assert( pCur->pgnoRoot==0 || pCur->apPage[pCur->iPage]->isInit );
			assert( pCur->eState==CURSOR_INVALID || pCur->apPage[pCur->iPage]->nCell>0 );
			if( pCur->eState==CURSOR_INVALID ){
				*pRes = -1;
				assert( pCur->pgnoRoot==0 || pCur->apPage[pCur->iPage]->nCell==0 );
				return SQLITE_OK;
			}
			assert( pCur->apPage[0]->intKey || pIdxKey );
			for(;;){
				int lwr, upr, idx;
				Pgno chldPg;
				MemPage *pPage = pCur->apPage[pCur->iPage];
				int c;

				/* pPage->nCell must be greater than zero. If this is the root-page
				** the cursor would have been INVALID above and this for(;;) loop
				** not run. If this is not the root-page, then the moveToChild() routine
				** would have already detected db corruption. Similarly, pPage must
				** be the right kind (index or table) of b-tree page. Otherwise
				** a moveToChild() or moveToRoot() call would have detected corruption.  */
				assert( pPage->nCell>0 );
				assert( pPage->intKey==(pIdxKey==0) );
				lwr = 0;
				upr = pPage->nCell-1;
				if( biasRight ){
					pCur->aiIdx[pCur->iPage] = (u16)(idx = upr);
				}else{
					pCur->aiIdx[pCur->iPage] = (u16)(idx = (upr+lwr)/2);
				}
				for(;;){
					u8 *pCell;                          /* Pointer to current cell in pPage */

					assert( idx==pCur->aiIdx[pCur->iPage] );
					pCur->info.nSize = 0;
					pCell = findCell(pPage, idx) + pPage->childPtrSize;
					if( pPage->intKey ){
						i64 nCellKey;
						if( pPage->hasData ){
							u32 dummy;
							pCell += getVarint32(pCell, dummy);
						}
						getVarint(pCell, (u64*)&nCellKey);
						if( nCellKey==intKey ){
							c = 0;
						}else if( nCellKey<intKey ){
							c = -1;
						}else{
							assert( nCellKey>intKey );
							c = +1;
						}
						pCur->validNKey = 1;
						pCur->info.nKey = nCellKey;
					}else{
						/* The maximum supported page-size is 65536 bytes. This means that
						** the maximum number of record bytes stored on an index B-Tree
						** page is less than 16384 bytes and may be stored as a 2-byte
						** varint. This information is used to attempt to avoid parsing 
						** the entire cell by checking for the cases where the record is 
						** stored entirely within the b-tree page by inspecting the first 
						** 2 bytes of the cell.
						*/
						int nCell = pCell[0];
						if( nCell<=pPage->max1bytePayload
							/* && (pCell+nCell)<pPage->aDataEnd */
								){
									/* This branch runs if the record-size field of the cell is a
									** single byte varint and the record fits entirely on the main
									** b-tree page.  */
									testcase( pCell+nCell+1==pPage->aDataEnd );
									c = sqlite3VdbeRecordCompare(nCell, (void*)&pCell[1], pIdxKey);
						}else if( !(pCell[1] & 0x80) 
							&& (nCell = ((nCell&0x7f)<<7) + pCell[1])<=pPage->maxLocal
							/* && (pCell+nCell+2)<=pPage->aDataEnd */
							){
								/* The record-size field is a 2 byte varint and the record 
								** fits entirely on the main b-tree page.  */
								testcase( pCell+nCell+2==pPage->aDataEnd );
								c = sqlite3VdbeRecordCompare(nCell, (void*)&pCell[2], pIdxKey);
						}else{
							/* The record flows over onto one or more overflow pages. In
							** this case the whole cell needs to be parsed, a buffer allocated
							** and accessPayload() used to retrieve the record into the
							** buffer before VdbeRecordCompare() can be called. */
							void *pCellKey;
							u8 * const pCellBody = pCell - pPage->childPtrSize;
							btreeParseCellPtr(pPage, pCellBody, &pCur->info);
							nCell = (int)pCur->info.nKey;
							pCellKey = sqlite3Malloc( nCell );
							if( pCellKey==0 ){
								rc = SQLITE_NOMEM;
								goto moveto_finish;
							}
							rc = accessPayload(pCur, 0, nCell, (unsigned char*)pCellKey, 0);
							if( rc ){
								sqlite3_free(pCellKey);
								goto moveto_finish;
							}
							c = sqlite3VdbeRecordCompare(nCell, pCellKey, pIdxKey);
							sqlite3_free(pCellKey);
						}
					}
					if( c==0 ){
						if( pPage->intKey && !pPage->leaf ){
							lwr = idx;
							break;
						}else{
							*pRes = 0;
							rc = SQLITE_OK;
							goto moveto_finish;
						}
					}
					if( c<0 ){
						lwr = idx+1;
					}else{
						upr = idx-1;
					}
					if( lwr>upr ){
						break;
					}
					pCur->aiIdx[pCur->iPage] = (u16)(idx = (lwr+upr)/2);
				}
				assert( lwr==upr+1 || (pPage->intKey && !pPage->leaf) );
				assert( pPage->isInit );
				if( pPage->leaf ){
					chldPg = 0;
				}else if( lwr>=pPage->nCell ){
					chldPg = get4byte(&pPage->aData[pPage->hdrOffset+8]);
				}else{
					chldPg = get4byte(findCell(pPage, lwr));
				}
				if( chldPg==0 ){
					assert( pCur->aiIdx[pCur->iPage]<pCur->apPage[pCur->iPage]->nCell );
					*pRes = c;
					rc = SQLITE_OK;
					goto moveto_finish;
				}
				pCur->aiIdx[pCur->iPage] = (u16)lwr;
				pCur->info.nSize = 0;
				pCur->validNKey = 0;
				rc = moveToChild(pCur, chldPg);
				if( rc ) goto moveto_finish;
			}
moveto_finish:
			return rc;
	}

	int sqlite3BtreeEof(BtCursor *pCur){
		/* TODO: What if the cursor is in CURSOR_REQUIRESEEK but all table entries
		** have been deleted? This API will need to change to return an error code
		** as well as the boolean result value.
		*/
		return (CURSOR_VALID!=pCur->eState);
	}

	int sqlite3BtreeNext(BtCursor *pCur, int *pRes){
		int rc;
		int idx;
		MemPage *pPage;

		assert( cursorHoldsMutex(pCur) );
		rc = restoreCursorPosition(pCur);
		if( rc!=SQLITE_OK ){
			return rc;
		}
		assert( pRes!=0 );
		if( CURSOR_INVALID==pCur->eState ){
			*pRes = 1;
			return SQLITE_OK;
		}
		if( pCur->skipNext>0 ){
			pCur->skipNext = 0;
			*pRes = 0;
			return SQLITE_OK;
		}
		pCur->skipNext = 0;

		pPage = pCur->apPage[pCur->iPage];
		idx = ++pCur->aiIdx[pCur->iPage];
		assert( pPage->isInit );

		/* If the database file is corrupt, it is possible for the value of idx 
		** to be invalid here. This can only occur if a second cursor modifies
		** the page while cursor pCur is holding a reference to it. Which can
		** only happen if the database is corrupt in such a way as to link the
		** page into more than one b-tree structure. */
		testcase( idx>pPage->nCell );

		pCur->info.nSize = 0;
		pCur->validNKey = 0;
		if( idx>=pPage->nCell ){
			if( !pPage->leaf ){
				rc = moveToChild(pCur, get4byte(&pPage->aData[pPage->hdrOffset+8]));
				if( rc ) return rc;
				rc = moveToLeftmost(pCur);
				*pRes = 0;
				return rc;
			}
			do{
				if( pCur->iPage==0 ){
					*pRes = 1;
					pCur->eState = CURSOR_INVALID;
					return SQLITE_OK;
				}
				moveToParent(pCur);
				pPage = pCur->apPage[pCur->iPage];
			}while( pCur->aiIdx[pCur->iPage]>=pPage->nCell );
			*pRes = 0;
			if( pPage->intKey ){
				rc = sqlite3BtreeNext(pCur, pRes);
			}else{
				rc = SQLITE_OK;
			}
			return rc;
		}
		*pRes = 0;
		if( pPage->leaf ){
			return SQLITE_OK;
		}
		rc = moveToLeftmost(pCur);
		return rc;
	}

	int sqlite3BtreePrevious(BtCursor *pCur, int *pRes){
		int rc;
		MemPage *pPage;

		assert( cursorHoldsMutex(pCur) );
		rc = restoreCursorPosition(pCur);
		if( rc!=SQLITE_OK ){
			return rc;
		}
		pCur->atLast = 0;
		if( CURSOR_INVALID==pCur->eState ){
			*pRes = 1;
			return SQLITE_OK;
		}
		if( pCur->skipNext<0 ){
			pCur->skipNext = 0;
			*pRes = 0;
			return SQLITE_OK;
		}
		pCur->skipNext = 0;

		pPage = pCur->apPage[pCur->iPage];
		assert( pPage->isInit );
		if( !pPage->leaf ){
			int idx = pCur->aiIdx[pCur->iPage];
			rc = moveToChild(pCur, get4byte(findCell(pPage, idx)));
			if( rc ){
				return rc;
			}
			rc = moveToRightmost(pCur);
		}else{
			while( pCur->aiIdx[pCur->iPage]==0 ){
				if( pCur->iPage==0 ){
					pCur->eState = CURSOR_INVALID;
					*pRes = 1;
					return SQLITE_OK;
				}
				moveToParent(pCur);
			}
			pCur->info.nSize = 0;
			pCur->validNKey = 0;

			pCur->aiIdx[pCur->iPage]--;
			pPage = pCur->apPage[pCur->iPage];
			if( pPage->intKey && !pPage->leaf ){
				rc = sqlite3BtreePrevious(pCur, pRes);
			}else{
				rc = SQLITE_OK;
			}
		}
		*pRes = 0;
		return rc;
	}

#pragma endregion

#pragma region Allocate Page

	static int allocateBtreePage(
		BtShared *pBt,         /* The btree */
		MemPage **ppPage,      /* Store pointer to the allocated page here */
		Pgno *pPgno,           /* Store the page number here */
		Pgno nearby,           /* Search for a page near this one */
		u8 eMode               /* BTALLOC_EXACT, BTALLOC_LT, or BTALLOC_ANY */
		){
			MemPage *pPage1;
			int rc;
			u32 n;     /* Number of pages on the freelist */
			u32 k;     /* Number of leaves on the trunk of the freelist */
			MemPage *pTrunk = 0;
			MemPage *pPrevTrunk = 0;
			Pgno mxPage;     /* Total size of the database file */

			assert( sqlite3_mutex_held(pBt->mutex) );
			assert( eMode==BTALLOC_ANY || (nearby>0 && IfNotOmitAV(pBt->autoVacuum)) );
			pPage1 = pBt->pPage1;
			mxPage = btreePagecount(pBt);
			n = get4byte(&pPage1->aData[36]);
			testcase( n==mxPage-1 );
			if( n>=mxPage ){
				return SQLITE_CORRUPT_BKPT;
			}
			if( n>0 ){
				/* There are pages on the freelist.  Reuse one of those pages. */
				Pgno iTrunk;
				u8 searchList = 0; /* If the free-list must be searched for 'nearby' */

				/* If eMode==BTALLOC_EXACT and a query of the pointer-map
				** shows that the page 'nearby' is somewhere on the free-list, then
				** the entire-list will be searched for that page.
				*/
#ifndef OMIT_AUTOVACUUM
				if( eMode==BTALLOC_EXACT ){
					if( nearby<=mxPage ){
						u8 eType;
						assert( nearby>0 );
						assert( pBt->autoVacuum );
						rc = ptrmapGet(pBt, nearby, &eType, 0);
						if( rc ) return rc;
						if( eType==PTRMAP_FREEPAGE ){
							searchList = 1;
						}
					}
				}else if( eMode==BTALLOC_LE ){
					searchList = 1;
				}
#endif

				/* Decrement the free-list count by 1. Set iTrunk to the index of the
				** first free-list trunk page. iPrevTrunk is initially 1.
				*/
				rc = sqlite3PagerWrite(pPage1->pDbPage);
				if( rc ) return rc;
				put4byte(&pPage1->aData[36], n-1);

				/* The code within this loop is run only once if the 'searchList' variable
				** is not true. Otherwise, it runs once for each trunk-page on the
				** free-list until the page 'nearby' is located (eMode==BTALLOC_EXACT)
				** or until a page less than 'nearby' is located (eMode==BTALLOC_LT)
				*/
				do {
					pPrevTrunk = pTrunk;
					if( pPrevTrunk ){
						iTrunk = get4byte(&pPrevTrunk->aData[0]);
					}else{
						iTrunk = get4byte(&pPage1->aData[32]);
					}
					testcase( iTrunk==mxPage );
					if( iTrunk>mxPage ){
						rc = SQLITE_CORRUPT_BKPT;
					}else{
						rc = btreeGetPage(pBt, iTrunk, &pTrunk, 0);
					}
					if( rc ){
						pTrunk = 0;
						goto end_allocate_page;
					}
					assert( pTrunk!=0 );
					assert( pTrunk->aData!=0 );

					k = get4byte(&pTrunk->aData[4]); /* # of leaves on this trunk page */
					if( k==0 && !searchList ){
						/* The trunk has no leaves and the list is not being searched. 
						** So extract the trunk page itself and use it as the newly 
						** allocated page */
						assert( pPrevTrunk==0 );
						rc = sqlite3PagerWrite(pTrunk->pDbPage);
						if( rc ){
							goto end_allocate_page;
						}
						*pPgno = iTrunk;
						memcpy(&pPage1->aData[32], &pTrunk->aData[0], 4);
						*ppPage = pTrunk;
						pTrunk = 0;
						TRACE(("ALLOCATE: %d trunk - %d free pages left\n", *pPgno, n-1));
					}else if( k>(u32)(pBt->usableSize/4 - 2) ){
						/* Value of k is out of range.  Database corruption */
						rc = SQLITE_CORRUPT_BKPT;
						goto end_allocate_page;
#ifndef OMIT_AUTOVACUUM
					}else if( searchList 
						&& (nearby==iTrunk || (iTrunk<nearby && eMode==BTALLOC_LE)) 
						){
							/* The list is being searched and this trunk page is the page
							** to allocate, regardless of whether it has leaves.
							*/
							*pPgno = iTrunk;
							*ppPage = pTrunk;
							searchList = 0;
							rc = sqlite3PagerWrite(pTrunk->pDbPage);
							if( rc ){
								goto end_allocate_page;
							}
							if( k==0 ){
								if( !pPrevTrunk ){
									memcpy(&pPage1->aData[32], &pTrunk->aData[0], 4);
								}else{
									rc = sqlite3PagerWrite(pPrevTrunk->pDbPage);
									if( rc!=SQLITE_OK ){
										goto end_allocate_page;
									}
									memcpy(&pPrevTrunk->aData[0], &pTrunk->aData[0], 4);
								}
							}else{
								/* The trunk page is required by the caller but it contains 
								** pointers to free-list leaves. The first leaf becomes a trunk
								** page in this case.
								*/
								MemPage *pNewTrunk;
								Pgno iNewTrunk = get4byte(&pTrunk->aData[8]);
								if( iNewTrunk>mxPage ){ 
									rc = SQLITE_CORRUPT_BKPT;
									goto end_allocate_page;
								}
								testcase( iNewTrunk==mxPage );
								rc = btreeGetPage(pBt, iNewTrunk, &pNewTrunk, 0);
								if( rc!=SQLITE_OK ){
									goto end_allocate_page;
								}
								rc = sqlite3PagerWrite(pNewTrunk->pDbPage);
								if( rc!=SQLITE_OK ){
									releasePage(pNewTrunk);
									goto end_allocate_page;
								}
								memcpy(&pNewTrunk->aData[0], &pTrunk->aData[0], 4);
								put4byte(&pNewTrunk->aData[4], k-1);
								memcpy(&pNewTrunk->aData[8], &pTrunk->aData[12], (k-1)*4);
								releasePage(pNewTrunk);
								if( !pPrevTrunk ){
									assert( sqlite3PagerIswriteable(pPage1->pDbPage) );
									put4byte(&pPage1->aData[32], iNewTrunk);
								}else{
									rc = sqlite3PagerWrite(pPrevTrunk->pDbPage);
									if( rc ){
										goto end_allocate_page;
									}
									put4byte(&pPrevTrunk->aData[0], iNewTrunk);
								}
							}
							pTrunk = 0;
							TRACE(("ALLOCATE: %d trunk - %d free pages left\n", *pPgno, n-1));
#endif
					}else if( k>0 ){
						/* Extract a leaf from the trunk */
						u32 closest;
						Pgno iPage;
						unsigned char *aData = pTrunk->aData;
						if( nearby>0 ){
							u32 i;
							closest = 0;
							if( eMode==BTALLOC_LE ){
								for(i=0; i<k; i++){
									iPage = get4byte(&aData[8+i*4]);
									if( iPage<=nearby ){
										closest = i;
										break;
									}
								}
							}else{
								int dist;
								dist = sqlite3AbsInt32(get4byte(&aData[8]) - nearby);
								for(i=1; i<k; i++){
									int d2 = sqlite3AbsInt32(get4byte(&aData[8+i*4]) - nearby);
									if( d2<dist ){
										closest = i;
										dist = d2;
									}
								}
							}
						}else{
							closest = 0;
						}

						iPage = get4byte(&aData[8+closest*4]);
						testcase( iPage==mxPage );
						if( iPage>mxPage ){
							rc = SQLITE_CORRUPT_BKPT;
							goto end_allocate_page;
						}
						testcase( iPage==mxPage );
						if( !searchList 
							|| (iPage==nearby || (iPage<nearby && eMode==BTALLOC_LE)) 
							){
								int noContent;
								*pPgno = iPage;
								TRACE(("ALLOCATE: %d was leaf %d of %d on trunk %d"
									": %d more free pages\n",
									*pPgno, closest+1, k, pTrunk->pgno, n-1));
								rc = sqlite3PagerWrite(pTrunk->pDbPage);
								if( rc ) goto end_allocate_page;
								if( closest<k-1 ){
									memcpy(&aData[8+closest*4], &aData[4+k*4], 4);
								}
								put4byte(&aData[4], k-1);
								noContent = !btreeGetHasContent(pBt, *pPgno);
								rc = btreeGetPage(pBt, *pPgno, ppPage, noContent);
								if( rc==SQLITE_OK ){
									rc = sqlite3PagerWrite((*ppPage)->pDbPage);
									if( rc!=SQLITE_OK ){
										releasePage(*ppPage);
									}
								}
								searchList = 0;
						}
					}
					releasePage(pPrevTrunk);
					pPrevTrunk = 0;
				}while( searchList );
			}else{
				/* There are no pages on the freelist, so append a new page to the
				** database image.
				**
				** Normally, new pages allocated by this block can be requested from the
				** pager layer with the 'no-content' flag set. This prevents the pager
				** from trying to read the pages content from disk. However, if the
				** current transaction has already run one or more incremental-vacuum
				** steps, then the page we are about to allocate may contain content
				** that is required in the event of a rollback. In this case, do
				** not set the no-content flag. This causes the pager to load and journal
				** the current page content before overwriting it.
				**
				** Note that the pager will not actually attempt to load or journal 
				** content for any page that really does lie past the end of the database
				** file on disk. So the effects of disabling the no-content optimization
				** here are confined to those pages that lie between the end of the
				** database image and the end of the database file.
				*/
				int bNoContent = (0==IfNotOmitAV(pBt->bDoTruncate));

				rc = sqlite3PagerWrite(pBt->pPage1->pDbPage);
				if( rc ) return rc;
				pBt->nPage++;
				if( pBt->nPage==PENDING_BYTE_PAGE(pBt) ) pBt->nPage++;

#ifndef OMIT_AUTOVACUUM
				if( pBt->autoVacuum && PTRMAP_ISPAGE(pBt, pBt->nPage) ){
					/* If *pPgno refers to a pointer-map page, allocate two new pages
					** at the end of the file instead of one. The first allocated page
					** becomes a new pointer-map page, the second is used by the caller.
					*/
					MemPage *pPg = 0;
					TRACE(("ALLOCATE: %d from end of file (pointer-map page)\n", pBt->nPage));
					assert( pBt->nPage!=PENDING_BYTE_PAGE(pBt) );
					rc = btreeGetPage(pBt, pBt->nPage, &pPg, bNoContent);
					if( rc==SQLITE_OK ){
						rc = sqlite3PagerWrite(pPg->pDbPage);
						releasePage(pPg);
					}
					if( rc ) return rc;
					pBt->nPage++;
					if( pBt->nPage==PENDING_BYTE_PAGE(pBt) ){ pBt->nPage++; }
				}
#endif
				put4byte(28 + (u8*)pBt->pPage1->aData, pBt->nPage);
				*pPgno = pBt->nPage;

				assert( *pPgno!=PENDING_BYTE_PAGE(pBt) );
				rc = btreeGetPage(pBt, *pPgno, ppPage, bNoContent);
				if( rc ) return rc;
				rc = sqlite3PagerWrite((*ppPage)->pDbPage);
				if( rc!=SQLITE_OK ){
					releasePage(*ppPage);
				}
				TRACE(("ALLOCATE: %d from end of file\n", *pPgno));
			}

			assert( *pPgno!=PENDING_BYTE_PAGE(pBt) );

end_allocate_page:
			releasePage(pTrunk);
			releasePage(pPrevTrunk);
			if( rc==SQLITE_OK ){
				if( sqlite3PagerPageRefcount((*ppPage)->pDbPage)>1 ){
					releasePage(*ppPage);
					return SQLITE_CORRUPT_BKPT;
				}
				(*ppPage)->isInit = 0;
			}else{
				*ppPage = 0;
			}
			assert( rc!=SQLITE_OK || sqlite3PagerIswriteable((*ppPage)->pDbPage) );
			return rc;
	}

	static int freePage2(BtShared *pBt, MemPage *pMemPage, Pgno iPage){
		MemPage *pTrunk = 0;                /* Free-list trunk page */
		Pgno iTrunk = 0;                    /* Page number of free-list trunk page */ 
		MemPage *pPage1 = pBt->pPage1;      /* Local reference to page 1 */
		MemPage *pPage;                     /* Page being freed. May be NULL. */
		int rc;                             /* Return Code */
		int nFree;                          /* Initial number of pages on free-list */

		assert( sqlite3_mutex_held(pBt->mutex) );
		assert( iPage>1 );
		assert( !pMemPage || pMemPage->pgno==iPage );

		if( pMemPage ){
			pPage = pMemPage;
			sqlite3PagerRef(pPage->pDbPage);
		}else{
			pPage = btreePageLookup(pBt, iPage);
		}

		/* Increment the free page count on pPage1 */
		rc = sqlite3PagerWrite(pPage1->pDbPage);
		if( rc ) goto freepage_out;
		nFree = get4byte(&pPage1->aData[36]);
		put4byte(&pPage1->aData[36], nFree+1);

		if( pBt->btsFlags & BTS_SECURE_DELETE ){
			/* If the secure_delete option is enabled, then
			** always fully overwrite deleted information with zeros.
			*/
			if( (!pPage && ((rc = btreeGetPage(pBt, iPage, &pPage, 0))!=0) )
				||            ((rc = sqlite3PagerWrite(pPage->pDbPage))!=0)
				){
					goto freepage_out;
			}
			memset(pPage->aData, 0, pPage->pBt->pageSize);
		}

		/* If the database supports auto-vacuum, write an entry in the pointer-map
		** to indicate that the page is free.
		*/
		if( ISAUTOVACUUM ){
			ptrmapPut(pBt, iPage, PTRMAP_FREEPAGE, 0, &rc);
			if( rc ) goto freepage_out;
		}

		/* Now manipulate the actual database free-list structure. There are two
		** possibilities. If the free-list is currently empty, or if the first
		** trunk page in the free-list is full, then this page will become a
		** new free-list trunk page. Otherwise, it will become a leaf of the
		** first trunk page in the current free-list. This block tests if it
		** is possible to add the page as a new free-list leaf.
		*/
		if( nFree!=0 ){
			u32 nLeaf;                /* Initial number of leaf cells on trunk page */

			iTrunk = get4byte(&pPage1->aData[32]);
			rc = btreeGetPage(pBt, iTrunk, &pTrunk, 0);
			if( rc!=SQLITE_OK ){
				goto freepage_out;
			}

			nLeaf = get4byte(&pTrunk->aData[4]);
			assert( pBt->usableSize>32 );
			if( nLeaf > (u32)pBt->usableSize/4 - 2 ){
				rc = SQLITE_CORRUPT_BKPT;
				goto freepage_out;
			}
			if( nLeaf < (u32)pBt->usableSize/4 - 8 ){
				/* In this case there is room on the trunk page to insert the page
				** being freed as a new leaf.
				**
				** Note that the trunk page is not really full until it contains
				** usableSize/4 - 2 entries, not usableSize/4 - 8 entries as we have
				** coded.  But due to a coding error in versions of SQLite prior to
				** 3.6.0, databases with freelist trunk pages holding more than
				** usableSize/4 - 8 entries will be reported as corrupt.  In order
				** to maintain backwards compatibility with older versions of SQLite,
				** we will continue to restrict the number of entries to usableSize/4 - 8
				** for now.  At some point in the future (once everyone has upgraded
				** to 3.6.0 or later) we should consider fixing the conditional above
				** to read "usableSize/4-2" instead of "usableSize/4-8".
				*/
				rc = sqlite3PagerWrite(pTrunk->pDbPage);
				if( rc==SQLITE_OK ){
					put4byte(&pTrunk->aData[4], nLeaf+1);
					put4byte(&pTrunk->aData[8+nLeaf*4], iPage);
					if( pPage && (pBt->btsFlags & BTS_SECURE_DELETE)==0 ){
						sqlite3PagerDontWrite(pPage->pDbPage);
					}
					rc = btreeSetHasContent(pBt, iPage);
				}
				TRACE(("FREE-PAGE: %d leaf on trunk page %d\n",pPage->pgno,pTrunk->pgno));
				goto freepage_out;
			}
		}

		/* If control flows to this point, then it was not possible to add the
		** the page being freed as a leaf page of the first trunk in the free-list.
		** Possibly because the free-list is empty, or possibly because the 
		** first trunk in the free-list is full. Either way, the page being freed
		** will become the new first trunk page in the free-list.
		*/
		if( pPage==0 && SQLITE_OK!=(rc = btreeGetPage(pBt, iPage, &pPage, 0)) ){
			goto freepage_out;
		}
		rc = sqlite3PagerWrite(pPage->pDbPage);
		if( rc!=SQLITE_OK ){
			goto freepage_out;
		}
		put4byte(pPage->aData, iTrunk);
		put4byte(&pPage->aData[4], 0);
		put4byte(&pPage1->aData[32], iPage);
		TRACE(("FREE-PAGE: %d new trunk page replacing %d\n", pPage->pgno, iTrunk));

freepage_out:
		if( pPage ){
			pPage->isInit = 0;
		}
		releasePage(pPage);
		releasePage(pTrunk);
		return rc;
	}
	static void freePage(MemPage *pPage, int *pRC){
		if( (*pRC)==SQLITE_OK ){
			*pRC = freePage2(pPage->pBt, pPage, pPage->pgno);
		}
	}

	static int clearCell(MemPage *pPage, unsigned char *pCell){
		BtShared *pBt = pPage->pBt;
		CellInfo info;
		Pgno ovflPgno;
		int rc;
		int nOvfl;
		u32 ovflPageSize;

		assert( sqlite3_mutex_held(pPage->pBt->mutex) );
		btreeParseCellPtr(pPage, pCell, &info);
		if( info.iOverflow==0 ){
			return SQLITE_OK;  /* No overflow pages. Return without doing anything */
		}
		if( pCell+info.iOverflow+3 > pPage->aData+pPage->maskPage ){
			return SQLITE_CORRUPT_BKPT;  /* Cell extends past end of page */
		}
		ovflPgno = get4byte(&pCell[info.iOverflow]);
		assert( pBt->usableSize > 4 );
		ovflPageSize = pBt->usableSize - 4;
		nOvfl = (info.nPayload - info.nLocal + ovflPageSize - 1)/ovflPageSize;
		assert( ovflPgno==0 || nOvfl>0 );
		while( nOvfl-- ){
			Pgno iNext = 0;
			MemPage *pOvfl = 0;
			if( ovflPgno<2 || ovflPgno>btreePagecount(pBt) ){
				/* 0 is not a legal page number and page 1 cannot be an 
				** overflow page. Therefore if ovflPgno<2 or past the end of the 
				** file the database must be corrupt. */
				return SQLITE_CORRUPT_BKPT;
			}
			if( nOvfl ){
				rc = getOverflowPage(pBt, ovflPgno, &pOvfl, &iNext);
				if( rc ) return rc;
			}

			if( ( pOvfl || ((pOvfl = btreePageLookup(pBt, ovflPgno))!=0) )
				&& sqlite3PagerPageRefcount(pOvfl->pDbPage)!=1
				){
					/* There is no reason any cursor should have an outstanding reference 
					** to an overflow page belonging to a cell that is being deleted/updated.
					** So if there exists more than one reference to this page, then it 
					** must not really be an overflow page and the database must be corrupt. 
					** It is helpful to detect this before calling freePage2(), as 
					** freePage2() may zero the page contents if secure-delete mode is
					** enabled. If this 'overflow' page happens to be a page that the
					** caller is iterating through or using in some other way, this
					** can be problematic.
					*/
					rc = SQLITE_CORRUPT_BKPT;
			}else{
				rc = freePage2(pBt, pOvfl, ovflPgno);
			}

			if( pOvfl ){
				sqlite3PagerUnref(pOvfl->pDbPage);
			}
			if( rc ) return rc;
			ovflPgno = iNext;
		}
		return SQLITE_OK;
	}

	static int fillInCell(
		MemPage *pPage,                /* The page that contains the cell */
		unsigned char *pCell,          /* Complete text of the cell */
		const void *pKey, i64 nKey,    /* The key */
		const void *pData,int nData,   /* The data */
		int nZero,                     /* Extra zero bytes to append to pData */
		int *pnSize                    /* Write cell size here */
		){
			int nPayload;
			const u8 *pSrc;
			int nSrc, n, rc;
			int spaceLeft;
			MemPage *pOvfl = 0;
			MemPage *pToRelease = 0;
			unsigned char *pPrior;
			unsigned char *pPayload;
			BtShared *pBt = pPage->pBt;
			Pgno pgnoOvfl = 0;
			int nHeader;
			CellInfo info;

			assert( sqlite3_mutex_held(pPage->pBt->mutex) );

			/* pPage is not necessarily writeable since pCell might be auxiliary
			** buffer space that is separate from the pPage buffer area */
			assert( pCell<pPage->aData || pCell>=&pPage->aData[pBt->pageSize]
			|| sqlite3PagerIswriteable(pPage->pDbPage) );

			/* Fill in the header. */
			nHeader = 0;
			if( !pPage->leaf ){
				nHeader += 4;
			}
			if( pPage->hasData ){
				nHeader += putVarint(&pCell[nHeader], nData+nZero);
			}else{
				nData = nZero = 0;
			}
			nHeader += putVarint(&pCell[nHeader], *(u64*)&nKey);
			btreeParseCellPtr(pPage, pCell, &info);
			assert( info.nHeader==nHeader );
			assert( info.nKey==nKey );
			assert( info.nData==(u32)(nData+nZero) );

			/* Fill in the payload */
			nPayload = nData + nZero;
			if( pPage->intKey ){
				pSrc = pData;
				nSrc = nData;
				nData = 0;
			}else{ 
				if( NEVER(nKey>0x7fffffff || pKey==0) ){
					return SQLITE_CORRUPT_BKPT;
				}
				nPayload += (int)nKey;
				pSrc = pKey;
				nSrc = (int)nKey;
			}
			*pnSize = info.nSize;
			spaceLeft = info.nLocal;
			pPayload = &pCell[nHeader];
			pPrior = &pCell[info.iOverflow];

			while( nPayload>0 ){
				if( spaceLeft==0 ){
#ifndef OMIT_AUTOVACUUM
					Pgno pgnoPtrmap = pgnoOvfl; /* Overflow page pointer-map entry page */
					if( pBt->autoVacuum ){
						do{
							pgnoOvfl++;
						} while( 
							PTRMAP_ISPAGE(pBt, pgnoOvfl) || pgnoOvfl==PENDING_BYTE_PAGE(pBt) 
							);
					}
#endif
					rc = allocateBtreePage(pBt, &pOvfl, &pgnoOvfl, pgnoOvfl, 0);
#ifndef OMIT_AUTOVACUUM
					/* If the database supports auto-vacuum, and the second or subsequent
					** overflow page is being allocated, add an entry to the pointer-map
					** for that page now. 
					**
					** If this is the first overflow page, then write a partial entry 
					** to the pointer-map. If we write nothing to this pointer-map slot,
					** then the optimistic overflow chain processing in clearCell()
					** may misinterpret the uninitialized values and delete the
					** wrong pages from the database.
					*/
					if( pBt->autoVacuum && rc==SQLITE_OK ){
						u8 eType = (pgnoPtrmap?PTRMAP_OVERFLOW2:PTRMAP_OVERFLOW1);
						ptrmapPut(pBt, pgnoOvfl, eType, pgnoPtrmap, &rc);
						if( rc ){
							releasePage(pOvfl);
						}
					}
#endif
					if( rc ){
						releasePage(pToRelease);
						return rc;
					}

					/* If pToRelease is not zero than pPrior points into the data area
					** of pToRelease.  Make sure pToRelease is still writeable. */
					assert( pToRelease==0 || sqlite3PagerIswriteable(pToRelease->pDbPage) );

					/* If pPrior is part of the data area of pPage, then make sure pPage
					** is still writeable */
					assert( pPrior<pPage->aData || pPrior>=&pPage->aData[pBt->pageSize]
					|| sqlite3PagerIswriteable(pPage->pDbPage) );

					put4byte(pPrior, pgnoOvfl);
					releasePage(pToRelease);
					pToRelease = pOvfl;
					pPrior = pOvfl->aData;
					put4byte(pPrior, 0);
					pPayload = &pOvfl->aData[4];
					spaceLeft = pBt->usableSize - 4;
				}
				n = nPayload;
				if( n>spaceLeft ) n = spaceLeft;

				/* If pToRelease is not zero than pPayload points into the data area
				** of pToRelease.  Make sure pToRelease is still writeable. */
				assert( pToRelease==0 || sqlite3PagerIswriteable(pToRelease->pDbPage) );

				/* If pPayload is part of the data area of pPage, then make sure pPage
				** is still writeable */
				assert( pPayload<pPage->aData || pPayload>=&pPage->aData[pBt->pageSize]
				|| sqlite3PagerIswriteable(pPage->pDbPage) );

				if( nSrc>0 ){
					if( n>nSrc ) n = nSrc;
					assert( pSrc );
					memcpy(pPayload, pSrc, n);
				}else{
					memset(pPayload, 0, n);
				}
				nPayload -= n;
				pPayload += n;
				pSrc += n;
				nSrc -= n;
				spaceLeft -= n;
				if( nSrc==0 ){
					nSrc = nData;
					pSrc = pData;
				}
			}
			releasePage(pToRelease);
			return SQLITE_OK;
	}

	static void dropCell(MemPage *pPage, int idx, int sz, int *pRC){
		u32 pc;         /* Offset to cell content of cell being deleted */
		u8 *data;       /* pPage->aData */
		u8 *ptr;        /* Used to move bytes around within data[] */
		u8 *endPtr;     /* End of loop */
		int rc;         /* The return code */
		int hdr;        /* Beginning of the header.  0 most pages.  100 page 1 */

		if( *pRC ) return;

		assert( idx>=0 && idx<pPage->nCell );
		assert( sz==cellSize(pPage, idx) );
		assert( sqlite3PagerIswriteable(pPage->pDbPage) );
		assert( sqlite3_mutex_held(pPage->pBt->mutex) );
		data = pPage->aData;
		ptr = &pPage->aCellIdx[2*idx];
		pc = get2byte(ptr);
		hdr = pPage->hdrOffset;
		testcase( pc==get2byte(&data[hdr+5]) );
		testcase( pc+sz==pPage->pBt->usableSize );
		if( pc < (u32)get2byte(&data[hdr+5]) || pc+sz > pPage->pBt->usableSize ){
			*pRC = SQLITE_CORRUPT_BKPT;
			return;
		}
		rc = freeSpace(pPage, pc, sz);
		if( rc ){
			*pRC = rc;
			return;
		}
		endPtr = &pPage->aCellIdx[2*pPage->nCell - 2];
		assert( (SQLITE_PTR_TO_INT(ptr)&1)==0 );  /* ptr is always 2-byte aligned */
		while( ptr<endPtr ){
			*(u16*)ptr = *(u16*)&ptr[2];
			ptr += 2;
		}
		pPage->nCell--;
		put2byte(&data[hdr+3], pPage->nCell);
		pPage->nFree += 2;
	}

	static void insertCell(
		MemPage *pPage,   /* Page into which we are copying */
		int i,            /* New cell becomes the i-th cell of the page */
		u8 *pCell,        /* Content of the new cell */
		int sz,           /* Bytes of content in pCell */
		u8 *pTemp,        /* Temp storage space for pCell, if needed */
		Pgno iChild,      /* If non-zero, replace first 4 bytes with this value */
		int *pRC          /* Read and write return code from here */
		){
			int idx = 0;      /* Where to write new cell content in data[] */
			int j;            /* Loop counter */
			int end;          /* First byte past the last cell pointer in data[] */
			int ins;          /* Index in data[] where new cell pointer is inserted */
			int cellOffset;   /* Address of first cell pointer in data[] */
			u8 *data;         /* The content of the whole page */
			u8 *ptr;          /* Used for moving information around in data[] */
			u8 *endPtr;       /* End of the loop */

			int nSkip = (iChild ? 4 : 0);

			if( *pRC ) return;

			assert( i>=0 && i<=pPage->nCell+pPage->nOverflow );
			assert( pPage->nCell<=MX_CELL(pPage->pBt) && MX_CELL(pPage->pBt)<=10921 );
			assert( pPage->nOverflow<=ArraySize(pPage->apOvfl) );
			assert( ArraySize(pPage->apOvfl)==ArraySize(pPage->aiOvfl) );
			assert( sqlite3_mutex_held(pPage->pBt->mutex) );
			/* The cell should normally be sized correctly.  However, when moving a
			** malformed cell from a leaf page to an interior page, if the cell size
			** wanted to be less than 4 but got rounded up to 4 on the leaf, then size
			** might be less than 8 (leaf-size + pointer) on the interior node.  Hence
			** the term after the || in the following assert(). */
			assert( sz==cellSizePtr(pPage, pCell) || (sz==8 && iChild>0) );
			if( pPage->nOverflow || sz+2>pPage->nFree ){
				if( pTemp ){
					memcpy(pTemp+nSkip, pCell+nSkip, sz-nSkip);
					pCell = pTemp;
				}
				if( iChild ){
					put4byte(pCell, iChild);
				}
				j = pPage->nOverflow++;
				assert( j<(int)(sizeof(pPage->apOvfl)/sizeof(pPage->apOvfl[0])) );
				pPage->apOvfl[j] = pCell;
				pPage->aiOvfl[j] = (u16)i;
			}else{
				int rc = sqlite3PagerWrite(pPage->pDbPage);
				if( rc!=SQLITE_OK ){
					*pRC = rc;
					return;
				}
				assert( sqlite3PagerIswriteable(pPage->pDbPage) );
				data = pPage->aData;
				cellOffset = pPage->cellOffset;
				end = cellOffset + 2*pPage->nCell;
				ins = cellOffset + 2*i;
				rc = allocateSpace(pPage, sz, &idx);
				if( rc ){ *pRC = rc; return; }
				/* The allocateSpace() routine guarantees the following two properties
				** if it returns success */
				assert( idx >= end+2 );
				assert( idx+sz <= (int)pPage->pBt->usableSize );
				pPage->nCell++;
				pPage->nFree -= (u16)(2 + sz);
				memcpy(&data[idx+nSkip], pCell+nSkip, sz-nSkip);
				if( iChild ){
					put4byte(&data[idx], iChild);
				}
				ptr = &data[end];
				endPtr = &data[ins];
				assert( (SQLITE_PTR_TO_INT(ptr)&1)==0 );  /* ptr is always 2-byte aligned */
				while( ptr>endPtr ){
					*(u16*)ptr = *(u16*)&ptr[-2];
					ptr -= 2;
				}
				put2byte(&data[ins], idx);
				put2byte(&data[pPage->hdrOffset+3], pPage->nCell);
#ifndef OMIT_AUTOVACUUM
				if( pPage->pBt->autoVacuum ){
					/* The cell may contain a pointer to an overflow page. If so, write
					** the entry for the overflow page into the pointer map.
					*/
					ptrmapPutOvflPtr(pPage, pCell, pRC);
				}
#endif
			}
	}

	static void assemblePage(
		MemPage *pPage,   /* The page to be assemblied */
		int nCell,        /* The number of cells to add to this page */
		u8 **apCell,      /* Pointers to cell bodies */
		u16 *aSize        /* Sizes of the cells */
		){
			int i;            /* Loop counter */
			u8 *pCellptr;     /* Address of next cell pointer */
			int cellbody;     /* Address of next cell body */
			u8 * const data = pPage->aData;             /* Pointer to data for pPage */
			const int hdr = pPage->hdrOffset;           /* Offset of header on pPage */
			const int nUsable = pPage->pBt->usableSize; /* Usable size of page */

			assert( pPage->nOverflow==0 );
			assert( sqlite3_mutex_held(pPage->pBt->mutex) );
			assert( nCell>=0 && nCell<=(int)MX_CELL(pPage->pBt)
				&& (int)MX_CELL(pPage->pBt)<=10921);
			assert( sqlite3PagerIswriteable(pPage->pDbPage) );

			/* Check that the page has just been zeroed by zeroPage() */
			assert( pPage->nCell==0 );
			assert( get2byteNotZero(&data[hdr+5])==nUsable );

			pCellptr = &pPage->aCellIdx[nCell*2];
			cellbody = nUsable;
			for(i=nCell-1; i>=0; i--){
				u16 sz = aSize[i];
				pCellptr -= 2;
				cellbody -= sz;
				put2byte(pCellptr, cellbody);
				memcpy(&data[cellbody], apCell[i], sz);
			}
			put2byte(&data[hdr+3], nCell);
			put2byte(&data[hdr+5], cellbody);
			pPage->nFree -= (nCell*2 + nUsable - cellbody);
			pPage->nCell = (u16)nCell;
	}

#pragma endregion

#pragma region Balance

#define NN 1             /* Number of neighbors on either side of pPage */
#define NB (NN*2+1)      /* Total pages involved in the balance */

#ifndef OMIT_QUICKBALANCE
	static int balance_quick(MemPage *pParent, MemPage *pPage, u8 *pSpace){
		BtShared *const pBt = pPage->pBt;    /* B-Tree Database */
		MemPage *pNew;                       /* Newly allocated page */
		int rc;                              /* Return Code */
		Pgno pgnoNew;                        /* Page number of pNew */

		assert( sqlite3_mutex_held(pPage->pBt->mutex) );
		assert( sqlite3PagerIswriteable(pParent->pDbPage) );
		assert( pPage->nOverflow==1 );

		/* This error condition is now caught prior to reaching this function */
		if( pPage->nCell==0 ) return SQLITE_CORRUPT_BKPT;

		/* Allocate a new page. This page will become the right-sibling of 
		** pPage. Make the parent page writable, so that the new divider cell
		** may be inserted. If both these operations are successful, proceed.
		*/
		rc = allocateBtreePage(pBt, &pNew, &pgnoNew, 0, 0);

		if( rc==SQLITE_OK ){

			u8 *pOut = &pSpace[4];
			u8 *pCell = pPage->apOvfl[0];
			u16 szCell = cellSizePtr(pPage, pCell);
			u8 *pStop;

			assert( sqlite3PagerIswriteable(pNew->pDbPage) );
			assert( pPage->aData[0]==(PTF_INTKEY|PTF_LEAFDATA|PTF_LEAF) );
			zeroPage(pNew, PTF_INTKEY|PTF_LEAFDATA|PTF_LEAF);
			assemblePage(pNew, 1, &pCell, &szCell);

			/* If this is an auto-vacuum database, update the pointer map
			** with entries for the new page, and any pointer from the 
			** cell on the page to an overflow page. If either of these
			** operations fails, the return code is set, but the contents
			** of the parent page are still manipulated by thh code below.
			** That is Ok, at this point the parent page is guaranteed to
			** be marked as dirty. Returning an error code will cause a
			** rollback, undoing any changes made to the parent page.
			*/
			if( ISAUTOVACUUM ){
				ptrmapPut(pBt, pgnoNew, PTRMAP_BTREE, pParent->pgno, &rc);
				if( szCell>pNew->minLocal ){
					ptrmapPutOvflPtr(pNew, pCell, &rc);
				}
			}

			/* Create a divider cell to insert into pParent. The divider cell
			** consists of a 4-byte page number (the page number of pPage) and
			** a variable length key value (which must be the same value as the
			** largest key on pPage).
			**
			** To find the largest key value on pPage, first find the right-most 
			** cell on pPage. The first two fields of this cell are the 
			** record-length (a variable length integer at most 32-bits in size)
			** and the key value (a variable length integer, may have any value).
			** The first of the while(...) loops below skips over the record-length
			** field. The second while(...) loop copies the key value from the
			** cell on pPage into the pSpace buffer.
			*/
			pCell = findCell(pPage, pPage->nCell-1);
			pStop = &pCell[9];
			while( (*(pCell++)&0x80) && pCell<pStop );
			pStop = &pCell[9];
			while( ((*(pOut++) = *(pCell++))&0x80) && pCell<pStop );

			/* Insert the new divider cell into pParent. */
			insertCell(pParent, pParent->nCell, pSpace, (int)(pOut-pSpace),
				0, pPage->pgno, &rc);

			/* Set the right-child pointer of pParent to point to the new page. */
			put4byte(&pParent->aData[pParent->hdrOffset+8], pgnoNew);

			/* Release the reference to the new page. */
			releasePage(pNew);
		}

		return rc;
	}
#endif

#if 0
	static int ptrmapCheckPages(MemPage **apPage, int nPage){
		int i, j;
		for(i=0; i<nPage; i++){
			Pgno n;
			u8 e;
			MemPage *pPage = apPage[i];
			BtShared *pBt = pPage->pBt;
			assert( pPage->isInit );

			for(j=0; j<pPage->nCell; j++){
				CellInfo info;
				u8 *z;

				z = findCell(pPage, j);
				btreeParseCellPtr(pPage, z, &info);
				if( info.iOverflow ){
					Pgno ovfl = get4byte(&z[info.iOverflow]);
					ptrmapGet(pBt, ovfl, &e, &n);
					assert( n==pPage->pgno && e==PTRMAP_OVERFLOW1 );
				}
				if( !pPage->leaf ){
					Pgno child = get4byte(z);
					ptrmapGet(pBt, child, &e, &n);
					assert( n==pPage->pgno && e==PTRMAP_BTREE );
				}
			}
			if( !pPage->leaf ){
				Pgno child = get4byte(&pPage->aData[pPage->hdrOffset+8]);
				ptrmapGet(pBt, child, &e, &n);
				assert( n==pPage->pgno && e==PTRMAP_BTREE );
			}
		}
		return 1;
	}
#endif

	static void copyNodeContent(MemPage *pFrom, MemPage *pTo, int *pRC){
		if( (*pRC)==SQLITE_OK ){
			BtShared * const pBt = pFrom->pBt;
			u8 * const aFrom = pFrom->aData;
			u8 * const aTo = pTo->aData;
			int const iFromHdr = pFrom->hdrOffset;
			int const iToHdr = ((pTo->pgno==1) ? 100 : 0);
			int rc;
			int iData;


			assert( pFrom->isInit );
			assert( pFrom->nFree>=iToHdr );
			assert( get2byte(&aFrom[iFromHdr+5]) <= (int)pBt->usableSize );

			/* Copy the b-tree node content from page pFrom to page pTo. */
			iData = get2byte(&aFrom[iFromHdr+5]);
			memcpy(&aTo[iData], &aFrom[iData], pBt->usableSize-iData);
			memcpy(&aTo[iToHdr], &aFrom[iFromHdr], pFrom->cellOffset + 2*pFrom->nCell);

			/* Reinitialize page pTo so that the contents of the MemPage structure
			** match the new data. The initialization of pTo can actually fail under
			** fairly obscure circumstances, even though it is a copy of initialized 
			** page pFrom.
			*/
			pTo->isInit = 0;
			rc = btreeInitPage(pTo);
			if( rc!=SQLITE_OK ){
				*pRC = rc;
				return;
			}

			/* If this is an auto-vacuum database, update the pointer-map entries
			** for any b-tree or overflow pages that pTo now contains the pointers to.
			*/
			if( ISAUTOVACUUM ){
				*pRC = setChildPtrmaps(pTo);
			}
		}
	}

#if defined(_MSC_VER) && _MSC_VER >= 1700 && defined(_M_ARM)
#pragma optimize("", off)
#endif
	static int balance_nonroot(
		MemPage *pParent,               /* Parent page of siblings being balanced */
		int iParentIdx,                 /* Index of "the page" in pParent */
		u8 *aOvflSpace,                 /* page-size bytes of space for parent ovfl */
		int isRoot,                     /* True if pParent is a root-page */
		int bBulk                       /* True if this call is part of a bulk load */
		){
			BtShared *pBt;               /* The whole database */
			int nCell = 0;               /* Number of cells in apCell[] */
			int nMaxCells = 0;           /* Allocated size of apCell, szCell, aFrom. */
			int nNew = 0;                /* Number of pages in apNew[] */
			int nOld;                    /* Number of pages in apOld[] */
			int i, j, k;                 /* Loop counters */
			int nxDiv;                   /* Next divider slot in pParent->aCell[] */
			int rc = SQLITE_OK;          /* The return code */
			u16 leafCorrection;          /* 4 if pPage is a leaf.  0 if not */
			int leafData;                /* True if pPage is a leaf of a LEAFDATA tree */
			int usableSpace;             /* Bytes in pPage beyond the header */
			int pageFlags;               /* Value of pPage->aData[0] */
			int subtotal;                /* Subtotal of bytes in cells on one page */
			int iSpace1 = 0;             /* First unused byte of aSpace1[] */
			int iOvflSpace = 0;          /* First unused byte of aOvflSpace[] */
			int szScratch;               /* Size of scratch memory requested */
			MemPage *apOld[NB];          /* pPage and up to two siblings */
			MemPage *apCopy[NB];         /* Private copies of apOld[] pages */
			MemPage *apNew[NB+2];        /* pPage and up to NB siblings after balancing */
			u8 *pRight;                  /* Location in parent of right-sibling pointer */
			u8 *apDiv[NB-1];             /* Divider cells in pParent */
			int cntNew[NB+2];            /* Index in aCell[] of cell after i-th page */
			int szNew[NB+2];             /* Combined size of cells place on i-th page */
			u8 **apCell = 0;             /* All cells begin balanced */
			u16 *szCell;                 /* Local size of all cells in apCell[] */
			u8 *aSpace1;                 /* Space for copies of dividers cells */
			Pgno pgno;                   /* Temp var to store a page number in */

			pBt = pParent->pBt;
			assert( sqlite3_mutex_held(pBt->mutex) );
			assert( sqlite3PagerIswriteable(pParent->pDbPage) );

#if 0
			TRACE(("BALANCE: begin page %d child of %d\n", pPage->pgno, pParent->pgno));
#endif

			/* At this point pParent may have at most one overflow cell. And if
			** this overflow cell is present, it must be the cell with 
			** index iParentIdx. This scenario comes about when this function
			** is called (indirectly) from sqlite3BtreeDelete().
			*/
			assert( pParent->nOverflow==0 || pParent->nOverflow==1 );
			assert( pParent->nOverflow==0 || pParent->aiOvfl[0]==iParentIdx );

			if( !aOvflSpace ){
				return SQLITE_NOMEM;
			}

			/* Find the sibling pages to balance. Also locate the cells in pParent 
			** that divide the siblings. An attempt is made to find NN siblings on 
			** either side of pPage. More siblings are taken from one side, however, 
			** if there are fewer than NN siblings on the other side. If pParent
			** has NB or fewer children then all children of pParent are taken.  
			**
			** This loop also drops the divider cells from the parent page. This
			** way, the remainder of the function does not have to deal with any
			** overflow cells in the parent page, since if any existed they will
			** have already been removed.
			*/
			i = pParent->nOverflow + pParent->nCell;
			if( i<2 ){
				nxDiv = 0;
			}else{
				assert( bBulk==0 || bBulk==1 );
				if( iParentIdx==0 ){                 
					nxDiv = 0;
				}else if( iParentIdx==i ){
					nxDiv = i-2+bBulk;
				}else{
					assert( bBulk==0 );
					nxDiv = iParentIdx-1;
				}
				i = 2-bBulk;
			}
			nOld = i+1;
			if( (i+nxDiv-pParent->nOverflow)==pParent->nCell ){
				pRight = &pParent->aData[pParent->hdrOffset+8];
			}else{
				pRight = findCell(pParent, i+nxDiv-pParent->nOverflow);
			}
			pgno = get4byte(pRight);
			while( 1 ){
				rc = getAndInitPage(pBt, pgno, &apOld[i]);
				if( rc ){
					memset(apOld, 0, (i+1)*sizeof(MemPage*));
					goto balance_cleanup;
				}
				nMaxCells += 1+apOld[i]->nCell+apOld[i]->nOverflow;
				if( (i--)==0 ) break;

				if( i+nxDiv==pParent->aiOvfl[0] && pParent->nOverflow ){
					apDiv[i] = pParent->apOvfl[0];
					pgno = get4byte(apDiv[i]);
					szNew[i] = cellSizePtr(pParent, apDiv[i]);
					pParent->nOverflow = 0;
				}else{
					apDiv[i] = findCell(pParent, i+nxDiv-pParent->nOverflow);
					pgno = get4byte(apDiv[i]);
					szNew[i] = cellSizePtr(pParent, apDiv[i]);

					/* Drop the cell from the parent page. apDiv[i] still points to
					** the cell within the parent, even though it has been dropped.
					** This is safe because dropping a cell only overwrites the first
					** four bytes of it, and this function does not need the first
					** four bytes of the divider cell. So the pointer is safe to use
					** later on.  
					**
					** But not if we are in secure-delete mode. In secure-delete mode,
					** the dropCell() routine will overwrite the entire cell with zeroes.
					** In this case, temporarily copy the cell into the aOvflSpace[]
					** buffer. It will be copied out again as soon as the aSpace[] buffer
					** is allocated.  */
					if( pBt->btsFlags & BTS_SECURE_DELETE ){
						int iOff;

						iOff = SQLITE_PTR_TO_INT(apDiv[i]) - SQLITE_PTR_TO_INT(pParent->aData);
						if( (iOff+szNew[i])>(int)pBt->usableSize ){
							rc = SQLITE_CORRUPT_BKPT;
							memset(apOld, 0, (i+1)*sizeof(MemPage*));
							goto balance_cleanup;
						}else{
							memcpy(&aOvflSpace[iOff], apDiv[i], szNew[i]);
							apDiv[i] = &aOvflSpace[apDiv[i]-pParent->aData];
						}
					}
					dropCell(pParent, i+nxDiv-pParent->nOverflow, szNew[i], &rc);
				}
			}

			/* Make nMaxCells a multiple of 4 in order to preserve 8-byte
			** alignment */
			nMaxCells = (nMaxCells + 3)&~3;

			/*
			** Allocate space for memory structures
			*/
			k = pBt->pageSize + ROUND8(sizeof(MemPage));
			szScratch =
				nMaxCells*sizeof(u8*)                       /* apCell */
				+ nMaxCells*sizeof(u16)                       /* szCell */
				+ pBt->pageSize                               /* aSpace1 */
				+ k*nOld;                                     /* Page copies (apCopy) */
			apCell = sqlite3ScratchMalloc( szScratch ); 
			if( apCell==0 ){
				rc = SQLITE_NOMEM;
				goto balance_cleanup;
			}
			szCell = (u16*)&apCell[nMaxCells];
			aSpace1 = (u8*)&szCell[nMaxCells];
			assert( EIGHT_BYTE_ALIGNMENT(aSpace1) );

			/*
			** Load pointers to all cells on sibling pages and the divider cells
			** into the local apCell[] array.  Make copies of the divider cells
			** into space obtained from aSpace1[] and remove the divider cells
			** from pParent.
			**
			** If the siblings are on leaf pages, then the child pointers of the
			** divider cells are stripped from the cells before they are copied
			** into aSpace1[].  In this way, all cells in apCell[] are without
			** child pointers.  If siblings are not leaves, then all cell in
			** apCell[] include child pointers.  Either way, all cells in apCell[]
			** are alike.
			**
			** leafCorrection:  4 if pPage is a leaf.  0 if pPage is not a leaf.
			**       leafData:  1 if pPage holds key+data and pParent holds only keys.
			*/
			leafCorrection = apOld[0]->leaf*4;
			leafData = apOld[0]->hasData;
			for(i=0; i<nOld; i++){
				int limit;

				/* Before doing anything else, take a copy of the i'th original sibling
				** The rest of this function will use data from the copies rather
				** that the original pages since the original pages will be in the
				** process of being overwritten.  */
				MemPage *pOld = apCopy[i] = (MemPage*)&aSpace1[pBt->pageSize + k*i];
				memcpy(pOld, apOld[i], sizeof(MemPage));
				pOld->aData = (void*)&pOld[1];
				memcpy(pOld->aData, apOld[i]->aData, pBt->pageSize);

				limit = pOld->nCell+pOld->nOverflow;
				if( pOld->nOverflow>0 ){
					for(j=0; j<limit; j++){
						assert( nCell<nMaxCells );
						apCell[nCell] = findOverflowCell(pOld, j);
						szCell[nCell] = cellSizePtr(pOld, apCell[nCell]);
						nCell++;
					}
				}else{
					u8 *aData = pOld->aData;
					u16 maskPage = pOld->maskPage;
					u16 cellOffset = pOld->cellOffset;
					for(j=0; j<limit; j++){
						assert( nCell<nMaxCells );
						apCell[nCell] = findCellv2(aData, maskPage, cellOffset, j);
						szCell[nCell] = cellSizePtr(pOld, apCell[nCell]);
						nCell++;
					}
				}       
				if( i<nOld-1 && !leafData){
					u16 sz = (u16)szNew[i];
					u8 *pTemp;
					assert( nCell<nMaxCells );
					szCell[nCell] = sz;
					pTemp = &aSpace1[iSpace1];
					iSpace1 += sz;
					assert( sz<=pBt->maxLocal+23 );
					assert( iSpace1 <= (int)pBt->pageSize );
					memcpy(pTemp, apDiv[i], sz);
					apCell[nCell] = pTemp+leafCorrection;
					assert( leafCorrection==0 || leafCorrection==4 );
					szCell[nCell] = szCell[nCell] - leafCorrection;
					if( !pOld->leaf ){
						assert( leafCorrection==0 );
						assert( pOld->hdrOffset==0 );
						/* The right pointer of the child page pOld becomes the left
						** pointer of the divider cell */
						memcpy(apCell[nCell], &pOld->aData[8], 4);
					}else{
						assert( leafCorrection==4 );
						if( szCell[nCell]<4 ){
							/* Do not allow any cells smaller than 4 bytes. */
							szCell[nCell] = 4;
						}
					}
					nCell++;
				}
			}

			/*
			** Figure out the number of pages needed to hold all nCell cells.
			** Store this number in "k".  Also compute szNew[] which is the total
			** size of all cells on the i-th page and cntNew[] which is the index
			** in apCell[] of the cell that divides page i from page i+1.  
			** cntNew[k] should equal nCell.
			**
			** Values computed by this block:
			**
			**           k: The total number of sibling pages
			**    szNew[i]: Spaced used on the i-th sibling page.
			**   cntNew[i]: Index in apCell[] and szCell[] for the first cell to
			**              the right of the i-th sibling page.
			** usableSpace: Number of bytes of space available on each sibling.
			** 
			*/
			usableSpace = pBt->usableSize - 12 + leafCorrection;
			for(subtotal=k=i=0; i<nCell; i++){
				assert( i<nMaxCells );
				subtotal += szCell[i] + 2;
				if( subtotal > usableSpace ){
					szNew[k] = subtotal - szCell[i];
					cntNew[k] = i;
					if( leafData ){ i--; }
					subtotal = 0;
					k++;
					if( k>NB+1 ){ rc = SQLITE_CORRUPT_BKPT; goto balance_cleanup; }
				}
			}
			szNew[k] = subtotal;
			cntNew[k] = nCell;
			k++;

			/*
			** The packing computed by the previous block is biased toward the siblings
			** on the left side.  The left siblings are always nearly full, while the
			** right-most sibling might be nearly empty.  This block of code attempts
			** to adjust the packing of siblings to get a better balance.
			**
			** This adjustment is more than an optimization.  The packing above might
			** be so out of balance as to be illegal.  For example, the right-most
			** sibling might be completely empty.  This adjustment is not optional.
			*/
			for(i=k-1; i>0; i--){
				int szRight = szNew[i];  /* Size of sibling on the right */
				int szLeft = szNew[i-1]; /* Size of sibling on the left */
				int r;              /* Index of right-most cell in left sibling */
				int d;              /* Index of first cell to the left of right sibling */

				r = cntNew[i-1] - 1;
				d = r + 1 - leafData;
				assert( d<nMaxCells );
				assert( r<nMaxCells );
				while( szRight==0 
					|| (!bBulk && szRight+szCell[d]+2<=szLeft-(szCell[r]+2)) 
					){
						szRight += szCell[d] + 2;
						szLeft -= szCell[r] + 2;
						cntNew[i-1]--;
						r = cntNew[i-1] - 1;
						d = r + 1 - leafData;
				}
				szNew[i] = szRight;
				szNew[i-1] = szLeft;
			}

			/* Either we found one or more cells (cntnew[0])>0) or pPage is
			** a virtual root page.  A virtual root page is when the real root
			** page is page 1 and we are the only child of that page.
			**
			** UPDATE:  The assert() below is not necessarily true if the database
			** file is corrupt.  The corruption will be detected and reported later
			** in this procedure so there is no need to act upon it now.
			*/
#if 0
			assert( cntNew[0]>0 || (pParent->pgno==1 && pParent->nCell==0) );
#endif

			TRACE(("BALANCE: old: %d %d %d  ",
				apOld[0]->pgno, 
				nOld>=2 ? apOld[1]->pgno : 0,
				nOld>=3 ? apOld[2]->pgno : 0
				));

			/*
			** Allocate k new pages.  Reuse old pages where possible.
			*/
			if( apOld[0]->pgno<=1 ){
				rc = SQLITE_CORRUPT_BKPT;
				goto balance_cleanup;
			}
			pageFlags = apOld[0]->aData[0];
			for(i=0; i<k; i++){
				MemPage *pNew;
				if( i<nOld ){
					pNew = apNew[i] = apOld[i];
					apOld[i] = 0;
					rc = sqlite3PagerWrite(pNew->pDbPage);
					nNew++;
					if( rc ) goto balance_cleanup;
				}else{
					assert( i>0 );
					rc = allocateBtreePage(pBt, &pNew, &pgno, (bBulk ? 1 : pgno), 0);
					if( rc ) goto balance_cleanup;
					apNew[i] = pNew;
					nNew++;

					/* Set the pointer-map entry for the new sibling page. */
					if( ISAUTOVACUUM ){
						ptrmapPut(pBt, pNew->pgno, PTRMAP_BTREE, pParent->pgno, &rc);
						if( rc!=SQLITE_OK ){
							goto balance_cleanup;
						}
					}
				}
			}

			/* Free any old pages that were not reused as new pages.
			*/
			while( i<nOld ){
				freePage(apOld[i], &rc);
				if( rc ) goto balance_cleanup;
				releasePage(apOld[i]);
				apOld[i] = 0;
				i++;
			}

			/*
			** Put the new pages in accending order.  This helps to
			** keep entries in the disk file in order so that a scan
			** of the table is a linear scan through the file.  That
			** in turn helps the operating system to deliver pages
			** from the disk more rapidly.
			**
			** An O(n^2) insertion sort algorithm is used, but since
			** n is never more than NB (a small constant), that should
			** not be a problem.
			**
			** When NB==3, this one optimization makes the database
			** about 25% faster for large insertions and deletions.
			*/
			for(i=0; i<k-1; i++){
				int minV = apNew[i]->pgno;
				int minI = i;
				for(j=i+1; j<k; j++){
					if( apNew[j]->pgno<(unsigned)minV ){
						minI = j;
						minV = apNew[j]->pgno;
					}
				}
				if( minI>i ){
					MemPage *pT;
					pT = apNew[i];
					apNew[i] = apNew[minI];
					apNew[minI] = pT;
				}
			}
			TRACE(("new: %d(%d) %d(%d) %d(%d) %d(%d) %d(%d)\n",
				apNew[0]->pgno, szNew[0],
				nNew>=2 ? apNew[1]->pgno : 0, nNew>=2 ? szNew[1] : 0,
				nNew>=3 ? apNew[2]->pgno : 0, nNew>=3 ? szNew[2] : 0,
				nNew>=4 ? apNew[3]->pgno : 0, nNew>=4 ? szNew[3] : 0,
				nNew>=5 ? apNew[4]->pgno : 0, nNew>=5 ? szNew[4] : 0));

			assert( sqlite3PagerIswriteable(pParent->pDbPage) );
			put4byte(pRight, apNew[nNew-1]->pgno);

			/*
			** Evenly distribute the data in apCell[] across the new pages.
			** Insert divider cells into pParent as necessary.
			*/
			j = 0;
			for(i=0; i<nNew; i++){
				/* Assemble the new sibling page. */
				MemPage *pNew = apNew[i];
				assert( j<nMaxCells );
				zeroPage(pNew, pageFlags);
				assemblePage(pNew, cntNew[i]-j, &apCell[j], &szCell[j]);
				assert( pNew->nCell>0 || (nNew==1 && cntNew[0]==0) );
				assert( pNew->nOverflow==0 );

				j = cntNew[i];

				/* If the sibling page assembled above was not the right-most sibling,
				** insert a divider cell into the parent page.
				*/
				assert( i<nNew-1 || j==nCell );
				if( j<nCell ){
					u8 *pCell;
					u8 *pTemp;
					int sz;

					assert( j<nMaxCells );
					pCell = apCell[j];
					sz = szCell[j] + leafCorrection;
					pTemp = &aOvflSpace[iOvflSpace];
					if( !pNew->leaf ){
						memcpy(&pNew->aData[8], pCell, 4);
					}else if( leafData ){
						/* If the tree is a leaf-data tree, and the siblings are leaves, 
						** then there is no divider cell in apCell[]. Instead, the divider 
						** cell consists of the integer key for the right-most cell of 
						** the sibling-page assembled above only.
						*/
						CellInfo info;
						j--;
						btreeParseCellPtr(pNew, apCell[j], &info);
						pCell = pTemp;
						sz = 4 + putVarint(&pCell[4], info.nKey);
						pTemp = 0;
					}else{
						pCell -= 4;
						/* Obscure case for non-leaf-data trees: If the cell at pCell was
						** previously stored on a leaf node, and its reported size was 4
						** bytes, then it may actually be smaller than this 
						** (see btreeParseCellPtr(), 4 bytes is the minimum size of
						** any cell). But it is important to pass the correct size to 
						** insertCell(), so reparse the cell now.
						**
						** Note that this can never happen in an SQLite data file, as all
						** cells are at least 4 bytes. It only happens in b-trees used
						** to evaluate "IN (SELECT ...)" and similar clauses.
						*/
						if( szCell[j]==4 ){
							assert(leafCorrection==4);
							sz = cellSizePtr(pParent, pCell);
						}
					}
					iOvflSpace += sz;
					assert( sz<=pBt->maxLocal+23 );
					assert( iOvflSpace <= (int)pBt->pageSize );
					insertCell(pParent, nxDiv, pCell, sz, pTemp, pNew->pgno, &rc);
					if( rc!=SQLITE_OK ) goto balance_cleanup;
					assert( sqlite3PagerIswriteable(pParent->pDbPage) );

					j++;
					nxDiv++;
				}
			}
			assert( j==nCell );
			assert( nOld>0 );
			assert( nNew>0 );
			if( (pageFlags & PTF_LEAF)==0 ){
				u8 *zChild = &apCopy[nOld-1]->aData[8];
				memcpy(&apNew[nNew-1]->aData[8], zChild, 4);
			}

			if( isRoot && pParent->nCell==0 && pParent->hdrOffset<=apNew[0]->nFree ){
				/* The root page of the b-tree now contains no cells. The only sibling
				** page is the right-child of the parent. Copy the contents of the
				** child page into the parent, decreasing the overall height of the
				** b-tree structure by one. This is described as the "balance-shallower"
				** sub-algorithm in some documentation.
				**
				** If this is an auto-vacuum database, the call to copyNodeContent() 
				** sets all pointer-map entries corresponding to database image pages 
				** for which the pointer is stored within the content being copied.
				**
				** The second assert below verifies that the child page is defragmented
				** (it must be, as it was just reconstructed using assemblePage()). This
				** is important if the parent page happens to be page 1 of the database
				** image.  */
				assert( nNew==1 );
				assert( apNew[0]->nFree == 
					(get2byte(&apNew[0]->aData[5])-apNew[0]->cellOffset-apNew[0]->nCell*2) 
					);
				copyNodeContent(apNew[0], pParent, &rc);
				freePage(apNew[0], &rc);
			}else if( ISAUTOVACUUM ){
				/* Fix the pointer-map entries for all the cells that were shifted around. 
				** There are several different types of pointer-map entries that need to
				** be dealt with by this routine. Some of these have been set already, but
				** many have not. The following is a summary:
				**
				**   1) The entries associated with new sibling pages that were not
				**      siblings when this function was called. These have already
				**      been set. We don't need to worry about old siblings that were
				**      moved to the free-list - the freePage() code has taken care
				**      of those.
				**
				**   2) The pointer-map entries associated with the first overflow
				**      page in any overflow chains used by new divider cells. These 
				**      have also already been taken care of by the insertCell() code.
				**
				**   3) If the sibling pages are not leaves, then the child pages of
				**      cells stored on the sibling pages may need to be updated.
				**
				**   4) If the sibling pages are not internal intkey nodes, then any
				**      overflow pages used by these cells may need to be updated
				**      (internal intkey nodes never contain pointers to overflow pages).
				**
				**   5) If the sibling pages are not leaves, then the pointer-map
				**      entries for the right-child pages of each sibling may need
				**      to be updated.
				**
				** Cases 1 and 2 are dealt with above by other code. The next
				** block deals with cases 3 and 4 and the one after that, case 5. Since
				** setting a pointer map entry is a relatively expensive operation, this
				** code only sets pointer map entries for child or overflow pages that have
				** actually moved between pages.  */
				MemPage *pNew = apNew[0];
				MemPage *pOld = apCopy[0];
				int nOverflow = pOld->nOverflow;
				int iNextOld = pOld->nCell + nOverflow;
				int iOverflow = (nOverflow ? pOld->aiOvfl[0] : -1);
				j = 0;                             /* Current 'old' sibling page */
				k = 0;                             /* Current 'new' sibling page */
				for(i=0; i<nCell; i++){
					int isDivider = 0;
					while( i==iNextOld ){
						/* Cell i is the cell immediately following the last cell on old
						** sibling page j. If the siblings are not leaf pages of an
						** intkey b-tree, then cell i was a divider cell. */
						assert( j+1 < ArraySize(apCopy) );
						assert( j+1 < nOld );
						pOld = apCopy[++j];
						iNextOld = i + !leafData + pOld->nCell + pOld->nOverflow;
						if( pOld->nOverflow ){
							nOverflow = pOld->nOverflow;
							iOverflow = i + !leafData + pOld->aiOvfl[0];
						}
						isDivider = !leafData;  
					}

					assert(nOverflow>0 || iOverflow<i );
					assert(nOverflow<2 || pOld->aiOvfl[0]==pOld->aiOvfl[1]-1);
					assert(nOverflow<3 || pOld->aiOvfl[1]==pOld->aiOvfl[2]-1);
					if( i==iOverflow ){
						isDivider = 1;
						if( (--nOverflow)>0 ){
							iOverflow++;
						}
					}

					if( i==cntNew[k] ){
						/* Cell i is the cell immediately following the last cell on new
						** sibling page k. If the siblings are not leaf pages of an
						** intkey b-tree, then cell i is a divider cell.  */
						pNew = apNew[++k];
						if( !leafData ) continue;
					}
					assert( j<nOld );
					assert( k<nNew );

					/* If the cell was originally divider cell (and is not now) or
					** an overflow cell, or if the cell was located on a different sibling
					** page before the balancing, then the pointer map entries associated
					** with any child or overflow pages need to be updated.  */
					if( isDivider || pOld->pgno!=pNew->pgno ){
						if( !leafCorrection ){
							ptrmapPut(pBt, get4byte(apCell[i]), PTRMAP_BTREE, pNew->pgno, &rc);
						}
						if( szCell[i]>pNew->minLocal ){
							ptrmapPutOvflPtr(pNew, apCell[i], &rc);
						}
					}
				}

				if( !leafCorrection ){
					for(i=0; i<nNew; i++){
						u32 key = get4byte(&apNew[i]->aData[8]);
						ptrmapPut(pBt, key, PTRMAP_BTREE, apNew[i]->pgno, &rc);
					}
				}

#if 0
				/* The ptrmapCheckPages() contains assert() statements that verify that
				** all pointer map pages are set correctly. This is helpful while 
				** debugging. This is usually disabled because a corrupt database may
				** cause an assert() statement to fail.  */
				ptrmapCheckPages(apNew, nNew);
				ptrmapCheckPages(&pParent, 1);
#endif
			}

			assert( pParent->isInit );
			TRACE(("BALANCE: finished: old=%d new=%d cells=%d\n",
				nOld, nNew, nCell));

			/*
			** Cleanup before returning.
			*/
balance_cleanup:
			sqlite3ScratchFree(apCell);
			for(i=0; i<nOld; i++){
				releasePage(apOld[i]);
			}
			for(i=0; i<nNew; i++){
				releasePage(apNew[i]);
			}

			return rc;
	}
#if defined(_MSC_VER) && _MSC_VER >= 1700 && defined(_M_ARM)
#pragma optimize("", on)
#endif

	static int balance_deeper(MemPage *pRoot, MemPage **ppChild){
		int rc;                        /* Return value from subprocedures */
		MemPage *pChild = 0;           /* Pointer to a new child page */
		Pgno pgnoChild = 0;            /* Page number of the new child page */
		BtShared *pBt = pRoot->pBt;    /* The BTree */

		assert( pRoot->nOverflow>0 );
		assert( sqlite3_mutex_held(pBt->mutex) );

		/* Make pRoot, the root page of the b-tree, writable. Allocate a new 
		** page that will become the new right-child of pPage. Copy the contents
		** of the node stored on pRoot into the new child page.
		*/
		rc = sqlite3PagerWrite(pRoot->pDbPage);
		if( rc==SQLITE_OK ){
			rc = allocateBtreePage(pBt,&pChild,&pgnoChild,pRoot->pgno,0);
			copyNodeContent(pRoot, pChild, &rc);
			if( ISAUTOVACUUM ){
				ptrmapPut(pBt, pgnoChild, PTRMAP_BTREE, pRoot->pgno, &rc);
			}
		}
		if( rc ){
			*ppChild = 0;
			releasePage(pChild);
			return rc;
		}
		assert( sqlite3PagerIswriteable(pChild->pDbPage) );
		assert( sqlite3PagerIswriteable(pRoot->pDbPage) );
		assert( pChild->nCell==pRoot->nCell );

		TRACE(("BALANCE: copy root %d into %d\n", pRoot->pgno, pChild->pgno));

		/* Copy the overflow cells from pRoot to pChild */
		memcpy(pChild->aiOvfl, pRoot->aiOvfl,
			pRoot->nOverflow*sizeof(pRoot->aiOvfl[0]));
		memcpy(pChild->apOvfl, pRoot->apOvfl,
			pRoot->nOverflow*sizeof(pRoot->apOvfl[0]));
		pChild->nOverflow = pRoot->nOverflow;

		/* Zero the contents of pRoot. Then install pChild as the right-child. */
		zeroPage(pRoot, pChild->aData[0] & ~PTF_LEAF);
		put4byte(&pRoot->aData[pRoot->hdrOffset+8], pgnoChild);

		*ppChild = pChild;
		return SQLITE_OK;
	}

	static int balance(BtCursor *pCur){
		int rc = SQLITE_OK;
		const int nMin = pCur->pBt->usableSize * 2 / 3;
		u8 aBalanceQuickSpace[13];
		u8 *pFree = 0;

		TESTONLY( int balance_quick_called = 0 );
		TESTONLY( int balance_deeper_called = 0 );

		do {
			int iPage = pCur->iPage;
			MemPage *pPage = pCur->apPage[iPage];

			if( iPage==0 ){
				if( pPage->nOverflow ){
					/* The root page of the b-tree is overfull. In this case call the
					** balance_deeper() function to create a new child for the root-page
					** and copy the current contents of the root-page to it. The
					** next iteration of the do-loop will balance the child page.
					*/ 
					assert( (balance_deeper_called++)==0 );
					rc = balance_deeper(pPage, &pCur->apPage[1]);
					if( rc==SQLITE_OK ){
						pCur->iPage = 1;
						pCur->aiIdx[0] = 0;
						pCur->aiIdx[1] = 0;
						assert( pCur->apPage[1]->nOverflow );
					}
				}else{
					break;
				}
			}else if( pPage->nOverflow==0 && pPage->nFree<=nMin ){
				break;
			}else{
				MemPage * const pParent = pCur->apPage[iPage-1];
				int const iIdx = pCur->aiIdx[iPage-1];

				rc = sqlite3PagerWrite(pParent->pDbPage);
				if( rc==SQLITE_OK ){
#ifndef SQLITE_OMIT_QUICKBALANCE
					if( pPage->hasData
						&& pPage->nOverflow==1
						&& pPage->aiOvfl[0]==pPage->nCell
						&& pParent->pgno!=1
						&& pParent->nCell==iIdx
						){
							/* Call balance_quick() to create a new sibling of pPage on which
							** to store the overflow cell. balance_quick() inserts a new cell
							** into pParent, which may cause pParent overflow. If this
							** happens, the next interation of the do-loop will balance pParent 
							** use either balance_nonroot() or balance_deeper(). Until this
							** happens, the overflow cell is stored in the aBalanceQuickSpace[]
							** buffer. 
							**
							** The purpose of the following assert() is to check that only a
							** single call to balance_quick() is made for each call to this
							** function. If this were not verified, a subtle bug involving reuse
							** of the aBalanceQuickSpace[] might sneak in.
							*/
							assert( (balance_quick_called++)==0 );
							rc = balance_quick(pParent, pPage, aBalanceQuickSpace);
					}else
#endif
					{
						/* In this case, call balance_nonroot() to redistribute cells
						** between pPage and up to 2 of its sibling pages. This involves
						** modifying the contents of pParent, which may cause pParent to
						** become overfull or underfull. The next iteration of the do-loop
						** will balance the parent page to correct this.
						** 
						** If the parent page becomes overfull, the overflow cell or cells
						** are stored in the pSpace buffer allocated immediately below. 
						** A subsequent iteration of the do-loop will deal with this by
						** calling balance_nonroot() (balance_deeper() may be called first,
						** but it doesn't deal with overflow cells - just moves them to a
						** different page). Once this subsequent call to balance_nonroot() 
						** has completed, it is safe to release the pSpace buffer used by
						** the previous call, as the overflow cell data will have been 
						** copied either into the body of a database page or into the new
						** pSpace buffer passed to the latter call to balance_nonroot().
						*/
						u8 *pSpace = sqlite3PageMalloc(pCur->pBt->pageSize);
						rc = balance_nonroot(pParent, iIdx, pSpace, iPage==1, pCur->hints);
						if( pFree ){
							/* If pFree is not NULL, it points to the pSpace buffer used 
							** by a previous call to balance_nonroot(). Its contents are
							** now stored either on real database pages or within the 
							** new pSpace buffer, so it may be safely freed here. */
							sqlite3PageFree(pFree);
						}

						/* The pSpace buffer will be freed after the next call to
						** balance_nonroot(), or just before this function returns, whichever
						** comes first. */
						pFree = pSpace;
					}
				}

				pPage->nOverflow = 0;

				/* The next iteration of the do-loop balances the parent page. */
				releasePage(pPage);
				pCur->iPage--;
			}
		}while( rc==SQLITE_OK );

		if( pFree ){
			sqlite3PageFree(pFree);
		}
		return rc;
	}

#pragma endregion

#pragma region Insert

	int sqlite3BtreeInsert(
		BtCursor *pCur,                /* Insert data into the table of this cursor */
		const void *pKey, i64 nKey,    /* The key of the new record */
		const void *pData, int nData,  /* The data of the new record */
		int nZero,                     /* Number of extra 0 bytes to append to data */
		int appendBias,                /* True if this is likely an append */
		int seekResult                 /* Result of prior MovetoUnpacked() call */
		){
			int rc;
			int loc = seekResult;          /* -1: before desired location  +1: after */
			int szNew = 0;
			int idx;
			MemPage *pPage;
			Btree *p = pCur->pBtree;
			BtShared *pBt = p->pBt;
			unsigned char *oldCell;
			unsigned char *newCell = 0;

			if( pCur->eState==CURSOR_FAULT ){
				assert( pCur->skipNext!=SQLITE_OK );
				return pCur->skipNext;
			}

			assert( cursorHoldsMutex(pCur) );
			assert( pCur->wrFlag && pBt->inTransaction==TRANS_WRITE
				&& (pBt->btsFlags & BTS_READ_ONLY)==0 );
			assert( hasSharedCacheTableLock(p, pCur->pgnoRoot, pCur->pKeyInfo!=0, 2) );

			/* Assert that the caller has been consistent. If this cursor was opened
			** expecting an index b-tree, then the caller should be inserting blob
			** keys with no associated data. If the cursor was opened expecting an
			** intkey table, the caller should be inserting integer keys with a
			** blob of associated data.  */
			assert( (pKey==0)==(pCur->pKeyInfo==0) );

			/* Save the positions of any other cursors open on this table.
			**
			** In some cases, the call to btreeMoveto() below is a no-op. For
			** example, when inserting data into a table with auto-generated integer
			** keys, the VDBE layer invokes sqlite3BtreeLast() to figure out the 
			** integer key to use. It then calls this function to actually insert the 
			** data into the intkey B-Tree. In this case btreeMoveto() recognizes
			** that the cursor is already where it needs to be and returns without
			** doing any work. To avoid thwarting these optimizations, it is important
			** not to clear the cursor here.
			*/
			rc = saveAllCursors(pBt, pCur->pgnoRoot, pCur);
			if( rc ) return rc;

			/* If this is an insert into a table b-tree, invalidate any incrblob 
			** cursors open on the row being replaced (assuming this is a replace
			** operation - if it is not, the following is a no-op).  */
			if( pCur->pKeyInfo==0 ){
				invalidateIncrblobCursors(p, nKey, 0);
			}

			if( !loc ){
				rc = btreeMoveto(pCur, pKey, nKey, appendBias, &loc);
				if( rc ) return rc;
			}
			assert( pCur->eState==CURSOR_VALID || (pCur->eState==CURSOR_INVALID && loc) );

			pPage = pCur->apPage[pCur->iPage];
			assert( pPage->intKey || nKey>=0 );
			assert( pPage->leaf || !pPage->intKey );

			TRACE(("INSERT: table=%d nkey=%lld ndata=%d page=%d %s\n",
				pCur->pgnoRoot, nKey, nData, pPage->pgno,
				loc==0 ? "overwrite" : "new entry"));
			assert( pPage->isInit );
			allocateTempSpace(pBt);
			newCell = pBt->pTmpSpace;
			if( newCell==0 ) return SQLITE_NOMEM;
			rc = fillInCell(pPage, newCell, pKey, nKey, pData, nData, nZero, &szNew);
			if( rc ) goto end_insert;
			assert( szNew==cellSizePtr(pPage, newCell) );
			assert( szNew <= MX_CELL_SIZE(pBt) );
			idx = pCur->aiIdx[pCur->iPage];
			if( loc==0 ){
				u16 szOld;
				assert( idx<pPage->nCell );
				rc = sqlite3PagerWrite(pPage->pDbPage);
				if( rc ){
					goto end_insert;
				}
				oldCell = findCell(pPage, idx);
				if( !pPage->leaf ){
					memcpy(newCell, oldCell, 4);
				}
				szOld = cellSizePtr(pPage, oldCell);
				rc = clearCell(pPage, oldCell);
				dropCell(pPage, idx, szOld, &rc);
				if( rc ) goto end_insert;
			}else if( loc<0 && pPage->nCell>0 ){
				assert( pPage->leaf );
				idx = ++pCur->aiIdx[pCur->iPage];
			}else{
				assert( pPage->leaf );
			}
			insertCell(pPage, idx, newCell, szNew, 0, 0, &rc);
			assert( rc!=SQLITE_OK || pPage->nCell>0 || pPage->nOverflow>0 );

			/* If no error has occurred and pPage has an overflow cell, call balance() 
			** to redistribute the cells within the tree. Since balance() may move
			** the cursor, zero the BtCursor.info.nSize and BtCursor.validNKey
			** variables.
			**
			** Previous versions of SQLite called moveToRoot() to move the cursor
			** back to the root page as balance() used to invalidate the contents
			** of BtCursor.apPage[] and BtCursor.aiIdx[]. Instead of doing that,
			** set the cursor state to "invalid". This makes common insert operations
			** slightly faster.
			**
			** There is a subtle but important optimization here too. When inserting
			** multiple records into an intkey b-tree using a single cursor (as can
			** happen while processing an "INSERT INTO ... SELECT" statement), it
			** is advantageous to leave the cursor pointing to the last entry in
			** the b-tree if possible. If the cursor is left pointing to the last
			** entry in the table, and the next row inserted has an integer key
			** larger than the largest existing key, it is possible to insert the
			** row without seeking the cursor. This can be a big performance boost.
			*/
			pCur->info.nSize = 0;
			pCur->validNKey = 0;
			if( rc==SQLITE_OK && pPage->nOverflow ){
				rc = balance(pCur);

				/* Must make sure nOverflow is reset to zero even if the balance()
				** fails. Internal data structure corruption will result otherwise. 
				** Also, set the cursor state to invalid. This stops saveCursorPosition()
				** from trying to save the current position of the cursor.  */
				pCur->apPage[pCur->iPage]->nOverflow = 0;
				pCur->eState = CURSOR_INVALID;
			}
			assert( pCur->apPage[pCur->iPage]->nOverflow==0 );

end_insert:
			return rc;
	}

	int sqlite3BtreeDelete(BtCursor *pCur){
		Btree *p = pCur->pBtree;
		BtShared *pBt = p->pBt;              
		int rc;                              /* Return code */
		MemPage *pPage;                      /* Page to delete cell from */
		unsigned char *pCell;                /* Pointer to cell to delete */
		int iCellIdx;                        /* Index of cell to delete */
		int iCellDepth;                      /* Depth of node containing pCell */ 

		assert( cursorHoldsMutex(pCur) );
		assert( pBt->inTransaction==TRANS_WRITE );
		assert( (pBt->btsFlags & BTS_READ_ONLY)==0 );
		assert( pCur->wrFlag );
		assert( hasSharedCacheTableLock(p, pCur->pgnoRoot, pCur->pKeyInfo!=0, 2) );
		assert( !hasReadConflicts(p, pCur->pgnoRoot) );

		if( NEVER(pCur->aiIdx[pCur->iPage]>=pCur->apPage[pCur->iPage]->nCell) 
			|| NEVER(pCur->eState!=CURSOR_VALID)
			){
				return SQLITE_ERROR;  /* Something has gone awry. */
		}

		iCellDepth = pCur->iPage;
		iCellIdx = pCur->aiIdx[iCellDepth];
		pPage = pCur->apPage[iCellDepth];
		pCell = findCell(pPage, iCellIdx);

		/* If the page containing the entry to delete is not a leaf page, move
		** the cursor to the largest entry in the tree that is smaller than
		** the entry being deleted. This cell will replace the cell being deleted
		** from the internal node. The 'previous' entry is used for this instead
		** of the 'next' entry, as the previous entry is always a part of the
		** sub-tree headed by the child page of the cell being deleted. This makes
		** balancing the tree following the delete operation easier.  */
		if( !pPage->leaf ){
			int notUsed;
			rc = sqlite3BtreePrevious(pCur, &notUsed);
			if( rc ) return rc;
		}

		/* Save the positions of any other cursors open on this table before
		** making any modifications. Make the page containing the entry to be 
		** deleted writable. Then free any overflow pages associated with the 
		** entry and finally remove the cell itself from within the page.  
		*/
		rc = saveAllCursors(pBt, pCur->pgnoRoot, pCur);
		if( rc ) return rc;

		/* If this is a delete operation to remove a row from a table b-tree,
		** invalidate any incrblob cursors open on the row being deleted.  */
		if( pCur->pKeyInfo==0 ){
			invalidateIncrblobCursors(p, pCur->info.nKey, 0);
		}

		rc = sqlite3PagerWrite(pPage->pDbPage);
		if( rc ) return rc;
		rc = clearCell(pPage, pCell);
		dropCell(pPage, iCellIdx, cellSizePtr(pPage, pCell), &rc);
		if( rc ) return rc;

		/* If the cell deleted was not located on a leaf page, then the cursor
		** is currently pointing to the largest entry in the sub-tree headed
		** by the child-page of the cell that was just deleted from an internal
		** node. The cell from the leaf node needs to be moved to the internal
		** node to replace the deleted cell.  */
		if( !pPage->leaf ){
			MemPage *pLeaf = pCur->apPage[pCur->iPage];
			int nCell;
			Pgno n = pCur->apPage[iCellDepth+1]->pgno;
			unsigned char *pTmp;

			pCell = findCell(pLeaf, pLeaf->nCell-1);
			nCell = cellSizePtr(pLeaf, pCell);
			assert( MX_CELL_SIZE(pBt) >= nCell );

			allocateTempSpace(pBt);
			pTmp = pBt->pTmpSpace;

			rc = sqlite3PagerWrite(pLeaf->pDbPage);
			insertCell(pPage, iCellIdx, pCell-4, nCell+4, pTmp, n, &rc);
			dropCell(pLeaf, pLeaf->nCell-1, nCell, &rc);
			if( rc ) return rc;
		}

		/* Balance the tree. If the entry deleted was located on a leaf page,
		** then the cursor still points to that page. In this case the first
		** call to balance() repairs the tree, and the if(...) condition is
		** never true.
		**
		** Otherwise, if the entry deleted was on an internal node page, then
		** pCur is pointing to the leaf page from which a cell was removed to
		** replace the cell deleted from the internal node. This is slightly
		** tricky as the leaf node may be underfull, and the internal node may
		** be either under or overfull. In this case run the balancing algorithm
		** on the leaf node first. If the balance proceeds far enough up the
		** tree that we can be sure that any problem in the internal node has
		** been corrected, so be it. Otherwise, after balancing the leaf node,
		** walk the cursor up the tree to the internal node and balance it as 
		** well.  */
		rc = balance(pCur);
		if( rc==SQLITE_OK && pCur->iPage>iCellDepth ){
			while( pCur->iPage>iCellDepth ){
				releasePage(pCur->apPage[pCur->iPage--]);
			}
			rc = balance(pCur);
		}

		if( rc==SQLITE_OK ){
			moveToRoot(pCur);
		}
		return rc;
	}

	static int btreeCreateTable(Btree *p, int *piTable, int createTabFlags){
		BtShared *pBt = p->pBt;
		MemPage *pRoot;
		Pgno pgnoRoot;
		int rc;
		int ptfFlags;          /* Page-type flage for the root page of new table */

		assert( sqlite3BtreeHoldsMutex(p) );
		assert( pBt->inTransaction==TRANS_WRITE );
		assert( (pBt->btsFlags & BTS_READ_ONLY)==0 );

#ifdef OMIT_AUTOVACUUM
		rc = allocateBtreePage(pBt, &pRoot, &pgnoRoot, 1, 0);
		if( rc ){
			return rc;
		}
#else
		if( pBt->autoVacuum ){
			Pgno pgnoMove;      /* Move a page here to make room for the root-page */
			MemPage *pPageMove; /* The page to move to. */

			/* Creating a new table may probably require moving an existing database
			** to make room for the new tables root page. In case this page turns
			** out to be an overflow page, delete all overflow page-map caches
			** held by open cursors.
			*/
			invalidateAllOverflowCache(pBt);

			/* Read the value of meta[3] from the database to determine where the
			** root page of the new table should go. meta[3] is the largest root-page
			** created so far, so the new root-page is (meta[3]+1).
			*/
			sqlite3BtreeGetMeta(p, BTREE_LARGEST_ROOT_PAGE, &pgnoRoot);
			pgnoRoot++;

			/* The new root-page may not be allocated on a pointer-map page, or the
			** PENDING_BYTE page.
			*/
			while( pgnoRoot==PTRMAP_PAGENO(pBt, pgnoRoot) ||
				pgnoRoot==PENDING_BYTE_PAGE(pBt) ){
					pgnoRoot++;
			}
			assert( pgnoRoot>=3 );

			/* Allocate a page. The page that currently resides at pgnoRoot will
			** be moved to the allocated page (unless the allocated page happens
			** to reside at pgnoRoot).
			*/
			rc = allocateBtreePage(pBt, &pPageMove, &pgnoMove, pgnoRoot, BTALLOC_EXACT);
			if( rc!=SQLITE_OK ){
				return rc;
			}

			if( pgnoMove!=pgnoRoot ){
				/* pgnoRoot is the page that will be used for the root-page of
				** the new table (assuming an error did not occur). But we were
				** allocated pgnoMove. If required (i.e. if it was not allocated
				** by extending the file), the current page at position pgnoMove
				** is already journaled.
				*/
				u8 eType = 0;
				Pgno iPtrPage = 0;

				releasePage(pPageMove);

				/* Move the page currently at pgnoRoot to pgnoMove. */
				rc = btreeGetPage(pBt, pgnoRoot, &pRoot, 0);
				if( rc!=SQLITE_OK ){
					return rc;
				}
				rc = ptrmapGet(pBt, pgnoRoot, &eType, &iPtrPage);
				if( eType==PTRMAP_ROOTPAGE || eType==PTRMAP_FREEPAGE ){
					rc = SQLITE_CORRUPT_BKPT;
				}
				if( rc!=SQLITE_OK ){
					releasePage(pRoot);
					return rc;
				}
				assert( eType!=PTRMAP_ROOTPAGE );
				assert( eType!=PTRMAP_FREEPAGE );
				rc = relocatePage(pBt, pRoot, eType, iPtrPage, pgnoMove, 0);
				releasePage(pRoot);

				/* Obtain the page at pgnoRoot */
				if( rc!=SQLITE_OK ){
					return rc;
				}
				rc = btreeGetPage(pBt, pgnoRoot, &pRoot, 0);
				if( rc!=SQLITE_OK ){
					return rc;
				}
				rc = sqlite3PagerWrite(pRoot->pDbPage);
				if( rc!=SQLITE_OK ){
					releasePage(pRoot);
					return rc;
				}
			}else{
				pRoot = pPageMove;
			} 

			/* Update the pointer-map and meta-data with the new root-page number. */
			ptrmapPut(pBt, pgnoRoot, PTRMAP_ROOTPAGE, 0, &rc);
			if( rc ){
				releasePage(pRoot);
				return rc;
			}

			/* When the new root page was allocated, page 1 was made writable in
			** order either to increase the database filesize, or to decrement the
			** freelist count.  Hence, the sqlite3BtreeUpdateMeta() call cannot fail.
			*/
			assert( sqlite3PagerIswriteable(pBt->pPage1->pDbPage) );
			rc = sqlite3BtreeUpdateMeta(p, 4, pgnoRoot);
			if( NEVER(rc) ){
				releasePage(pRoot);
				return rc;
			}

		}else{
			rc = allocateBtreePage(pBt, &pRoot, &pgnoRoot, 1, 0);
			if( rc ) return rc;
		}
#endif
		assert( sqlite3PagerIswriteable(pRoot->pDbPage) );
		if( createTabFlags & BTREE_INTKEY ){
			ptfFlags = PTF_INTKEY | PTF_LEAFDATA | PTF_LEAF;
		}else{
			ptfFlags = PTF_ZERODATA | PTF_LEAF;
		}
		zeroPage(pRoot, ptfFlags);
		sqlite3PagerUnref(pRoot->pDbPage);
		assert( (pBt->openFlags & BTREE_SINGLE)==0 || pgnoRoot==2 );
		*piTable = (int)pgnoRoot;
		return SQLITE_OK;
	}
	int sqlite3BtreeCreateTable(Btree *p, int *piTable, int flags){
		int rc;
		sqlite3BtreeEnter(p);
		rc = btreeCreateTable(p, piTable, flags);
		sqlite3BtreeLeave(p);
		return rc;
	}

	static int clearDatabasePage(
		BtShared *pBt,           /* The BTree that contains the table */
		Pgno pgno,               /* Page number to clear */
		int freePageFlag,        /* Deallocate page if true */
		int *pnChange            /* Add number of Cells freed to this counter */
		){
			MemPage *pPage;
			int rc;
			unsigned char *pCell;
			int i;

			assert( sqlite3_mutex_held(pBt->mutex) );
			if( pgno>btreePagecount(pBt) ){
				return SQLITE_CORRUPT_BKPT;
			}

			rc = getAndInitPage(pBt, pgno, &pPage);
			if( rc ) return rc;
			for(i=0; i<pPage->nCell; i++){
				pCell = findCell(pPage, i);
				if( !pPage->leaf ){
					rc = clearDatabasePage(pBt, get4byte(pCell), 1, pnChange);
					if( rc ) goto cleardatabasepage_out;
				}
				rc = clearCell(pPage, pCell);
				if( rc ) goto cleardatabasepage_out;
			}
			if( !pPage->leaf ){
				rc = clearDatabasePage(pBt, get4byte(&pPage->aData[8]), 1, pnChange);
				if( rc ) goto cleardatabasepage_out;
			}else if( pnChange ){
				assert( pPage->intKey );
				*pnChange += pPage->nCell;
			}
			if( freePageFlag ){
				freePage(pPage, &rc);
			}else if( (rc = sqlite3PagerWrite(pPage->pDbPage))==0 ){
				zeroPage(pPage, pPage->aData[0] | PTF_LEAF);
			}

cleardatabasepage_out:
			releasePage(pPage);
			return rc;
	}

	int sqlite3BtreeClearTable(Btree *p, int iTable, int *pnChange){
		int rc;
		BtShared *pBt = p->pBt;
		sqlite3BtreeEnter(p);
		assert( p->inTrans==TRANS_WRITE );

		rc = saveAllCursors(pBt, (Pgno)iTable, 0);

		if( SQLITE_OK==rc ){
			/* Invalidate all incrblob cursors open on table iTable (assuming iTable
			** is the root of a table b-tree - if it is not, the following call is
			** a no-op).  */
			invalidateIncrblobCursors(p, 0, 1);
			rc = clearDatabasePage(pBt, (Pgno)iTable, 0, pnChange);
		}
		sqlite3BtreeLeave(p);
		return rc;
	}

	static int btreeDropTable(Btree *p, Pgno iTable, int *piMoved){
		int rc;
		MemPage *pPage = 0;
		BtShared *pBt = p->pBt;

		assert( sqlite3BtreeHoldsMutex(p) );
		assert( p->inTrans==TRANS_WRITE );

		/* It is illegal to drop a table if any cursors are open on the
		** database. This is because in auto-vacuum mode the backend may
		** need to move another root-page to fill a gap left by the deleted
		** root page. If an open cursor was using this page a problem would 
		** occur.
		**
		** This error is caught long before control reaches this point.
		*/
		if( NEVER(pBt->pCursor) ){
			sqlite3ConnectionBlocked(p->db, pBt->pCursor->pBtree->db);
			return SQLITE_LOCKED_SHAREDCACHE;
		}

		rc = btreeGetPage(pBt, (Pgno)iTable, &pPage, 0);
		if( rc ) return rc;
		rc = sqlite3BtreeClearTable(p, iTable, 0);
		if( rc ){
			releasePage(pPage);
			return rc;
		}

		*piMoved = 0;

		if( iTable>1 ){
#ifdef SQLITE_OMIT_AUTOVACUUM
			freePage(pPage, &rc);
			releasePage(pPage);
#else
			if( pBt->autoVacuum ){
				Pgno maxRootPgno;
				sqlite3BtreeGetMeta(p, BTREE_LARGEST_ROOT_PAGE, &maxRootPgno);

				if( iTable==maxRootPgno ){
					/* If the table being dropped is the table with the largest root-page
					** number in the database, put the root page on the free list. 
					*/
					freePage(pPage, &rc);
					releasePage(pPage);
					if( rc!=SQLITE_OK ){
						return rc;
					}
				}else{
					/* The table being dropped does not have the largest root-page
					** number in the database. So move the page that does into the 
					** gap left by the deleted root-page.
					*/
					MemPage *pMove;
					releasePage(pPage);
					rc = btreeGetPage(pBt, maxRootPgno, &pMove, 0);
					if( rc!=SQLITE_OK ){
						return rc;
					}
					rc = relocatePage(pBt, pMove, PTRMAP_ROOTPAGE, 0, iTable, 0);
					releasePage(pMove);
					if( rc!=SQLITE_OK ){
						return rc;
					}
					pMove = 0;
					rc = btreeGetPage(pBt, maxRootPgno, &pMove, 0);
					freePage(pMove, &rc);
					releasePage(pMove);
					if( rc!=SQLITE_OK ){
						return rc;
					}
					*piMoved = maxRootPgno;
				}

				/* Set the new 'max-root-page' value in the database header. This
				** is the old value less one, less one more if that happens to
				** be a root-page number, less one again if that is the
				** PENDING_BYTE_PAGE.
				*/
				maxRootPgno--;
				while( maxRootPgno==PENDING_BYTE_PAGE(pBt)
					|| PTRMAP_ISPAGE(pBt, maxRootPgno) ){
						maxRootPgno--;
				}
				assert( maxRootPgno!=PENDING_BYTE_PAGE(pBt) );

				rc = sqlite3BtreeUpdateMeta(p, 4, maxRootPgno);
			}else{
				freePage(pPage, &rc);
				releasePage(pPage);
			}
#endif
		}else{
			/* If sqlite3BtreeDropTable was called on page 1.
			** This really never should happen except in a corrupt
			** database. 
			*/
			zeroPage(pPage, PTF_INTKEY|PTF_LEAF );
			releasePage(pPage);
		}
		return rc;  
	}
	int sqlite3BtreeDropTable(Btree *p, int iTable, int *piMoved){
		int rc;
		sqlite3BtreeEnter(p);
		rc = btreeDropTable(p, iTable, piMoved);
		sqlite3BtreeLeave(p);
		return rc;
	}

#pragma endregion

#pragma region Meta

	void sqlite3BtreeGetMeta(Btree *p, int idx, u32 *pMeta){
		BtShared *pBt = p->pBt;

		sqlite3BtreeEnter(p);
		assert( p->inTrans>TRANS_NONE );
		assert( SQLITE_OK==querySharedCacheTableLock(p, MASTER_ROOT, READ_LOCK) );
		assert( pBt->pPage1 );
		assert( idx>=0 && idx<=15 );

		*pMeta = get4byte(&pBt->pPage1->aData[36 + idx*4]);

		/* If auto-vacuum is disabled in this build and this is an auto-vacuum
		** database, mark the database as read-only.  */
#ifdef SQLITE_OMIT_AUTOVACUUM
		if( idx==BTREE_LARGEST_ROOT_PAGE && *pMeta>0 ){
			pBt->btsFlags |= BTS_READ_ONLY;
		}
#endif

		sqlite3BtreeLeave(p);
	}

	int sqlite3BtreeUpdateMeta(Btree *p, int idx, u32 iMeta){
		BtShared *pBt = p->pBt;
		unsigned char *pP1;
		int rc;
		assert( idx>=1 && idx<=15 );
		sqlite3BtreeEnter(p);
		assert( p->inTrans==TRANS_WRITE );
		assert( pBt->pPage1!=0 );
		pP1 = pBt->pPage1->aData;
		rc = sqlite3PagerWrite(pBt->pPage1->pDbPage);
		if( rc==SQLITE_OK ){
			put4byte(&pP1[36 + idx*4], iMeta);
#ifndef SQLITE_OMIT_AUTOVACUUM
			if( idx==BTREE_INCR_VACUUM ){
				assert( pBt->autoVacuum || iMeta==0 );
				assert( iMeta==0 || iMeta==1 );
				pBt->incrVacuum = (u8)iMeta;
			}
#endif
		}
		sqlite3BtreeLeave(p);
		return rc;
	}

#ifndef OMIT_BTREECOUNT
	int sqlite3BtreeCount(BtCursor *pCur, i64 *pnEntry){
		i64 nEntry = 0;                      /* Value to return in *pnEntry */
		int rc;                              /* Return code */

		if( pCur->pgnoRoot==0 ){
			*pnEntry = 0;
			return SQLITE_OK;
		}
		rc = moveToRoot(pCur);

		/* Unless an error occurs, the following loop runs one iteration for each
		** page in the B-Tree structure (not including overflow pages). 
		*/
		while( rc==SQLITE_OK ){
			int iIdx;                          /* Index of child node in parent */
			MemPage *pPage;                    /* Current page of the b-tree */

			/* If this is a leaf page or the tree is not an int-key tree, then 
			** this page contains countable entries. Increment the entry counter
			** accordingly.
			*/
			pPage = pCur->apPage[pCur->iPage];
			if( pPage->leaf || !pPage->intKey ){
				nEntry += pPage->nCell;
			}

			/* pPage is a leaf node. This loop navigates the cursor so that it 
			** points to the first interior cell that it points to the parent of
			** the next page in the tree that has not yet been visited. The
			** pCur->aiIdx[pCur->iPage] value is set to the index of the parent cell
			** of the page, or to the number of cells in the page if the next page
			** to visit is the right-child of its parent.
			**
			** If all pages in the tree have been visited, return SQLITE_OK to the
			** caller.
			*/
			if( pPage->leaf ){
				do {
					if( pCur->iPage==0 ){
						/* All pages of the b-tree have been visited. Return successfully. */
						*pnEntry = nEntry;
						return SQLITE_OK;
					}
					moveToParent(pCur);
				}while ( pCur->aiIdx[pCur->iPage]>=pCur->apPage[pCur->iPage]->nCell );

				pCur->aiIdx[pCur->iPage]++;
				pPage = pCur->apPage[pCur->iPage];
			}

			/* Descend to the child node of the cell that the cursor currently 
			** points at. This is the right-child if (iIdx==pPage->nCell).
			*/
			iIdx = pCur->aiIdx[pCur->iPage];
			if( iIdx==pPage->nCell ){
				rc = moveToChild(pCur, get4byte(&pPage->aData[pPage->hdrOffset+8]));
			}else{
				rc = moveToChild(pCur, get4byte(findCell(pPage, iIdx)));
			}
		}

		/* An error has occurred. Return an error code. */
		return rc;
	}
#endif

	Pager *sqlite3BtreePager(Btree *p){
		return p->pBt->pPager;
	}

#pragma endregion

#pragma region INTEGRITY_CHECK
#ifndef OMIT_INTEGRITY_CHECK
	static void checkAppendMsg(
		IntegrityCk *pCheck,
		char *zMsg1,
		const char *zFormat,
		...
		){
			va_list ap;
			if( !pCheck->mxErr ) return;
			pCheck->mxErr--;
			pCheck->nErr++;
			va_start(ap, zFormat);
			if( pCheck->errMsg.nChar ){
				sqlite3StrAccumAppend(&pCheck->errMsg, "\n", 1);
			}
			if( zMsg1 ){
				sqlite3StrAccumAppend(&pCheck->errMsg, zMsg1, -1);
			}
			sqlite3VXPrintf(&pCheck->errMsg, 1, zFormat, ap);
			va_end(ap);
			if( pCheck->errMsg.mallocFailed ){
				pCheck->mallocFailed = 1;
			}
	}

	static int getPageReferenced(IntegrityCk *pCheck, Pgno iPg){
		assert( iPg<=pCheck->nPage && sizeof(pCheck->aPgRef[0])==1 );
		return (pCheck->aPgRef[iPg/8] & (1 << (iPg & 0x07)));
	}

	static void setPageReferenced(IntegrityCk *pCheck, Pgno iPg){
		assert( iPg<=pCheck->nPage && sizeof(pCheck->aPgRef[0])==1 );
		pCheck->aPgRef[iPg/8] |= (1 << (iPg & 0x07));
	}

	static int checkRef(IntegrityCk *pCheck, Pgno iPage, char *zContext){
		if( iPage==0 ) return 1;
		if( iPage>pCheck->nPage ){
			checkAppendMsg(pCheck, zContext, "invalid page number %d", iPage);
			return 1;
		}
		if( getPageReferenced(pCheck, iPage) ){
			checkAppendMsg(pCheck, zContext, "2nd reference to page %d", iPage);
			return 1;
		}
		setPageReferenced(pCheck, iPage);
		return 0;
	}

#ifndef OMIT_AUTOVACUUM
	static void checkPtrmap(
		IntegrityCk *pCheck,   /* Integrity check context */
		Pgno iChild,           /* Child page number */
		u8 eType,              /* Expected pointer map type */
		Pgno iParent,          /* Expected pointer map parent page number */
		char *zContext         /* Context description (used for error msg) */
		){
			int rc;
			u8 ePtrmapType;
			Pgno iPtrmapParent;

			rc = ptrmapGet(pCheck->pBt, iChild, &ePtrmapType, &iPtrmapParent);
			if( rc!=SQLITE_OK ){
				if( rc==SQLITE_NOMEM || rc==SQLITE_IOERR_NOMEM ) pCheck->mallocFailed = 1;
				checkAppendMsg(pCheck, zContext, "Failed to read ptrmap key=%d", iChild);
				return;
			}

			if( ePtrmapType!=eType || iPtrmapParent!=iParent ){
				checkAppendMsg(pCheck, zContext, 
					"Bad ptr map entry key=%d expected=(%d,%d) got=(%d,%d)", 
					iChild, eType, iParent, ePtrmapType, iPtrmapParent);
			}
	}
#endif

	static void checkList(
		IntegrityCk *pCheck,  /* Integrity checking context */
		int isFreeList,       /* True for a freelist.  False for overflow page list */
		int iPage,            /* Page number for first page in the list */
		int N,                /* Expected number of pages in the list */
		char *zContext        /* Context for error messages */
		){
			int i;
			int expected = N;
			int iFirst = iPage;
			while( N-- > 0 && pCheck->mxErr ){
				DbPage *pOvflPage;
				unsigned char *pOvflData;
				if( iPage<1 ){
					checkAppendMsg(pCheck, zContext,
						"%d of %d pages missing from overflow list starting at %d",
						N+1, expected, iFirst);
					break;
				}
				if( checkRef(pCheck, iPage, zContext) ) break;
				if( sqlite3PagerGet(pCheck->pPager, (Pgno)iPage, &pOvflPage) ){
					checkAppendMsg(pCheck, zContext, "failed to get page %d", iPage);
					break;
				}
				pOvflData = (unsigned char *)sqlite3PagerGetData(pOvflPage);
				if( isFreeList ){
					int n = get4byte(&pOvflData[4]);
#ifndef OMIT_AUTOVACUUM
					if( pCheck->pBt->autoVacuum ){
						checkPtrmap(pCheck, iPage, PTRMAP_FREEPAGE, 0, zContext);
					}
#endif
					if( n>(int)pCheck->pBt->usableSize/4-2 ){
						checkAppendMsg(pCheck, zContext,
							"freelist leaf count too big on page %d", iPage);
						N--;
					}else{
						for(i=0; i<n; i++){
							Pgno iFreePage = get4byte(&pOvflData[8+i*4]);
#ifndef OMIT_AUTOVACUUM
							if( pCheck->pBt->autoVacuum ){
								checkPtrmap(pCheck, iFreePage, PTRMAP_FREEPAGE, 0, zContext);
							}
#endif
							checkRef(pCheck, iFreePage, zContext);
						}
						N -= n;
					}
				}
#ifndef OMIT_AUTOVACUUM
				else{
					/* If this database supports auto-vacuum and iPage is not the last
					** page in this overflow list, check that the pointer-map entry for
					** the following page matches iPage.
					*/
					if( pCheck->pBt->autoVacuum && N>0 ){
						i = get4byte(pOvflData);
						checkPtrmap(pCheck, i, PTRMAP_OVERFLOW2, iPage, zContext);
					}
				}
#endif
				iPage = get4byte(pOvflData);
				sqlite3PagerUnref(pOvflPage);
			}
	}

	static int checkTreePage(
		IntegrityCk *pCheck,  /* Context for the sanity check */
		int iPage,            /* Page number of the page to check */
		char *zParentContext, /* Parent context */
		i64 *pnParentMinKey, 
		i64 *pnParentMaxKey
		){
			MemPage *pPage;
			int i, rc, depth, d2, pgno, cnt;
			int hdr, cellStart;
			int nCell;
			u8 *data;
			BtShared *pBt;
			int usableSize;
			char zContext[100];
			char *hit = 0;
			i64 nMinKey = 0;
			i64 nMaxKey = 0;

			sqlite3_snprintf(sizeof(zContext), zContext, "Page %d: ", iPage);

			/* Check that the page exists
			*/
			pBt = pCheck->pBt;
			usableSize = pBt->usableSize;
			if( iPage==0 ) return 0;
			if( checkRef(pCheck, iPage, zParentContext) ) return 0;
			if( (rc = btreeGetPage(pBt, (Pgno)iPage, &pPage, 0))!=0 ){
				checkAppendMsg(pCheck, zContext,
					"unable to get the page. error code=%d", rc);
				return 0;
			}

			/* Clear MemPage.isInit to make sure the corruption detection code in
			** btreeInitPage() is executed.  */
			pPage->isInit = 0;
			if( (rc = btreeInitPage(pPage))!=0 ){
				assert( rc==SQLITE_CORRUPT );  /* The only possible error from InitPage */
				checkAppendMsg(pCheck, zContext, 
					"btreeInitPage() returns error code %d", rc);
				releasePage(pPage);
				return 0;
			}

			/* Check out all the cells.
			*/
			depth = 0;
			for(i=0; i<pPage->nCell && pCheck->mxErr; i++){
				u8 *pCell;
				u32 sz;
				CellInfo info;

				/* Check payload overflow pages
				*/
				sqlite3_snprintf(sizeof(zContext), zContext,
					"On tree page %d cell %d: ", iPage, i);
				pCell = findCell(pPage,i);
				btreeParseCellPtr(pPage, pCell, &info);
				sz = info.nData;
				if( !pPage->intKey ) sz += (int)info.nKey;
				/* For intKey pages, check that the keys are in order.
				*/
				else if( i==0 ) nMinKey = nMaxKey = info.nKey;
				else{
					if( info.nKey <= nMaxKey ){
						checkAppendMsg(pCheck, zContext, 
							"Rowid %lld out of order (previous was %lld)", info.nKey, nMaxKey);
					}
					nMaxKey = info.nKey;
				}
				assert( sz==info.nPayload );
				if( (sz>info.nLocal) 
					&& (&pCell[info.iOverflow]<=&pPage->aData[pBt->usableSize])
					){
						int nPage = (sz - info.nLocal + usableSize - 5)/(usableSize - 4);
						Pgno pgnoOvfl = get4byte(&pCell[info.iOverflow]);
#ifndef OMIT_AUTOVACUUM
						if( pBt->autoVacuum ){
							checkPtrmap(pCheck, pgnoOvfl, PTRMAP_OVERFLOW1, iPage, zContext);
						}
#endif
						checkList(pCheck, 0, pgnoOvfl, nPage, zContext);
				}

				/* Check sanity of left child page.
				*/
				if( !pPage->leaf ){
					pgno = get4byte(pCell);
#ifndef OMIT_AUTOVACUUM
					if( pBt->autoVacuum ){
						checkPtrmap(pCheck, pgno, PTRMAP_BTREE, iPage, zContext);
					}
#endif
					d2 = checkTreePage(pCheck, pgno, zContext, &nMinKey, i==0 ? NULL : &nMaxKey);
					if( i>0 && d2!=depth ){
						checkAppendMsg(pCheck, zContext, "Child page depth differs");
					}
					depth = d2;
				}
			}

			if( !pPage->leaf ){
				pgno = get4byte(&pPage->aData[pPage->hdrOffset+8]);
				sqlite3_snprintf(sizeof(zContext), zContext, 
					"On page %d at right child: ", iPage);
#ifndef OMIT_AUTOVACUUM
				if( pBt->autoVacuum ){
					checkPtrmap(pCheck, pgno, PTRMAP_BTREE, iPage, zContext);
				}
#endif
				checkTreePage(pCheck, pgno, zContext, NULL, !pPage->nCell ? NULL : &nMaxKey);
			}

			/* For intKey leaf pages, check that the min/max keys are in order
			** with any left/parent/right pages.
			*/
			if( pPage->leaf && pPage->intKey ){
				/* if we are a left child page */
				if( pnParentMinKey ){
					/* if we are the left most child page */
					if( !pnParentMaxKey ){
						if( nMaxKey > *pnParentMinKey ){
							checkAppendMsg(pCheck, zContext, 
								"Rowid %lld out of order (max larger than parent min of %lld)",
								nMaxKey, *pnParentMinKey);
						}
					}else{
						if( nMinKey <= *pnParentMinKey ){
							checkAppendMsg(pCheck, zContext, 
								"Rowid %lld out of order (min less than parent min of %lld)",
								nMinKey, *pnParentMinKey);
						}
						if( nMaxKey > *pnParentMaxKey ){
							checkAppendMsg(pCheck, zContext, 
								"Rowid %lld out of order (max larger than parent max of %lld)",
								nMaxKey, *pnParentMaxKey);
						}
						*pnParentMinKey = nMaxKey;
					}
					/* else if we're a right child page */
				} else if( pnParentMaxKey ){
					if( nMinKey <= *pnParentMaxKey ){
						checkAppendMsg(pCheck, zContext, 
							"Rowid %lld out of order (min less than parent max of %lld)",
							nMinKey, *pnParentMaxKey);
					}
				}
			}

			/* Check for complete coverage of the page
			*/
			data = pPage->aData;
			hdr = pPage->hdrOffset;
			hit = sqlite3PageMalloc( pBt->pageSize );
			if( hit==0 ){
				pCheck->mallocFailed = 1;
			}else{
				int contentOffset = get2byteNotZero(&data[hdr+5]);
				assert( contentOffset<=usableSize );  /* Enforced by btreeInitPage() */
				memset(hit+contentOffset, 0, usableSize-contentOffset);
				memset(hit, 1, contentOffset);
				nCell = get2byte(&data[hdr+3]);
				cellStart = hdr + 12 - 4*pPage->leaf;
				for(i=0; i<nCell; i++){
					int pc = get2byte(&data[cellStart+i*2]);
					u32 size = 65536;
					int j;
					if( pc<=usableSize-4 ){
						size = cellSizePtr(pPage, &data[pc]);
					}
					if( (int)(pc+size-1)>=usableSize ){
						checkAppendMsg(pCheck, 0, 
							"Corruption detected in cell %d on page %d",i,iPage);
					}else{
						for(j=pc+size-1; j>=pc; j--) hit[j]++;
					}
				}
				i = get2byte(&data[hdr+1]);
				while( i>0 ){
					int size, j;
					assert( i<=usableSize-4 );     /* Enforced by btreeInitPage() */
					size = get2byte(&data[i+2]);
					assert( i+size<=usableSize );  /* Enforced by btreeInitPage() */
					for(j=i+size-1; j>=i; j--) hit[j]++;
					j = get2byte(&data[i]);
					assert( j==0 || j>i+size );  /* Enforced by btreeInitPage() */
					assert( j<=usableSize-4 );   /* Enforced by btreeInitPage() */
					i = j;
				}
				for(i=cnt=0; i<usableSize; i++){
					if( hit[i]==0 ){
						cnt++;
					}else if( hit[i]>1 ){
						checkAppendMsg(pCheck, 0,
							"Multiple uses for byte %d of page %d", i, iPage);
						break;
					}
				}
				if( cnt!=data[hdr+7] ){
					checkAppendMsg(pCheck, 0, 
						"Fragmentation of %d bytes reported as %d on page %d",
						cnt, data[hdr+7], iPage);
				}
			}
			sqlite3PageFree(hit);
			releasePage(pPage);
			return depth+1;
	}

	char *sqlite3BtreeIntegrityCheck(
		Btree *p,     /* The btree to be checked */
		int *aRoot,   /* An array of root pages numbers for individual trees */
		int nRoot,    /* Number of entries in aRoot[] */
		int mxErr,    /* Stop reporting errors after this many */
		int *pnErr    /* Write number of errors seen to this variable */
		){
			Pgno i;
			int nRef;
			IntegrityCk sCheck;
			BtShared *pBt = p->pBt;
			char zErr[100];

			sqlite3BtreeEnter(p);
			assert( p->inTrans>TRANS_NONE && pBt->inTransaction>TRANS_NONE );
			nRef = sqlite3PagerRefcount(pBt->pPager);
			sCheck.pBt = pBt;
			sCheck.pPager = pBt->pPager;
			sCheck.nPage = btreePagecount(sCheck.pBt);
			sCheck.mxErr = mxErr;
			sCheck.nErr = 0;
			sCheck.mallocFailed = 0;
			*pnErr = 0;
			if( sCheck.nPage==0 ){
				sqlite3BtreeLeave(p);
				return 0;
			}

			sCheck.aPgRef = sqlite3MallocZero((sCheck.nPage / 8)+ 1);
			if( !sCheck.aPgRef ){
				*pnErr = 1;
				sqlite3BtreeLeave(p);
				return 0;
			}
			i = PENDING_BYTE_PAGE(pBt);
			if( i<=sCheck.nPage ) setPageReferenced(&sCheck, i);
			sqlite3StrAccumInit(&sCheck.errMsg, zErr, sizeof(zErr), SQLITE_MAX_LENGTH);
			sCheck.errMsg.useMalloc = 2;

			/* Check the integrity of the freelist
			*/
			checkList(&sCheck, 1, get4byte(&pBt->pPage1->aData[32]),
				get4byte(&pBt->pPage1->aData[36]), "Main freelist: ");

			/* Check all the tables.
			*/
			for(i=0; (int)i<nRoot && sCheck.mxErr; i++){
				if( aRoot[i]==0 ) continue;
#ifndef OMIT_AUTOVACUUM
				if( pBt->autoVacuum && aRoot[i]>1 ){
					checkPtrmap(&sCheck, aRoot[i], PTRMAP_ROOTPAGE, 0, 0);
				}
#endif
				checkTreePage(&sCheck, aRoot[i], "List of tree roots: ", NULL, NULL);
			}

			/* Make sure every page in the file is referenced
			*/
			for(i=1; i<=sCheck.nPage && sCheck.mxErr; i++){
#ifdef OMIT_AUTOVACUUM
				if( getPageReferenced(&sCheck, i)==0 ){
					checkAppendMsg(&sCheck, 0, "Page %d is never used", i);
				}
#else
				/* If the database supports auto-vacuum, make sure no tables contain
				** references to pointer-map pages.
				*/
				if( getPageReferenced(&sCheck, i)==0 && 
					(PTRMAP_PAGENO(pBt, i)!=i || !pBt->autoVacuum) ){
						checkAppendMsg(&sCheck, 0, "Page %d is never used", i);
				}
				if( getPageReferenced(&sCheck, i)!=0 && 
					(PTRMAP_PAGENO(pBt, i)==i && pBt->autoVacuum) ){
						checkAppendMsg(&sCheck, 0, "Pointer map page %d is referenced", i);
				}
#endif
			}

			/* Make sure this analysis did not leave any unref() pages.
			** This is an internal consistency check; an integrity check
			** of the integrity check.
			*/
			if( NEVER(nRef != sqlite3PagerRefcount(pBt->pPager)) ){
				checkAppendMsg(&sCheck, 0, 
					"Outstanding page count goes from %d to %d during this analysis",
					nRef, sqlite3PagerRefcount(pBt->pPager)
					);
			}

			/* Clean  up and report errors.
			*/
			sqlite3BtreeLeave(p);
			sqlite3_free(sCheck.aPgRef);
			if( sCheck.mallocFailed ){
				sqlite3StrAccumReset(&sCheck.errMsg);
				*pnErr = sCheck.nErr+1;
				return 0;
			}
			*pnErr = sCheck.nErr;
			if( sCheck.nErr==0 ) sqlite3StrAccumReset(&sCheck.errMsg);
			return sqlite3StrAccumFinish(&sCheck.errMsg);
	}
#endif
#pragma endregion

#pragma region Meta

	/*
	** Return the full pathname of the underlying database file.  Return
	** an empty string if the database is in-memory or a TEMP database.
	**
	** The pager filename is invariant as long as the pager is
	** open so it is safe to access without the BtShared mutex.
	*/
	const char *sqlite3BtreeGetFilename(Btree *p){
		assert( p->pBt->pPager!=0 );
		return sqlite3PagerFilename(p->pBt->pPager, 1);
	}

	/*
	** Return the pathname of the journal file for this database. The return
	** value of this routine is the same regardless of whether the journal file
	** has been created or not.
	**
	** The pager journal filename is invariant as long as the pager is
	** open so it is safe to access without the BtShared mutex.
	*/
	const char *sqlite3BtreeGetJournalname(Btree *p){
		assert( p->pBt->pPager!=0 );
		return sqlite3PagerJournalname(p->pBt->pPager);
	}

	/*
	** Return non-zero if a transaction is active.
	*/
	int sqlite3BtreeIsInTrans(Btree *p){
		assert( p==0 || sqlite3_mutex_held(p->db->mutex) );
		return (p && (p->inTrans==TRANS_WRITE));
	}

#ifndef SQLITE_OMIT_WAL
	/*
	** Run a checkpoint on the Btree passed as the first argument.
	**
	** Return SQLITE_LOCKED if this or any other connection has an open 
	** transaction on the shared-cache the argument Btree is connected to.
	**
	** Parameter eMode is one of SQLITE_CHECKPOINT_PASSIVE, FULL or RESTART.
	*/
	int sqlite3BtreeCheckpoint(Btree *p, int eMode, int *pnLog, int *pnCkpt){
		int rc = SQLITE_OK;
		if( p ){
			BtShared *pBt = p->pBt;
			sqlite3BtreeEnter(p);
			if( pBt->inTransaction!=TRANS_NONE ){
				rc = SQLITE_LOCKED;
			}else{
				rc = sqlite3PagerCheckpoint(pBt->pPager, eMode, pnLog, pnCkpt);
			}
			sqlite3BtreeLeave(p);
		}
		return rc;
	}
#endif

	/*
	** Return non-zero if a read (or write) transaction is active.
	*/
	int sqlite3BtreeIsInReadTrans(Btree *p){
		assert( p );
		assert( sqlite3_mutex_held(p->db->mutex) );
		return p->inTrans!=TRANS_NONE;
	}

	int sqlite3BtreeIsInBackup(Btree *p){
		assert( p );
		assert( sqlite3_mutex_held(p->db->mutex) );
		return p->nBackup!=0;
	}

	/*
	** This function returns a pointer to a blob of memory associated with
	** a single shared-btree. The memory is used by client code for its own
	** purposes (for example, to store a high-level schema associated with 
	** the shared-btree). The btree layer manages reference counting issues.
	**
	** The first time this is called on a shared-btree, nBytes bytes of memory
	** are allocated, zeroed, and returned to the caller. For each subsequent 
	** call the nBytes parameter is ignored and a pointer to the same blob
	** of memory returned. 
	**
	** If the nBytes parameter is 0 and the blob of memory has not yet been
	** allocated, a null pointer is returned. If the blob has already been
	** allocated, it is returned as normal.
	**
	** Just before the shared-btree is closed, the function passed as the 
	** xFree argument when the memory allocation was made is invoked on the 
	** blob of allocated memory. The xFree function should not call sqlite3_free()
	** on the memory, the btree layer does that.
	*/
	void *sqlite3BtreeSchema(Btree *p, int nBytes, void(*xFree)(void *)){
		BtShared *pBt = p->pBt;
		sqlite3BtreeEnter(p);
		if( !pBt->pSchema && nBytes ){
			pBt->pSchema = sqlite3DbMallocZero(0, nBytes);
			pBt->xFreeSchema = xFree;
		}
		sqlite3BtreeLeave(p);
		return pBt->pSchema;
	}

	/*
	** Return SQLITE_LOCKED_SHAREDCACHE if another user of the same shared 
	** btree as the argument handle holds an exclusive lock on the 
	** sqlite_master table. Otherwise SQLITE_OK.
	*/
	int sqlite3BtreeSchemaLocked(Btree *p){
		int rc;
		assert( sqlite3_mutex_held(p->db->mutex) );
		sqlite3BtreeEnter(p);
		rc = querySharedCacheTableLock(p, MASTER_ROOT, READ_LOCK);
		assert( rc==SQLITE_OK || rc==SQLITE_LOCKED_SHAREDCACHE );
		sqlite3BtreeLeave(p);
		return rc;
	}


#ifndef SQLITE_OMIT_SHARED_CACHE
	/*
	** Obtain a lock on the table whose root page is iTab.  The
	** lock is a write lock if isWritelock is true or a read lock
	** if it is false.
	*/
	int sqlite3BtreeLockTable(Btree *p, int iTab, u8 isWriteLock){
		int rc = SQLITE_OK;
		assert( p->inTrans!=TRANS_NONE );
		if( p->sharable ){
			u8 lockType = READ_LOCK + isWriteLock;
			assert( READ_LOCK+1==WRITE_LOCK );
			assert( isWriteLock==0 || isWriteLock==1 );

			sqlite3BtreeEnter(p);
			rc = querySharedCacheTableLock(p, iTab, lockType);
			if( rc==SQLITE_OK ){
				rc = setSharedCacheTableLock(p, iTab, lockType);
			}
			sqlite3BtreeLeave(p);
		}
		return rc;
	}
#endif

#ifndef SQLITE_OMIT_INCRBLOB
	/*
	** Argument pCsr must be a cursor opened for writing on an 
	** INTKEY table currently pointing at a valid table entry. 
	** This function modifies the data stored as part of that entry.
	**
	** Only the data content may only be modified, it is not possible to 
	** change the length of the data stored. If this function is called with
	** parameters that attempt to write past the end of the existing data,
	** no modifications are made and SQLITE_CORRUPT is returned.
	*/
	int sqlite3BtreePutData(BtCursor *pCsr, u32 offset, u32 amt, void *z){
		int rc;
		assert( cursorHoldsMutex(pCsr) );
		assert( sqlite3_mutex_held(pCsr->pBtree->db->mutex) );
		assert( pCsr->isIncrblobHandle );

		rc = restoreCursorPosition(pCsr);
		if( rc!=SQLITE_OK ){
			return rc;
		}
		assert( pCsr->eState!=CURSOR_REQUIRESEEK );
		if( pCsr->eState!=CURSOR_VALID ){
			return SQLITE_ABORT;
		}

		/* Check some assumptions: 
		**   (a) the cursor is open for writing,
		**   (b) there is a read/write transaction open,
		**   (c) the connection holds a write-lock on the table (if required),
		**   (d) there are no conflicting read-locks, and
		**   (e) the cursor points at a valid row of an intKey table.
		*/
		if( !pCsr->wrFlag ){
			return SQLITE_READONLY;
		}
		assert( (pCsr->pBt->btsFlags & BTS_READ_ONLY)==0
			&& pCsr->pBt->inTransaction==TRANS_WRITE );
		assert( hasSharedCacheTableLock(pCsr->pBtree, pCsr->pgnoRoot, 0, 2) );
		assert( !hasReadConflicts(pCsr->pBtree, pCsr->pgnoRoot) );
		assert( pCsr->apPage[pCsr->iPage]->intKey );

		return accessPayload(pCsr, offset, amt, (unsigned char *)z, 1);
	}

	/* 
	** Set a flag on this cursor to cache the locations of pages from the 
	** overflow list for the current row. This is used by cursors opened
	** for incremental blob IO only.
	**
	** This function sets a flag only. The actual page location cache
	** (stored in BtCursor.aOverflow[]) is allocated and used by function
	** accessPayload() (the worker function for sqlite3BtreeData() and
	** sqlite3BtreePutData()).
	*/
	void sqlite3BtreeCacheOverflow(BtCursor *pCur){
		assert( cursorHoldsMutex(pCur) );
		assert( sqlite3_mutex_held(pCur->pBtree->db->mutex) );
		invalidateOverflowCache(pCur);
		pCur->isIncrblobHandle = 1;
	}
#endif

	/*
	** Set both the "read version" (single byte at byte offset 18) and 
	** "write version" (single byte at byte offset 19) fields in the database
	** header to iVersion.
	*/
	int sqlite3BtreeSetVersion(Btree *pBtree, int iVersion){
		BtShared *pBt = pBtree->pBt;
		int rc;                         /* Return code */

		assert( iVersion==1 || iVersion==2 );

		/* If setting the version fields to 1, do not automatically open the
		** WAL connection, even if the version fields are currently set to 2.
		*/
		pBt->btsFlags &= ~BTS_NO_WAL;
		if( iVersion==1 ) pBt->btsFlags |= BTS_NO_WAL;

		rc = sqlite3BtreeBeginTrans(pBtree, 0);
		if( rc==SQLITE_OK ){
			u8 *aData = pBt->pPage1->aData;
			if( aData[18]!=(u8)iVersion || aData[19]!=(u8)iVersion ){
				rc = sqlite3BtreeBeginTrans(pBtree, 2);
				if( rc==SQLITE_OK ){
					rc = sqlite3PagerWrite(pBt->pPage1->pDbPage);
					if( rc==SQLITE_OK ){
						aData[18] = (u8)iVersion;
						aData[19] = (u8)iVersion;
					}
				}
			}
		}

		pBt->btsFlags &= ~BTS_NO_WAL;
		return rc;
	}

	/*
	** set the mask of hint flags for cursor pCsr. Currently the only valid
	** values are 0 and BTREE_BULKLOAD.
	*/
	void sqlite3BtreeCursorHints(BtCursor *pCsr, unsigned int mask){
		assert( mask==BTREE_BULKLOAD || mask==0 );
		pCsr->hints = mask;
	}
}