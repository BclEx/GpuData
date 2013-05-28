using System;
using Pgno = System.UInt32;
namespace Core
{
    public partial class Pager
    {
#if !OMIT_WAL
//        static int pagerUndoCallback(Pager pCtx, Pgno iPg)
//        {
//            var rc = RC.OK;
//            Pager pPager = (Pager)pCtx;
//            PgHdr pPg;

//            pPg = sqlite3PagerLookup(pPager, iPg);
//            if (pPg)
//            {
//                if (sqlite3PcachePageRefcount(pPg) == 1)
//                {
//                    sqlite3PcacheDrop(pPg);
//                }
//                else
//                {
//                    rc = readDbPage(pPg);
//                    if (rc == SQLITE.OK)
//                    {
//                        pPager._reiniter(pPg);
//                    }
//                    sqlite3PagerUnref(pPg);
//                }
//            }
//            // Normally, if a transaction is rolled back, any backup processes are updated as data is copied out of the rollback journal and into the
//            // database. This is not generally possible with a WAL database, as rollback involves simply truncating the log file. Therefore, if one
//            // or more frames have already been written to the log (and therefore also copied into the backup databases) as part of this transaction,
//            // the backups must be restarted.
//            sqlite3BackupRestart(pPager.pBackup);
//            return rc;
//        }

//        static int pagerRollbackWal(Pager pPager)
//        {
//            int rc;                         /* Return Code */
//            PgHdr pList;                   /* List of dirty pages to revert */

//            /* For all pages in the cache that are currently dirty or have already been written (but not committed) to the log file, do one of the 
//            ** following:
//            **   + Discard the cached page (if refcount==0), or
//            **   + Reload page content from the database (if refcount>0).
//            */
//            pPager._dbSize = pPager._dbOrigSize;
//            rc = sqlite3WalUndo(pPager._wal, pagerUndoCallback, pPager);
//            pList = sqlite3PcacheDirtyList(pPager._pcache);
//            while (pList && rc == SQLITE.OK)
//            {
//                PgHdr pNext = pList.pDirty;
//                rc = pagerUndoCallback(pPager, pList.pgno);
//                pList = pNext;
//            }

//            return rc;
//        }


//        static int pagerWalFrames(Pager pPager, PgHdr pList, Pgno nTruncate, int isCommit, int syncFlags)
//        {
//            int rc;                         /* Return code */
//#if DEBUG || CHECK_PAGES
//            PgHdr p;                       /* For looping over pages */
//#endif

//            Debug.Assert(pPager._wal);
//#if DEBUG
///* Verify that the page list is in accending order */
//for(p=pList; p && p->pDirty; p=p->pDirty){
//assert( p->pgno < p->pDirty->pgno );
//}
//#endif

//            if (isCommit)
//            {
//                /* If a WAL transaction is being committed, there is no point in writing
//                ** any pages with page numbers greater than nTruncate into the WAL file.
//                ** They will never be read by any client. So remove them from the pDirty
//                ** list here. */
//                PgHdr* p;
//                PgHdr** ppNext = &pList;
//                for (p = pList; (*ppNext = p); p = p->pDirty)
//                {
//                    if (p->pgno <= nTruncate) ppNext = &p->pDirty;
//                }
//                assert(pList);
//            }


//            if (pList->pgno == 1) pager_write_changecounter(pList);
//            rc = sqlite3WalFrames(pPager._wal,
//            pPager._pageSize, pList, nTruncate, isCommit, syncFlags
//            );
//            if (rc == SQLITE.OK && pPager.pBackup)
//            {
//                PgHdr* p;
//                for (p = pList; p; p = p->pDirty)
//                {
//                    sqlite3BackupUpdate(pPager.pBackup, p->pgno, (u8*)p->pData);
//                }
//            }

//#if CHECK_PAGES
//pList = sqlite3PcacheDirtyList(pPager.pPCache);
//for(p=pList; p; p=p->pDirty){
//pager_set_pagehash(p);
//}
//#endif

//            return rc;
//        }

//        static int pagerBeginReadTransaction(Pager* pPager)
//        {
//            int rc;                         /* Return code */
//            int changed = 0;                /* True if cache must be reset */

//            assert(_pagerUseWal(pPager));
//            assert(pPager.eState == PAGER_OPEN || pPager.eState == PAGER_READER);

//            /* sqlite3WalEndReadTransaction() was not called for the previous
//            ** transaction in locking_mode=EXCLUSIVE.  So call it now.  If we
//            ** are in locking_mode=NORMAL and EndRead() was previously called,
//            ** the duplicate call is harmless.
//            */
//            sqlite3WalEndReadTransaction(pPager.pWal);

//            rc = sqlite3WalBeginReadTransaction(pPager.pWal, &changed);
//            if (rc != SQLITE.OK || changed)
//            {
//                pager_reset(pPager);
//            }

//            return rc;
//        }

        //static int pagerOpenWalIfPresent(Pager* pPager)
        //{
        //    int rc = SQLITE.OK;
        //    Debug.Assert(pPager.eState == PAGER_OPEN);
        //    Debug.Assert(pPager.eLock >= SHARED_LOCK || pPager.noReadlock);

        //    if (!pPager.tempFile)
        //    {
        //        int isWal;                    /* True if WAL file exists */
        //        Pgno nPage;                   /* Size of the database file */

        //        rc = pagerPagecount(pPager, &nPage);
        //        if (rc) return rc;
        //        if (nPage == 0)
        //        {
        //            rc = sqlite3OsDelete(pPager.pVfs, pPager.zWal, 0);
        //            isWal = 0;
        //        }
        //        else
        //        {
        //            rc = sqlite3OsAccess(
        //            pPager.pVfs, pPager.zWal, SQLITE_ACCESS_EXISTS, &isWal
        //            );
        //        }
        //        if (rc == SQLITE.OK)
        //        {
        //            if (isWal)
        //            {
        //                testcase(sqlite3PcachePagecount(pPager.pPCache) == 0);
        //                rc = sqlite3PagerOpenWal(pPager, 0);
        //            }
        //            else if (pPager.journalMode == PAGER_JOURNALMODE_WAL)
        //            {
        //                pPager.journalMode = PAGER_JOURNALMODE_DELETE;
        //            }
        //        }
        //    }
        //    return rc;
        //}

int sqlite3PagerCheckpoint(Pager *pPager, int eMode, int *pnLog, int *pnCkpt){
  int rc = SQLITE.OK;
  if( pPager.pWal ){
    rc = sqlite3WalCheckpoint(pPager.pWal, eMode,
        pPager.xBusyHandler, pPager.pBusyHandlerArg,
        pPager.ckptSyncFlags, pPager.pageSize, (u8 *)pPager.pTmpSpace,
        pnLog, pnCkpt
    );
  }
  return rc;
}

    int sqlite3PagerWalCallback(Pager *pPager){
return sqlite3WalCallback(pPager.pWal);
}

int sqlite3PagerWalSupported(Pager *pPager){
const sqlite3_io_methods *pMethods = pPager.fd->pMethods;
return pPager.exclusiveMode || (pMethods->iVersion>=2 && pMethods->xShmMap);
}

static int pagerExclusiveLock(Pager *pPager){
int rc;                         /* Return code */

assert( pPager.eLock==SHARED_LOCK || pPager.eLock==EXCLUSIVE_LOCK );
rc = pagerLockDb(pPager, EXCLUSIVE_LOCK);
if( rc!=SQLITE.OK ){
/* If the attempt to grab the exclusive lock failed, release the
** pending lock that may have been obtained instead.  */
pagerUnlockDb(pPager, SHARED_LOCK);
}

return rc;
}

static int pagerOpenWal(Pager *pPager){
int rc = SQLITE.OK;

assert( pPager.pWal==0 && pPager.tempFile==0 );
assert( pPager.eLock==SHARED_LOCK || pPager.eLock==EXCLUSIVE_LOCK || pPager.noReadlock);

/* If the pager is already in exclusive-mode, the WAL module will use 
** heap-memory for the wal-index instead of the VFS shared-memory 
** implementation. Take the exclusive lock now, before opening the WAL
** file, to make sure this is safe.
*/
if( pPager.exclusiveMode ){
rc = pagerExclusiveLock(pPager);
}

/* Open the connection to the log file. If this operation fails, 
** (e.g. due to malloc() failure), return an error code.
*/
if( rc==SQLITE.OK ){
rc = sqlite3WalOpen(pPager.pVfs, 
pPager.fd, pPager.zWal, pPager.exclusiveMode, &pPager.pWal
        pPager.journalSizeLimit, &pPager.pWal
);
}

return rc;
}


int sqlite3PagerOpenWal(
Pager *pPager,                  /* Pager object */
int *pbOpen                     /* OUT: Set to true if call is a no-op */
){
int rc = SQLITE.OK;             /* Return code */

assert( assert_pager_state(pPager) );
assert( pPager.eState==PAGER_OPEN   || pbOpen );
assert( pPager.eState==PAGER_READER || !pbOpen );
assert( pbOpen==0 || *pbOpen==0 );
assert( pbOpen!=0 || (!pPager.tempFile && !pPager.pWal) );

if( !pPager.tempFile && !pPager.pWal ){
if( !sqlite3PagerWalSupported(pPager) ) return SQLITE_CANTOPEN;

/* Close any rollback journal previously open */
sqlite3OsClose(pPager.jfd);

rc = pagerOpenWal(pPager);
if( rc==SQLITE.OK ){
pPager.journalMode = PAGER_JOURNALMODE_WAL;
pPager.eState = PAGER_OPEN;
}
}else{
*pbOpen = 1;
}

return rc;
}

int sqlite3PagerCloseWal(Pager *pPager){
int rc = SQLITE.OK;

assert( pPager.journalMode==PAGER_JOURNALMODE_WAL );

/* If the log file is not already open, but does exist in the file-system,
** it may need to be checkpointed before the connection can switch to
** rollback mode. Open it now so this can happen.
*/
if( !pPager.pWal ){
int logexists = 0;
rc = pagerLockDb(pPager, SHARED_LOCK);
if( rc==SQLITE.OK ){
rc = sqlite3OsAccess(
pPager.pVfs, pPager.zWal, SQLITE_ACCESS_EXISTS, &logexists
);
}
if( rc==SQLITE.OK && logexists ){
rc = pagerOpenWal(pPager);
}
}

/* Checkpoint and close the log. Because an EXCLUSIVE lock is held on
** the database file, the log and log-summary files will be deleted.
*/
if( rc==SQLITE.OK && pPager.pWal ){
rc = pagerExclusiveLock(pPager);
if( rc==SQLITE.OK ){
rc = sqlite3WalClose(pPager.pWal, pPager.ckptSyncFlags,
           pPager.pageSize, (u8*)pPager.pTmpSpace);
pPager.pWal = 0;
}
}
return rc;
}

#endif
    }
}
