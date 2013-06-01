// wal.c
#include "Core+Pager.cu.h"
using namespace Core;

class Wal
{
public:
	static RC Open(VFileSystem *vfs, VFile *dbFile, const char *walName, bool noShm, int64 maxWalSize, Wal **walOut);
	static void Limit(Wal *wal, int64 limit);

#ifdef OMIT_WAL
	static RC Open(VFileSystem *x, VFile *y, char *z) { return RC.OK; }
	static void Limit(Wal *a, long y) { }
	static RC Close(Wal *a, int x, int y, byte z) { return RC::OK; }
	static RC BeginReadTransaction(Wal *a, int z) { return RC::OK; }
	static void EndReadTransaction(Wal *a) { }
	static RC Read(Wal *a, Pid w, ref int x, int y, byte[] z) { return RC::OK; }
	static Pid DBSize(Wal *a) { return 0; }
	static RC BeginWriteTransaction(Wal *a) { return RC::OK; }
	static RC EndWriteTransaction(Wal *a) { return RC::OK; }
	static RC Undo(Wal *a, int y, object z) { return RC::OK; }
	static void Savepoint(Wal *a, object z) { }
	static RC SavepointUndo(Wal *a, object z) { return RC::OK; }
	static RC Frames(Wal *a, int v, PgHdr w, Pid x, int y, int z) { return RC::OK; }
	static RC Checkpoint(Wal *a, int s, int t, byte[] u, int v, int w, byte[] x, ref int y, ref int z) { y = 0; z = 0; return RC::OK; }
	static RC Callback(Wal *a) { return RC::OK; }
	static bool ExclusiveMode(Wal *a, int z) { return false; }
	static bool HeapMemory(Wal *a) { return false; }
#endif
};



//#define Wal_Open(x,y,z)                   0
//#define Wal_Limit(x,y)
//#define Wal_Close(w,x,y,z)                0
//#define Wal_BeginReadTransaction(y,z)     0
//#define Wal_EndReadTransaction(z)
//#define Wal_Read(v,w,x,y,z)               0
//#define Wal_Dbsize(y)                     0
//#define Wal_BeginWriteTransaction(y)      0
//#define Wal_EndWriteTransaction(x)        0
//#define Wal_Undo(x,y,z)                   0
//#define Wal_Savepoint(y,z)
//#define Wal_SavepointUndo(y,z)            0
//#define Wal_Frames(u,v,w,x,y,z)           0
//#define Wal_Checkpoint(r,s,t,u,v,w,x,y,z) 0
//#define Wal_Callback(z)                   0
//#define Wal_ExclusiveMode(y,z)            0
//#define Wal_HeapMemory(z)                 0
//#define Wal_Framesize(z)                  0

