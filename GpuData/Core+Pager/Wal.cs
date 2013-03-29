using Core.IO;
using Pgno = System.UInt32;
namespace Core
{
    public class Wal
    {
        internal static RC Open(VFileSystem x, VFile y, string z) { return RC.OK; }

        public static class WalExtensions
        {
            internal static void Limit(this Wal a, long y) { }
            internal static RC Close(this Wal a, int x, int y, byte z) { return 0; }
            internal static RC BeginReadTransaction(this Wal a, int z) { return 0; }
            internal static void EndReadTransaction(this Wal a) { }
            internal static RC Read(this Wal a, Pgno w, ref int x, int y, byte[] z) { return 0; }
            internal static Pgno DBSize(this Wal a) { return 0; }
            internal static RC BeginWriteTransaction(this Wal a) { return 0; }
            internal static RC EndWriteTransaction(this Wal a) { return 0; }
            internal static RC Undo(this Wal a, int y, object z) { return 0; }
            internal static void Savepoint(this Wal a, object z) { }
            internal static RC SavepointUndo(this Wal a, object z) { return 0; }
            internal static RC Frames(this Wal a, int v, PgHdr w, Pgno x, int y, int z) { return 0; }
            internal static RC Checkpoint(this Wal a, int s, int t, byte[] u, int v, int w, byte[] x, ref int y, ref int z) { y = 0; z = 0; return 0; }
            internal static RC Callback(this Wal a) { return 0; }
            internal static bool ExclusiveMode(this Wal a, int z) { return false; }
            internal static bool HeapMemory(this Wal a) { return false; }
        }
    }
}