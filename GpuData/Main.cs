using Core;
using Core.IO;
using System;

namespace GpuData
{
    class MainClass
    {
        public static void Main(string[] args)
        {
            TestVFS();
            //Console.ReadKey();
        }

        private static void TestVFS()
        {
            var vfs = VFileSystem.FindVfs("win32");
            if (vfs == null)
                throw new InvalidOperationException();
            var file = new CoreVFile();
            VFileSystem.OPEN flagOut;
            var rc = vfs.Open(@"Test", file, VFileSystem.OPEN.READWRITE | VFileSystem.OPEN.CREATE | VFileSystem.OPEN.MAIN_DB, out flagOut);
        }

        private static void TestPager()
        {
            var vfs = VFileSystem.FindVfs(null);
            var pager = vfs.Open();
            if (pager == null)
                throw new Exception();
            var rc = pager.SharedLock();
            if (rc != RC.OK)
                throw new Exception();
            //
            PgHdr p = null;
            rc = pager.Get(1, ref p, 0);
            if (rc != RC.OK)
                throw new Exception();
            rc = pager.Begin(false, false);
            if (rc != RC.OK)
                throw new Exception();
            Array.Copy(new byte[] { 0, 1, 2, 3, 4, 5, 6, 7, 8, 9 }, p.Data, 10);
            Pager.Write(p);
            pager.CommitPhaseOne(null, false);
            pager.CommitPhaseTwo();
            //
            if (pager != null)
                pager.Close();
        }

        private static Pager Open(VFileSystem vfs)
        {
            var zDbHeader = new byte[100]; // Database header content
            Pager pager;
            Pager.PAGEROPEN flags = 0;
            var vfsFlags = VFileSystem.OPEN.CREATE | VFileSystem.OPEN.READWRITE | VFileSystem.OPEN.MAIN_DB;
            //
            var rc = Pager.Open(vfs, out pager, @"Test", 0, flags, vfsFlags, x => { }, null);
            if (rc == RC.OK)
                rc = pager.ReadFileHeader(zDbHeader.Length, zDbHeader);
            pager.SetBusyHandler(BusyHandler, null);
            var readOnly = pager.IsReadonly;
            //
            int nReserve;
            var pageSize = (uint)((zDbHeader[16] << 8) | (zDbHeader[17] << 16));
            if (pageSize < 512 || pageSize > Pager.SQLITE_MAX_PAGE_SIZE || ((pageSize - 1) & pageSize) != 0)
            {
                pageSize = 0;
                nReserve = 0;
            }
            else
                nReserve = zDbHeader[20];
            rc = pager.SetPageSize(ref pageSize, nReserve);
            if (rc != RC.OK)
                goto _out;
        _out:
            if (rc != RC.OK)
            {
                if (pager != null)
                    pager.Close();
                pager = null;
            }
            pager.SetCacheSize(2000);
            return pager;
        }

        private static int BusyHandler(object x) { Console.WriteLine("BUSY"); return -1; }

    }
}
