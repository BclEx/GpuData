using Core;
using Core.IO;
using System;

namespace GpuData
{
    class MainClass
    {
        public static void Main(string[] args)
        {
            SysEx.Initialize();
            //TestVFS();
            TestPager();
            //Console.ReadKey();
        }

        static void TestVFS()
        {
            var vfs = VSystem.FindVfs("win32");
            if (vfs == null)
                throw new InvalidOperationException();
            var file = vfs.CreateOsFile();
            VSystem.OPEN flagOut;
            var rc = vfs.Open(@"C:\T_\Test.db", file, VSystem.OPEN.CREATE | VSystem.OPEN.READWRITE | VSystem.OPEN.MAIN_DB, out flagOut);
            if (rc != RC.OK)
                throw new InvalidOperationException();
            file.Write4(0, 12345);
            file.Close();
        }

        static int BusyHandler(object x) { Console.WriteLine("BUSY"); return -1; }

        static Pager Open(VSystem vfs)
        {
            var dbHeader = new byte[100]; // Database header content

            IPager.PAGEROPEN flags = 0;
            var vfsFlags = VSystem.OPEN.CREATE | VSystem.OPEN.READWRITE | VSystem.OPEN.MAIN_DB;
            //
            Pager pager;
            var rc = Pager.Open(vfs, out pager, @"C:\T_\Test.db", 0, flags, vfsFlags, x => { }, null);
            if (rc == RC.OK)
                rc = pager.ReadFileHeader(dbHeader.Length, dbHeader);
            if (rc != RC.OK)
                goto _out;
            pager.SetBusyHandler(BusyHandler, null);
            var readOnly = pager.get_Readonly();
            //
            int reserves;
            var pageSize = (uint)((dbHeader[16] << 8) | (dbHeader[17] << 16));
            if (pageSize < 512 || pageSize > Pager.MAX_PAGE_SIZE || ((pageSize - 1) & pageSize) != 0)
            {
                pageSize = 0;
                reserves = 0;
            }
            else
                reserves = dbHeader[20];
            rc = pager.SetPageSize(ref pageSize, reserves);
            if (rc != RC.OK) goto _out;
        _out:
            if (rc != RC.OK)
            {
                if (pager != null)
                    pager.Close();
                pager = null;
            }
            else
                pager.SetCacheSize(2000);
            return pager;
        }

        static void TestPager()
        {
            var vfs = VSystem.FindVfs("win32");
            var pager = Open(vfs);
            if (pager == null)
                throw new Exception();
            var rc = pager.SharedLock();
            if (rc != RC.OK)
                throw new Exception();
            //
            PgHdr p = null;
            rc = pager.Acquire(1, ref p, false);
            if (rc != RC.OK)
                throw new Exception();
            rc = pager.Begin(false, false);
            if (rc != RC.OK)
                throw new Exception();
            var values = new byte[] { 0, 1, 2, 3, 4, 5, 6, 7, 8, 9 };
            Array.Copy(values, p.Data, 10);
            Pager.Write(p);
            pager.CommitPhaseOne(null, false);
            pager.CommitPhaseTwo();
            //
            if (pager != null)
                pager.Close();
        }
    }
}
