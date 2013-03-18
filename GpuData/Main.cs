using Core.IO;
using System;

namespace GpuData
{
    class MainClass
    {
        public static void Main(string[] args)
        {
            TestVFS();
            Console.ReadKey();
        }

        private static void TestVFS()
        {
            var vfs = VFileSystem.FindVfs("win32");
            if (vfs == null)
                throw new InvalidOperationException();
            var file = new CoreVFile();
            VFileSystem.OPEN flagOut;
            var rc = vfs.Open(@"Test", file, VFileSystem.OPEN.CREATE, out flagOut);
        }
    }
}
