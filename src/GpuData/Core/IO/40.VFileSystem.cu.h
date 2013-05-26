// sqlite.h
namespace Core
{
	class VFileSystem
	{
	public:
		enum OPEN : unsigned int
		{
			READONLY = 0x00000001,          // Ok for sqlite3_open_v2() 
			OREADWRITE = 0x00000002,        // Ok for sqlite3_open_v2() 
			CREATE = 0x00000004,            // Ok for sqlite3_open_v2() 
			DELETEONCLOSE = 0x00000008,     // VFS only 
			EXCLUSIVE = 0x00000010,         // VFS only 
			AUTOPROXY = 0x00000020,         // VFS only 
			URI = 0x00000040,               // Ok for sqlite3_open_v2() 
			MAIN_DB = 0x00000100,           // VFS only 
			TEMP_DB = 0x00000200,           // VFS only 
			TRANSIENT_DB = 0x00000400,      // VFS only 
			MAIN_JOURNAL = 0x00000800,      // VFS only 
			TEMP_JOURNAL = 0x00001000,      // VFS only 
			SUBJOURNAL = 0x00002000,        // VFS only 
			MASTER_JOURNAL = 0x00004000,    // VFS only 
			NOMUTEX = 0x00008000,           // Ok for sqlite3_open_v2() 
			FULLMUTEX = 0x00010000,         // Ok for sqlite3_open_v2() 
			SHAREDCACHE = 0x00020000,       // Ok for sqlite3_open_v2() 
			PRIVATECACHE = 0x00040000,      // Ok for sqlite3_open_v2() 
			WAL = 0x00080000,               // VFS only 
		};

		enum ACCESS
		{
			EXISTS = 0,
			AREADWRITE = 1,	// Used by PRAGMA temp_store_directory
			READ = 2,		// Unused
		};

		VFileSystem *Next;	// Next registered VFS
		const char *Name;	// Name of this virtual file system
		void *Tag;			// Pointer to application-specific data
        int SizeOsFile;     // Size of subclassed VirtualFile
        int MaxPathname;	// Maximum file pathname length

		static VFileSystem *Find(const char *name);
		static int RegisterVfs(VFileSystem *vfs, bool _default);
		static int UnregisterVfs(VFileSystem *vfs);
		//
		virtual RC Open(const char *path, VFile *file, OPEN flags, OPEN *outFlags) = 0;
		virtual RC Delete(const char *path, bool syncDirectory) = 0;
		virtual RC Access(const char *path, ACCESS flags, int *outRC) = 0;
		virtual RC FullPathname(const char *path, char **outPath) = 0;
	};
}
