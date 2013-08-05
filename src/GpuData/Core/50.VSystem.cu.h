// sqlite.h
namespace Core { namespace IO
{
	typedef class VFile VFile;

	class VSystem
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
			MEMORY = 0x00000080,            // Ok for sqlite3_open_v2()
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

		VSystem *Next;	// Next registered VFS
		const char *Name;	// Name of this virtual file system
		void *Tag;			// Pointer to application-specific data
		int SizeOsFile;     // Size of subclassed VirtualFile
		int MaxPathname;	// Maximum file pathname length

		__device__ static VSystem *Find(const char *name);
		__device__ static int RegisterVfs(VSystem *vfs, bool _default);
		__device__ static int UnregisterVfs(VSystem *vfs);
		//
		__device__ virtual RC Open(const char *path, VFile *file, OPEN flags, OPEN *outFlags) = 0;
		__device__ virtual RC Delete(const char *path, bool syncDirectory) = 0;
		__device__ virtual RC Access(const char *path, ACCESS flags, int *outRC) = 0;
		__device__ virtual RC FullPathname(const char *path, int pathOutLength, char *pathOut) = 0;
	};

#if defined(TEST) || defined(_DEBUG)
	bool OsTrace = false;
#define OSTRACE(X, ...) if (OsTrace) { printf(X, __VA_ARGS__); }
#else
#define OSTRACE(X, ...)
#endif

#ifdef TEST
	int io_error_hit = 0;            // Total number of I/O Errors
	int io_error_hardhit = 0;        // Number of non-benign errors
	int io_error_pending = 0;        // Count down to first I/O error
	int io_error_persist = 0;        // True if I/O errors persist
	int io_error_benign = 0;         // True if errors are benign
	int diskfull_pending = 0;
	int diskfull = 0;
#define SimulateIOErrorBenign(X) io_error_benign=(X)
#define SimulateIOError(CODE) \
	if ((io_error_persist && io_error_hit) || io_error_pending-- == 1) { local_ioerr(); CODE; }
	static void local_ioerr() { OSTRACE("IOERR\n"); io_error_hit++; if (!io_error_benign) io_error_hardhit++; }
#define SimulateDiskfullError(CODE) \
	if (diskfull_pending) { if (diskfull_pending == 1) { \
	local_ioerr(); diskfull = 1; io_error_hit = 1; CODE; \
	} else diskfull_pending--; }
#else
#define SimulateIOErrorBenign(X)
#define SimulateIOError(A)
#define SimulateDiskfullError(A)
#endif

	// When testing, keep a count of the number of open files.
#ifdef TEST
	int open_file_count = 0;
#define OpenCounter(X) open_file_count += (X)
#else
#define OpenCounter(X)
#endif

	VSystem::OPEN inline operator |= (VSystem::OPEN a, VSystem::OPEN b) { return (VSystem::OPEN)(a | b); }
}}