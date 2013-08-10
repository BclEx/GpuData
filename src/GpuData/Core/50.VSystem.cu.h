// sqlite.h
#pragma once
namespace Core { namespace IO
{
	typedef class VFile VFile;
	typedef void (*syscall_ptr)();

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

		enum class ACCESS
		{
			EXISTS = 0,
			READWRITE = 1,	// Used by PRAGMA temp_store_directory
			READ = 2,		// Unused
		};

		VSystem *Next;	// Next registered VFS
		const char *Name;	// Name of this virtual file system
		void *Tag;			// Pointer to application-specific data
		int SizeOsFile;     // Size of subclassed VirtualFile
		int MaxPathname;	// Maximum file pathname length

		__device__ static RC Initialize();
		__device__ static void Shutdown();

		__device__ static VSystem *Find(const char *name);
		__device__ static int RegisterVfs(VSystem *vfs, bool _default);
		__device__ static int UnregisterVfs(VSystem *vfs);

		__device__ virtual RC Open(const char *path, VFile *file, OPEN flags, OPEN *outFlags) = 0;
		__device__ virtual RC Delete(const char *path, bool syncDirectory) = 0;
		__device__ virtual RC Access(const char *path, ACCESS flags, int *outRC) = 0;
		__device__ virtual RC FullPathname(const char *path, int pathOutLength, char *pathOut) = 0;

		__device__ virtual void *DlOpen(const char *filename) = 0;
		__device__ virtual void DlError(int bufLength, char *buf) = 0;
		__device__ virtual void (*DlSym(void *handle, const char *symbol))() = 0;
		__device__ virtual void DlClose(void *handle) = 0;

		__device__ virtual int Randomness(int bufLength, char *buf) = 0;
		__device__ virtual int Sleep(int microseconds) = 0;
		__device__ virtual RC CurrentTimeInt64(int64 *now) = 0;
		__device__ virtual RC CurrentTime(double *now) = 0;
		__device__ virtual RC GetLastError(int bufLength, char *buf) = 0;

		__device__ virtual RC SetSystemCall(const char *name, syscall_ptr newFunc) = 0;
		__device__ virtual syscall_ptr GetSystemCall(const char *name) = 0;
		__device__ virtual const char *NextSystemCall(const char *name) = 0;
	};

	VSystem::OPEN inline operator | (VSystem::OPEN a, VSystem::OPEN b) { return (VSystem::OPEN)(a | b); }
	VSystem::OPEN inline operator |= (VSystem::OPEN a, VSystem::OPEN b) { return (VSystem::OPEN)(a | b); }
}}