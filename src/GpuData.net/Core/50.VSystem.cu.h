// sqlite.h
#pragma once
namespace Core { namespace IO { typedef class VFile VFile; }}
namespace Core
{

#ifndef OS_WIN
#if defined(_WIN32) || defined(WIN32) || defined(__CYGWIN__) || defined(__MINGW32__) || defined(__BORLANDC__)
#define OS_WIN 1
#define OS_UNIX 0
#else
#define OS_WIN 0
#define OS_UNIX 1
#endif
#else
#define OS_UNIX 0
#endif

	typedef void (*syscall_ptr)();

	class VSystem
	{
	public:
		enum OPEN : int
		{
			OPEN_READONLY = 0x00000001,          // Ok for sqlite3_open_v2() 
			OPEN_READWRITE = 0x00000002,        // Ok for sqlite3_open_v2() 
			OPEN_CREATE = 0x00000004,            // Ok for sqlite3_open_v2() 
			OPEN_DELETEONCLOSE = 0x00000008,     // VFS only 
			OPEN_EXCLUSIVE = 0x00000010,         // VFS only 
			OPEN_AUTOPROXY = 0x00000020,         // VFS only 
			OPEN_URI = 0x00000040,               // Ok for sqlite3_open_v2() 
			OPEN_MEMORY = 0x00000080,            // Ok for sqlite3_open_v2()
			OPEN_MAIN_DB = 0x00000100,           // VFS only 
			OPEN_TEMP_DB = 0x00000200,           // VFS only 
			OPEN_TRANSIENT_DB = 0x00000400,      // VFS only 
			OPEN_MAIN_JOURNAL = 0x00000800,      // VFS only 
			OPEN_TEMP_JOURNAL = 0x00001000,      // VFS only 
			OPEN_SUBJOURNAL = 0x00002000,        // VFS only 
			OPEN_MASTER_JOURNAL = 0x00004000,    // VFS only 
			OPEN_NOMUTEX = 0x00008000,           // Ok for sqlite3_open_v2() 
			OPEN_FULLMUTEX = 0x00010000,         // Ok for sqlite3_open_v2() 
			OPEN_SHAREDCACHE = 0x00020000,       // Ok for sqlite3_open_v2() 
			OPEN_PRIVATECACHE = 0x00040000,      // Ok for sqlite3_open_v2() 
			OPEN_WAL = 0x00080000,               // VFS only 
		};

		enum ACCESS
		{
			ACCESS_EXISTS = 0,
			ACCESS_READWRITE = 1,	// Used by PRAGMA temp_store_directory
			ACCESS_READ = 2,		// Unused
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

		__device__ virtual IO::VFile *_AttachFile(void *buffer) = 0;
		__device__ virtual RC Open(const char *path, IO::VFile *file, OPEN flags, OPEN *outFlags) = 0;
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

	//__device__ VSystem::OPEN inline operator|(VSystem::OPEN a, VSystem::OPEN b) { return (VSystem::OPEN)((unsigned int)a | (unsigned int)b); }
	__device__ VSystem::OPEN inline operator|=(VSystem::OPEN a, int b) { return (VSystem::OPEN)(a | b); }
}