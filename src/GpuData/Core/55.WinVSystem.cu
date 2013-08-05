// ow_win.c
#include "../Core.cu.h"
#if 1 //OS_WIN // This file is used for Windows only
#include <Windows.h>

namespace Core { namespace IO
{
#pragma region Polyfill

#if !OS_WINNT && !defined(OMIT_WAL) // Compiling and using WAL mode requires several APIs that are only available in Windows platforms based on the NT kernel.
#error "WAL mode requires support from the Windows NT kernel, compile with OMIT_WAL."
#endif

	// Are most of the Win32 ANSI APIs available (i.e. with certain exceptions based on the sub-platform)?
#ifdef __CYGWIN__
#include <sys/cygwin.h>
#endif
#if !OS_WINCE && !OS_WINRT 
#define WIN32_HAS_ANSI
#endif
#if OS_WINCE || OS_WINNT || OS_WINRT
#define WIN32_HAS_WIDE
#endif
#if WIN32_FILEMAPPING_API && !defined(OMIT_WAL)
#if OS_WINRT
	WINBASEAPI HANDLE WINAPI CreateFileMappingFromApp(HANDLE, LPSECURITY_ATTRIBUTES, ULONG, ULONG64, LPCWSTR);
	WINBASEAPI LPVOID WINAPI MapViewOfFileFromApp(HANDLE, ULONG, ULONG64, SIZE_T);
#else
#if defined(WIN32_HAS_ANSI)
	WINBASEAPI HANDLE WINAPI CreateFileMappingA(HANDLE, LPSECURITY_ATTRIBUTES, DWORD, DWORD, DWORD, LPCSTR);
#endif
#if defined(WIN32_HAS_WIDE)
	WINBASEAPI HANDLE WINAPI CreateFileMappingW(HANDLE, LPSECURITY_ATTRIBUTES, DWORD, DWORD, DWORD, LPCWSTR);
#endif
	WINBASEAPI LPVOID WINAPI MapViewOfFile(HANDLE, DWORD, DWORD, DWORD, SIZE_T);
#endif
	WINBASEAPI BOOL WINAPI UnmapViewOfFile(LPCVOID);
#endif
#if OS_WINCE // WinCE lacks native support for file locking so we have to fake it with some code of our own.
	typedef struct winceLock
	{
		int Readers;       // Number of reader locks obtained
		bool Pending;      // Indicates a pending lock has been obtained
		bool Reserved;     // Indicates a reserved lock has been obtained
		bool Exclusive;    // Indicates an exclusive lock has been obtained
	} winceLock;
#endif

	// Some Microsoft compilers lack this definition.
#ifndef INVALID_FILE_ATTRIBUTES
#define INVALID_FILE_ATTRIBUTES ((DWORD)-1) 
#endif
#ifndef FILE_FLAG_MASK
#define FILE_FLAG_MASK (0xFF3C0000)
#endif
#ifndef FILE_ATTRIBUTE_MASK
#define FILE_ATTRIBUTE_MASK (0x0003FFF7)
#endif
#ifndef INVALID_SET_FILE_POINTER
#define INVALID_SET_FILE_POINTER ((DWORD)-1)
#endif

	// The following variable is (normally) set once and never changes thereafter.  It records whether the operating system is Win9x or WinNT.
	// 0:   Operating system unknown.
	// 1:   Operating system is Win9x.
	// 2:   Operating system is WinNT.
	// In order to facilitate testing on a WinNT system, the test fixture can manually set this value to 1 to emulate Win98 behavior.
#ifdef TEST
	int os_type = 0;
#else
	static int os_type = 0;
#endif
#if OS_WINCE || OS_WINRT
#define isNT() (true)
#elif !defined(WIN32_HAS_WIDE)
#define isNT() (false)
#else
	static bool isNT()
	{
		if (os_type == 0)
		{
			OSVERSIONINFOA sInfo;
			sInfo.dwOSVersionInfoSize = sizeof(sInfo);
			osGetVersionExA(&sInfo);
			os_type = (sInfo.dwPlatformId == VER_PLATFORM_WIN32_NT ? 2 : 1);
		}
		return (os_type == 2);
	}
#endif

#pragma endregion


	// LOCKFILE_FAIL_IMMEDIATELY is undefined on some Windows systems.
#ifndef LOCKFILE_FAIL_IMMEDIATELY
#define LOCKFILE_FAIL_IMMEDIATELY 1
#endif
#ifndef LOCKFILE_EXCLUSIVE_LOCK
#define LOCKFILE_EXCLUSIVE_LOCK 2
#endif

	/*
	** Historically, SQLite has used both the LockFile and LockFileEx functions.
	** When the LockFile function was used, it was always expected to fail
	** immediately if the lock could not be obtained.  Also, it always expected to
	** obtain an exclusive lock.  These flags are used with the LockFileEx function
	** and reflect those expectations; therefore, they should not be changed.
	*/
#ifndef SQLITE_LOCKFILE_FLAGS
#define SQLITE_LOCKFILE_FLAGS (LOCKFILE_FAIL_IMMEDIATELY | LOCKFILE_EXCLUSIVE_LOCK)
#endif
#ifndef SQLITE_LOCKFILEEX_FLAGS
#define SQLITE_LOCKFILEEX_FLAGS (LOCKFILE_FAIL_IMMEDIATELY)
#endif


#pragma region WinVFile

#ifndef OMIT_WAL // Forward references
	typedef struct winShm winShm;           // A connection to shared-memory
	typedef struct winShmNode winShmNode;   // A region of shared-memory
#endif

	// winFile
	class WinVFile : public VFile
	{
	public:
		enum class WINFILE : uint8
		{
			PERSIST_WA = 0x04,  // Persistent WAL mode
			PSOW = 0x10,		// SQLITE_IOCAP_POWERSAFE_OVERWRITE
		};

		VSystem *Vfs;			// The VFS used to open this file
		HANDLE H;               // Handle for accessing the file
		LOCK Lock;				// Type of lock currently held on this file
		short SharedLockByte;   // Randomly chosen byte used as a shared lock
		WINFILE CtrlFlags;      // Flags.  See WINFILE_* below
		DWORD LastErrno;        // The Windows errno from the last I/O error
#ifndef OMIT_WAL
		winShm *Shm;			// Instance of shared memory on this file
#endif
		const char *Path;		// Full pathname of this file
		int SizeChunk;          // Chunk size configured by FCNTL_CHUNK_SIZE
#if OS_WINCE
		LPWSTR DeleteOnClose;  // Name of file to delete when closing
		HANDLE Mutex;			// Mutex used to control access to shared lock
		HANDLE SharedHandle;	// Shared memory segment used for locking
		winceLock Local;        // Locks obtained by this instance of winFile
		winceLock *Shared;      // Global shared lock memory for the file
#endif
	};

#pragma endregion

#pragma region Win32

#ifndef WIN32_DBG_BUF_SIZE // The size of the buffer used by sqlite3_win32_write_debug().
#define WIN32_DBG_BUF_SIZE ((int)(4096 - sizeof(DWORD)))
#endif
#ifndef WIN32_DATA_DIRECTORY_TYPE // The value used with sqlite3_win32_set_directory() to specify that the data directory should be changed.
#define WIN32_DATA_DIRECTORY_TYPE (1)
#endif
#ifndef WIN32_TEMP_DIRECTORY_TYPE // The value used with sqlite3_win32_set_directory() to specify that the temporary directory should be changed.
#define WIN32_TEMP_DIRECTORY_TYPE (2) 
#endif

#pragma endregion

#pragma region Syscall

	typedef void (*syscall_ptr)(void);
#ifndef SYSCALL
#define SYSCALL syscall_ptr
#endif

	static struct win_syscall {
		const char *Name;            // Name of the system call
		syscall_ptr Current; // Current value of the system call
		syscall_ptr Default; // Default value
	} Syscalls[] = 
	{
#if !OS_WINCE && !OS_WINRT
		{"AreFileApisANSI", (SYSCALL)AreFileApisANSI, nullptr},
#else
		{"AreFileApisANSI", (SYSCALL)nullptr, nullptr},
#endif
#if OS_WINCE || OS_WINRT // This function is not available on Windows CE or WinRT.
#define osAreFileApisANSI() 1
#else
#define osAreFileApisANSI ((BOOL(WINAPI*)(VOID))Syscalls[0].Current)
#endif
#if OS_WINCE && defined(WIN32_HAS_WIDE)
		{"CharLowerW", (SYSCALL)CharLowerW, nullptr},
#else
		{"CharLowerW", (SYSCALL)nullptr, nullptr},
#endif
#define osCharLowerW ((LPWSTR(WINAPI*)(LPWSTR))aSyscall[1].pCurrent)
#if OS_WINCE && defined(WIN32_HAS_WIDE)
		{"CharUpperW", (SYSCALL)CharUpperW, nullptr},
#else
		{"CharUpperW", (SYSCALL)nullptr, nullptr},
#endif
#define osCharUpperW ((LPWSTR(WINAPI *)(LPWSTR))Syscalls[2].Current)
		{"CloseHandle", (SYSCALL)CloseHandle, nullptr},
#define osCloseHandle ((BOOL(WINAPI *)(HANDLE))Syscalls[3].Current)
#if defined(WIN32_HAS_ANSI)
		{"CreateFileA", (SYSCALL)CreateFileA, nullptr},
#else
		{"CreateFileA", (SYSCALL)nullptr, nullptr},
#endif
#define osCreateFileA ((HANDLE(WINAPI *)(LPCSTR,DWORD,DWORD,LPSECURITY_ATTRIBUTES,DWORD,DWORD,HANDLE))Syscalls[4].Current)
#if !OS_WINRT && defined(WIN32_HAS_WIDE)
		{"CreateFileW", (SYSCALL)CreateFileW, nullptr},
#else
		{"CreateFileW", (SYSCALL)nullptr, nullptr},
#endif
#define osCreateFileW ((HANDLE(WINAPI*)(LPCWSTR,DWORD,DWORD,LPSECURITY_ATTRIBUTES,DWORD,DWORD,HANDLE))Syscalls[5].Current)
#if (!OS_WINRT && defined(WIN32_HAS_ANSI) && !defined(OMIT_WAL))
		{"CreateFileMappingA", (SYSCALL)CreateFileMappingA, nullptr},
#else
		{"CreateFileMappingA", (SYSCALL)nullptr, nullptr},
#endif
#define osCreateFileMappingA ((HANDLE(WINAPI *)(HANDLE,LPSECURITY_ATTRIBUTES,DWORD,DWORD,DWORD,LPCSTR))Syscalls[6].Current)

#if OS_WINCE || (!OS_WINRT && defined(WIN32_HAS_WIDE) && !defined(OMIT_WAL))
		{"CreateFileMappingW", (SYSCALL)CreateFileMappingW, nullptr},
#else
		{"CreateFileMappingW", (SYSCALL)nullptr, nullptr},
#endif
#define osCreateFileMappingW ((HANDLE(WINAPI *)(HANDLE,LPSECURITY_ATTRIBUTES,DWORD,DWORD,DWORD,LPCWSTR))Syscalls[7].Current)
#if !OS_WINRT && defined(WIN32_HAS_WIDE)
		{"CreateMutexW", (SYSCALL)CreateMutexW, nullptr},
#else
		{"CreateMutexW", (SYSCALL)nullptr, nullptr},
#endif
#define osCreateMutexW ((HANDLE(WINAPI *)(LPSECURITY_ATTRIBUTES,BOOL,LPCWSTR))Syscalls[8].Current)
#if defined(WIN32_HAS_ANSI)
		{"DeleteFileA", (SYSCALL)DeleteFileA, nullptr},
#else
		{"DeleteFileA", (SYSCALL)nullptr, nullptr},
#endif
#define osDeleteFileA ((BOOL(WINAPI *)(LPCSTR))Syscalls[9].Current)
#if defined(SQLITE_WIN32_HAS_WIDE)
		{"DeleteFileW", (SYSCALL)DeleteFileW, nullptr},
#else
		{"DeleteFileW", (SYSCALL)nullptr, nullptr},
#endif
#define osDeleteFileW ((BOOL(WINAPI *)(LPCWSTR))Syscalls[10].Current)
#if OS_WINCE
		{"FileTimeToLocalFileTime", (SYSCALL)FileTimeToLocalFileTime, nullptr},
#else
		{"FileTimeToLocalFileTime", (SYSCALL)nullptr, nullptr},
#endif
#define osFileTimeToLocalFileTime ((BOOL(WINAPI *)(CONST FILETIME*,LPFILETIME))Syscalls[11].Current)
#if OS_WINCE
		{"FileTimeToSystemTime", (SYSCALL)FileTimeToSystemTime, nullptr},
#else
		{"FileTimeToSystemTime", (SYSCALL)nullptr, nullptr},
#endif
#define osFileTimeToSystemTime ((BOOL(WINAPI *)(CONST FILETIME*,LPSYSTEMTIME))Syscalls[12].Current)
		{"FlushFileBuffers", (SYSCALL)FlushFileBuffers, nullptr},
#define osFlushFileBuffers ((BOOL(WINAPI *)(HANDLE))Syscalls[13].Current)
#if defined(WIN32_HAS_ANSI)
		{"FormatMessageA", (SYSCALL)FormatMessageA, nullptr},
#else
		{"FormatMessageA", (SYSCALL)nullptr, nullptr},
#endif
#define osFormatMessageA ((DWORD(WINAPI *)(DWORD,LPCVOID,DWORD,DWORD,LPSTR,DWORD,va_list*))Syscalls[14].Current)
#if defined(WIN32_HAS_WIDE)
		{"FormatMessageW", (SYSCALL)FormatMessageW, nullptr},
#else
		{"FormatMessageW", (SYSCALL)nullptr, nullptr},
#endif
#define osFormatMessageW ((DWORD(WINAPI *)(DWORD,LPCVOID,DWORD,DWORD,LPWSTR,DWORD,va_list*))Syscalls[15].Current)
#if !defined(OMIT_LOAD_EXTENSION)
		{"FreeLibrary", (SYSCALL)FreeLibrary, nullptr},
#else
		{"FreeLibrary", (SYSCALL)nullptr, nullptr},
#endif
#define osFreeLibrary ((BOOL(WINAPI *)(HMODULE))Syscalls[16].Current)
		{"GetCurrentProcessId", (SYSCALL)GetCurrentProcessId, nullptr},
#define osGetCurrentProcessId ((DWORD(WINAPI *)(VOID))Syscalls[17].Current)
#if !OS_WINCE && defined(WIN32_HAS_ANSI)
		{"GetDiskFreeSpaceA", (SYSCALL)GetDiskFreeSpaceA, nullptr},
#else
		{"GetDiskFreeSpaceA", (SYSCALL)nullptr, nullptr},
#endif
#define osGetDiskFreeSpaceA ((BOOL(WINAPI *)(LPCSTR,LPDWORD,LPDWORD,LPDWORD,LPDWORD))Syscalls[18].Current)
#if !OS_WINCE && !OS_WINRT && defined(WIN32_HAS_WIDE)
		{"GetDiskFreeSpaceW", (SYSCALL)GetDiskFreeSpaceW, nullptr},
#else
		{"GetDiskFreeSpaceW", (SYSCALL)nullptr, nullptr},
#endif
#define osGetDiskFreeSpaceW ((BOOL(WINAPI*)(LPCWSTR,LPDWORD,LPDWORD,LPDWORD,LPDWORD))Syscalls[19].Current)
#if defined(WIN32_HAS_ANSI)
		{"GetFileAttributesA", (SYSCALL)GetFileAttributesA, nullptr},
#else
		{"GetFileAttributesA", (SYSCALL)nullptr, nullptr},
#endif
#define osGetFileAttributesA ((DWORD(WINAPI *)(LPCSTR))Syscalls[20].Current)
#if !OS_WINRT && defined(WIN32_HAS_WIDE)
		{"GetFileAttributesW", (SYSCALL)GetFileAttributesW, nullptr},
#else
		{"GetFileAttributesW", (SYSCALL)nullptr, nullptr},
#endif
#define osGetFileAttributesW ((DWORD(WINAPI *)(LPCWSTR))Syscalls[21].Current)

#if defined(WIN32_HAS_WIDE)
		{"GetFileAttributesExW", (SYSCALL)GetFileAttributesExW, nullptr},
#else
		{"GetFileAttributesExW", (SYSCALL)nullptr, nullptr},
#endif
#define osGetFileAttributesExW ((BOOL(WINAPI*)(LPCWSTR,GET_FILEEX_INFO_LEVELS,LPVOID))Syscalls[22].Current)
#if !OS_WINRT
		{"GetFileSize", (SYSCALL)GetFileSize, nullptr},
#else
		{"GetFileSize", (SYSCALL)nullptr, nullptr},
#endif
#define osGetFileSize ((DWORD(WINAPI *)(HANDLE,LPDWORD))Syscalls[23].Current)
#if !OS_WINCE && defined(WIN32_HAS_ANSI)
		{"GetFullPathNameA", (SYSCALL)GetFullPathNameA, nullptr},
#else
		{"GetFullPathNameA", (SYSCALL)nullptr, nullptr},
#endif
#define osGetFullPathNameA ((DWORD(WINAPI*)(LPCSTR,DWORD,LPSTR,LPSTR*))Syscalls[24].Current)
#if !OS_WINCE && !OS_WINRT && defined(WIN32_HAS_WIDE)
		{"GetFullPathNameW", (SYSCALL)GetFullPathNameW, nullptr},
#else
		{"GetFullPathNameW", (SYSCALL)nullptr, nullptr},
#endif
#define osGetFullPathNameW ((DWORD(WINAPI *)(LPCWSTR,DWORD,LPWSTR,LPWSTR*))Syscalls[25].Current)
		{"GetLastError", (SYSCALL)GetLastError, nullptr},
#define osGetLastError ((DWORD(WINAPI *)(VOID))Syscalls[26].Current)
#if !defined(OMIT_LOAD_EXTENSION)
#if OS_WINCE
		// The GetProcAddressA() routine is only available on Windows CE.
		{"GetProcAddressA", (SYSCALL)GetProcAddressA, nullptr},
#else
		// All other Windows platforms expect GetProcAddress() to take an ANSI string regardless of the _UNICODE setting
		{"GetProcAddressA", (SYSCALL)GetProcAddress, nullptr},
#endif
#else
		{"GetProcAddressA", (SYSCALL)nullptr, nullptr},
#endif
#define osGetProcAddressA ((FARPROC(WINAPI *)(HMODULE,LPCSTR))Syscalls[27].Current)
#if !OS_WINRT
		{"GetSystemInfo", (SYSCALL)GetSystemInfo, nullptr},
#else
		{"GetSystemInfo", (SYSCALL)nullptr, nullptr},
#endif
#define osGetSystemInfo ((VOID(WINAPI *)(LPSYSTEM_INFO))Syscalls[28].Current)
		{"GetSystemTime", (SYSCALL)GetSystemTime, nullptr},
#define osGetSystemTime ((VOID(WINAPIs*)(LPSYSTEMTIME))Syscalls[29].Current)
#if !OS_WINCE
		{"GetSystemTimeAsFileTime", (SYSCALL)GetSystemTimeAsFileTime, nullptr},
#else
		{"GetSystemTimeAsFileTime", (SYSCALL)nullptr, nullptr},
#endif
#define osGetSystemTimeAsFileTime ((VOID(WINAPI *)(LPFILETIME))Syscalls[30].Current)
#if defined(WIN32_HAS_ANSI)
		{"GetTempPathA", (SYSCALL)GetTempPathA, nullptr},
#else
		{"GetTempPathA", (SYSCALL)nullptr, nullptr},
#endif
#define osGetTempPathA ((DWORD(WINAPI*)(DWORD,LPSTR))Syscalls[31].Current)
#if !OS_WINRT && defined(WIN32_HAS_WIDE)
		{"GetTempPathW", (SYSCALL)GetTempPathW, nullptr},
#else
		{"GetTempPathW", (SYSCALL)nullptr, nullptr},
#endif
#define osGetTempPathW ((DWORD(WINAPI *)(DWORD,LPWSTR))Syscalls[32].Current)
#if !OS_WINRT
		{"GetTickCount", (SYSCALL)GetTickCount, nullptr},
#else
		{"GetTickCount", (SYSCALL)nullptr, nullptr},
#endif
#define osGetTickCount ((DWORD(WINAPI *)(VOID))Syscalls[33].Current)
#if defined(SQLITE_WIN32_HAS_ANSI)
		{"GetVersionExA", (SYSCALL)GetVersionExA, nullptr},
#else
		{"GetVersionExA", (SYSCALL)nullptr, nullptr},
#endif
#define osGetVersionExA ((BOOL(WINAPI *)(LPOSVERSIONINFOA))Syscalls[34].Current)
		{"HeapAlloc", (SYSCALL)HeapAlloc, nullptr},
#define osHeapAlloc ((LPVOID(WINAPI *)(HANDLE,DWORD,SIZE_T))Syscalls[35].Current)
#if !OS_WINRT
		{"HeapCreate", (SYSCALL)HeapCreate, nullptr},
#else
		{"HeapCreate", (SYSCALL)nullptr, nullptr},
#endif
#define osHeapCreate ((HANDLE(WINAPI *)(DWORD,SIZE_T,SIZE_T))Syscalls[36].Current)
#if !OS_WINRT
		{"HeapDestroy", (SYSCALL)HeapDestroy, nullptr},
#else
		{"HeapDestroy", (SYSCALL)nullptr, nullptr},
#endif
#define osHeapDestroy ((BOOL(WINAPI *)(HANDLE))Syscalls[37].Current)
		{"HeapFree", (SYSCALL)HeapFree, nullptr},
#define osHeapFree ((BOOL(WINAPI *)(HANDLE,DWORD,LPVOID))Syscalls[38].Current)
		{"HeapReAlloc", (SYSCALL)HeapReAlloc, nullptr},
#define osHeapReAlloc ((LPVOID(WINAPI *)(HANDLE,DWORD,LPVOID,SIZE_T))Syscalls[39].Current)
		{"HeapSize", (SYSCALL)HeapSize, nullptr},

#define osHeapSize ((SIZE_T(WINAPI *)(HANDLE,DWORD,LPCVOID))Syscalls[40].Current)
#if !OS_WINRT
		{"HeapValidate", (SYSCALL)HeapValidate, nullptr},
#else
		{"HeapValidate", (SYSCALL)nullptr, nullptr},
#endif
#define osHeapValidate ((BOOL(WINAPI *)(HANDLE,DWORD,LPCVOID))Syscalls[41].Current)
#if defined(WIN32_HAS_ANSI) && !defined(OMIT_LOAD_EXTENSION)
		{"LoadLibraryA", (SYSCALL)LoadLibraryA, nullptr},
#else
		{"LoadLibraryA", (SYSCALL)nullptr, nullptr},
#endif
#define osLoadLibraryA ((HMODULE(WINAPI *)(LPCSTR))Syscalls[42].Current)
#if !OS_WINRT && defined(WIN32_HAS_WIDE) && !defined(OMIT_LOAD_EXTENSION)
		{"LoadLibraryW", (SYSCALL)LoadLibraryW, nullptr},
#else
		{"LoadLibraryW", (SYSCALL)nullptr, nullptr},
#endif
#define osLoadLibraryW ((HMODULE(WINAPI *)(LPCWSTR))Syscalls[43].Current)
#if !OS_WINRT
		{"LocalFree", (SYSCALL)LocalFree, nullptr},
#else
		{"LocalFree", (SYSCALL)nullptr, nullptr},
#endif
#define osLocalFree ((HLOCAL(WINAPI *)(HLOCAL))Syscalls[44].Current)
#if !OS_WINCE && !OS_WINRT
		{"LockFile", (SYSCALL)LockFile, nullptr},
#else
		{"LockFile", (SYSCALL)nullptr, nullptr},
#endif
#ifndef osLockFile
#define osLockFile ((BOOL(WINAPI *)(HANDLE,DWORD,DWORD,DWORD,DWORD))Syscalls[45].Current)
#endif
#if !OS_WINCE
		{"LockFileEx", (SYSCALL)LockFileEx, nullptr},
#else
		{"LockFileEx", (SYSCALL)nullptr, nullptr},
#endif
#ifndef osLockFileEx
#define osLockFileEx ((BOOL(WINAPI *)(HANDLE,DWORD,DWORD,DWORD,DWORD,LPOVERLAPPED))Syscalls[46].Current)
#endif
#if OS_WINCE || (!OS_WINRT && !defined(OMIT_WAL))
		{"MapViewOfFile", (SYSCALL)MapViewOfFile,nullptr},
#else
		{"MapViewOfFile", (SYSCALL)nullptr,nullptr},
#endif
#define osMapViewOfFile ((LPVOID(WINAPI *)(HANDLE,DWORD,DWORD,DWORD,SIZE_T))Syscalls[47].Current)
		{"MultiByteToWideChar", (SYSCALL)MultiByteToWideChar, nullptr},
#define osMultiByteToWideChar ((int(WINAPI *)(UINT,DWORD,LPCSTR,int,LPWSTR,int))Syscalls[48].Current)
		{"QueryPerformanceCounter", (SYSCALL)QueryPerformanceCounter, nullptr},
#define osQueryPerformanceCounter ((BOOL(WINAPI *)(LARGE_INTEGER*))Syscalls[49].Current)
		{"ReadFile", (SYSCALL)ReadFile, nullptr},
#define osReadFile ((BOOL(WINAPI *)(HANDLE,LPVOID,DWORD,LPDWORD,LPOVERLAPPED))Syscalls[50].Current)
		{"SetEndOfFile", (SYSCALL)SetEndOfFile, nullptr},
#define osSetEndOfFile ((BOOL(WINAPI *)(HANDLE))Syscalls[51].Current)
#if !OS_WINRT
		{"SetFilePointer", (SYSCALL)SetFilePointer, nullptr},
#else
		{"SetFilePointer", (SYSCALL)nullptr, nullptr},
#endif
#define osSetFilePointer ((DWORD(WINAPI *)(HANDLE,LONG,PLONG,DWORD))Syscalls[52].Current)
#if !OS_WINRT
		{"Sleep", (SYSCALL)Sleep, nullptr},
#else
		{"Sleep", (SYSCALL)nullptr, nullptr},
#endif
#define osSleep ((VOID(WINAPI *)(DWORD))Syscalls[53].Current)
		{"SystemTimeToFileTime", (SYSCALL)SystemTimeToFileTime, nullptr},
#define osSystemTimeToFileTime ((BOOL(WINAPI *)(CONST SYSTEMTIME*,LPFILETIME))Syscalls[54].Current)
#if !OS_WINCE && !OS_WINRT
		{"UnlockFile", (SYSCALL)UnlockFile, nullptr},
#else
		{"UnlockFile", (SYSCALL)nullptr, nullptr},
#endif
#ifndef osUnlockFile
#define osUnlockFile ((BOOL(WINAPI *)(HANDLE,DWORD,DWORD,DWORD,DWORD))Syscalls[55].Current)
#endif
#if !OS_WINCE
		{"UnlockFileEx", (SYSCALL)UnlockFileEx, nullptr},
#else
		{"UnlockFileEx", (SYSCALL)nullptr, nullptr},
#endif
#define osUnlockFileEx ((BOOL(WINAPI *)(HANDLE,DWORD,DWORD,DWORD,LPOVERLAPPED))Syscalls[56].Current)
#if OS_WINCE || !defined(OMIT_WAL)
		{"UnmapViewOfFile", (SYSCALL)UnmapViewOfFile, nullptr},
#else
		{"UnmapViewOfFile", (SYSCALL)nullptr, nullptr},
#endif
#define osUnmapViewOfFile ((BOOL(WINAPI *)(LPCVOID))Syscalls[57].Current)
		{"WideCharToMultiByte", (SYSCALL)WideCharToMultiByte, nullptr},
#define osWideCharToMultiByte ((int(WINAPI *)(UINT,DWORD,LPCWSTR,int,LPSTR,int,LPCSTR,LPBOOL))Syscalls[58].Current)
		{"WriteFile", (SYSCALL)WriteFile, nullptr},
#define osWriteFile ((BOOL(WINAPI *)(HANDLE,LPCVOID,DWORD,LPDWORD,LPOVERLAPPED))Syscalls[59].Current)
#if OS_WINRT
		{"CreateEventExW", (SYSCALL)CreateEventExW, nullptr},
#else
		{"CreateEventExW", (SYSCALL)nullptr, nullptr},
#endif
#define osCreateEventExW ((HANDLE(WINAPI *)(LPSECURITY_ATTRIBUTES,LPCWSTR,DWORD,DWORD))Syscalls[60].Current)
#if !OS_WINRT
		{"WaitForSingleObject", (SYSCALL)WaitForSingleObject, nullptr},
#else
		{"WaitForSingleObject", (SYSCALL)nullptr, nullptr},
#endif
#define osWaitForSingleObject ((DWORD(WINAPI *)(HANDLE,DWORD))Syscalls[61].Current)
#if OS_WINRT
		{"WaitForSingleObjectEx", (SYSCALL)WaitForSingleObjectEx, nullptr},
#else
		{"WaitForSingleObjectEx", (SYSCALL)nullptr, nullptr},
#endif
#define osWaitForSingleObjectEx ((DWORD(WINAPI *)(HANDLE,DWORD,BOOL))Syscalls[62].Current)
#if OS_WINRT
		{"SetFilePointerEx", (SYSCALL)SetFilePointerEx, nullptr},
#else
		{"SetFilePointerEx", (SYSCALL)nullptr, nullptr},
#endif
#define osSetFilePointerEx ((BOOL(WINAPI *)(HANDLE,LARGE_INTEGER,PLARGE_INTEGER,DWORD))Syscalls[63].Current)
#if OS_WINRT
		{"GetFileInformationByHandleEx", (SYSCALL)GetFileInformationByHandleEx, nullptr},
#else
		{"GetFileInformationByHandleEx", (SYSCALL)nullptr, nullptr},
#endif
#define osGetFileInformationByHandleEx ((BOOL(WINAPI *)(HANDLE,FILE_INFO_BY_HANDLE_CLASS,LPVOID,DWORD))Syscalls[64].Current)
#if OS_WINRT && !defined(OMIT_WAL)
		{"MapViewOfFileFromApp", (SYSCALL)MapViewOfFileFromApp, nullptr},
#else
		{"MapViewOfFileFromApp", (SYSCALL)nullptr, nullptr},
#endif
#define osMapViewOfFileFromApp ((LPVOID(WINAPI *)(HANDLE,ULONG,ULONG64,SIZE_T))Syscalls[65].Current)
#if OS_WINRT
		{"CreateFile2", (SYSCALL)CreateFile2, nullptr},
#else
		{"CreateFile2", (SYSCALL)nullptr, nullptr},
#endif
#define osCreateFile2 ((HANDLE(WINAPI *)(LPCWSTR,DWORD,DWORD,DWORD,LPCREATEFILE2_EXTENDED_PARAMETERS))Syscalls[66].Current)
#if OS_WINRT && !defined(OMIT_LOAD_EXTENSION)
		{"LoadPackagedLibrary", (SYSCALL)LoadPackagedLibrary, nullptr},
#else
		{"LoadPackagedLibrary", (SYSCALL)nullptr, nullptr},
#endif
#define osLoadPackagedLibrary ((HMODULE(WINAPI *)(LPCWSTR,DWORD))Syscalls[67].Current)
#if OS_WINRT
		{"GetTickCount64", (SYSCALL)GetTickCount64, nullptr},
#else
		{"GetTickCount64", (SYSCALL)nullptr, nullptr},
#endif
#define osGetTickCount64 ((ULONGLONG(WINAPI *)(VOID))Syscalls[68].Current)
#if OS_WINRT
		{"GetNativeSystemInfo", (SYSCALL)GetNativeSystemInfo, nullptr},
#else
		{"GetNativeSystemInfo", (SYSCALL)nullptr, nullptr},
#endif
#define osGetNativeSystemInfo ((VOID(WINAPI *)(LPSYSTEM_INFO))Syscalls[69].Current)
#if defined(WIN32_HAS_ANSI)
		{"OutputDebugStringA", (SYSCALL)OutputDebugStringA, nullptr},
#else
		{"OutputDebugStringA", (SYSCALL)nullptr, nullptr},
#endif
#define osOutputDebugStringA ((VOID(WINAPI *)(LPCSTR))Syscalls[70].Current)
#if defined(WIN32_HAS_WIDE)
		{"OutputDebugStringW", (SYSCALL)OutputDebugStringW, nullptr},
#else
		{"OutputDebugStringW", (SYSCALL)nullptr, nullptr},
#endif
#define osOutputDebugStringW ((VOID(WINAPI *)(LPCWSTR))Syscalls[71].Current)
		{"GetProcessHeap", (SYSCALL)GetProcessHeap, nullptr},
#define osGetProcessHeap ((HANDLE(WINAPI *)(VOID))Syscalls[72].Current)
#if OS_WINRT && !defined(OMIT_WAL)
		{"CreateFileMappingFromApp", (SYSCALL)CreateFileMappingFromApp, nullptr},
#else
		{"CreateFileMappingFromApp", (SYSCALL)nullptr, nullptr},
#endif
#define osCreateFileMappingFromApp ((HANDLE(WINAPI *)(HANDLE,LPSECURITY_ATTRIBUTES,ULONG,ULONG64,LPCWSTR))Syscalls[73].Current)
	}; // End of the overrideable system calls

	static int winSetSystemCall(VSystem *notUsed, const char *name, syscall_ptr newFunc)
	{
		RC rc = RC::NOTFOUND;
		if (name == nullptr)
		{
			/// If no zName is given, restore all system calls to their default settings and return NULL
			rc = RC::OK;
			for (int i = 0; i < __arrayStaticLength(Syscalls); i++)
				if (Syscalls[i].Default)
					Syscalls[i].Current = Syscalls[i].Default;
			return rc;
		}
		// If zName is specified, operate on only the one system call specified.
		for (int i = 0; i < __arrayStaticLength(Syscalls); i++)
		{
			if (_!strcmp(name, Syscalls[i].Name))
			{
				if (!Syscalls[i].Default)
					Syscalls[i].Default = Syscalls[i].Current;
				rc = RC::OK;
				if (!newFunc) newFunc = Syscalls[i].Default;
				Syscalls[i].Current = newFunc;
				break;
			}
		}
		return rc;
	}

	static syscall_ptr winGetSystemCall(VSystem *notUsed, const char *name)
	{
		for (int i = 0; i < __arrayStaticLength(Syscalls); i++)
			if (!_strcmp(name, Syscalls[i].Name)) return Syscalls[i].Current;
		return nullptr;
	}

	static const char *winNextSystemCall(VSystem *notUsed, const char *name)
	{
		int i = -1;
		if (name)
			for (i = 0; i < __arrayStaticLength(Syscalls)-1; i++)
				if (!_strcmp(name, Syscalls[i].Name)) break;
		for (i++; i < __arrayStaticLength(Syscalls); i++)
			if (Syscalls[i].Current) return Syscalls[i].Name;
		return 0;
	}

#pragma endregion

#pragma region Win32

	void win32_WriteDebug(const char *buf, int bufLength)
	{
		char dbgBuf[WIN32_DBG_BUF_SIZE];
		int min = MIN(bufLength, (WIN32_DBG_BUF_SIZE - 1)); // may be negative.
		if (min < -1) min = -1; // all negative values become -1.
		_assert(min == -1 || min == 0 || min < WIN32_DBG_BUF_SIZE);
#if defined(WIN32_HAS_ANSI)
		if (min > 0)
		{
			memset(dbgBuf, 0, WIN32_DBG_BUF_SIZE);
			memcpy(dbgBuf, buf, min);
			osOutputDebugStringA(dbgBuf);
		}
		else
			osOutputDebugStringA(buf);
#elif defined(WIN32_HAS_WIDE)
		memset(dbgBuf, 0, WIN32_DBG_BUF_SIZE);
		if (osMultiByteToWideChar(osAreFileApisANSI() ? CP_ACP : CP_OEMCP, 0, buf, min, (LPWSTR)dbgBuf, WIN32_DBG_BUF_SIZE/sizeof(WCHAR)) <= 0)
			return;
		osOutputDebugStringW((LPCWSTR)dbgBuf);
#else
		if (min > 0)
		{
			memset(dbgBuf, 0, WIN32_DBG_BUF_SIZE);
			memcpy(dbgBuf, buf, min);
			fprintf(stderr, "%s", dbgBuf);
		}
		else
			fprintf(stderr, "%s", buf);
#endif
	}

#if OS_WINRT
	static HANDLE sleepObj = NULL;
#endif
	void win32_Sleep(DWORD milliseconds)
	{
#if OS_WINRT
		if (sleepObj == NULL)
			sleepObj = osCreateEventExW(NULL, NULL, CREATE_EVENT_MANUAL_RESET, SYNCHRONIZE);
		_assert(sleepObj != NULL);
		osWaitForSingleObjectEx(sleepObj, milliseconds, FALSE);
#else
		osSleep(milliseconds);
#endif
	}

#pragma endregion

#pragma region WIN32_MALLOC
#ifdef WIN32_MALLOC

	// If compiled with WIN32_MALLOC on Windows, we will use the various Win32 API heap functions instead of our own.

	// If this is non-zero, an isolated heap will be created by the native Win32 allocator subsystem; otherwise, the default process heap will be used.  This
	// setting has no effect when compiling for WinRT.  By default, this is enabled and an isolated heap will be created to store all allocated data.
	//
	//*****************************************************************************
	// WARNING: It is important to note that when this setting is non-zero and the winMemShutdown function is called (e.g. by the sqlite3_shutdown
	//          function), all data that was allocated using the isolated heap will be freed immediately and any attempt to access any of that freed
	//          data will almost certainly result in an immediate access violation.
	//*****************************************************************************
#ifndef WIN32_HEAP_CREATE
#define WIN32_HEAP_CREATE (TRUE)
#endif
#ifndef WIN32_HEAP_INIT_SIZE // The initial size of the Win32-specific heap.  This value may be zero.
#define WIN32_HEAP_INIT_SIZE ((DEFAULT_CACHE_SIZE) * (DEFAULT_PAGE_SIZE) + 4194304)
#endif
#ifndef WIN32_HEAP_MAX_SIZE // The maximum size of the Win32-specific heap.  This value may be zero.
#define WIN32_HEAP_MAX_SIZE (0)
#endif
#ifndef WIN32_HEAP_FLAGS // The extra flags to use in calls to the Win32 heap APIs. This value may be zero for the default behavior.
#define WIN32_HEAP_FLAGS (0)
#endif

	// The winMemData structure stores information required by the Win32-specific sqlite3_mem_methods implementation.
	typedef struct WinMemData
	{
#ifdef _DEBUG
		uint32 Magic;    // Magic number to detect structure corruption.
#endif
		HANDLE Heap; // The handle to our heap.
		BOOL Owned;  // Do we own the heap (i.e. destroy it on shutdown)?
	} WinMemData;

#ifdef _DEBUG
#define WINMEM_MAGIC 0x42b2830b
#endif

	static struct WinMemData winMemData_ = {
#ifdef _DEBUG
		WINMEM_MAGIC,
#endif
		NULL, FALSE
	};

#ifdef _DEBUG
#define winMemAssertMagic() _assert(winMemData_.Magic == WINMEM_MAGIC)
#else
#define winMemAssertMagic()
#endif
#define winMemGetHeap() winMemData_.Heap

	void *WinMem::Malloc(int bytes)
	{
		winMemAssertMagic();
		HANDLE heap = winMemGetHeap();
		_assert(heap != 0);
		_assert(heap != INVALID_HANDLE_VALUE);
#if !OS_WINRT && defined(WIN32_MALLOC_VALIDATE)
		_assert(osHeapValidate(heap, WIN32_HEAP_FLAGS, NULL));
#endif
		_assert(bytes >=0);
		void *p = osHeapAlloc(heap, WIN32_HEAP_FLAGS, (SIZE_T)bytes);
		if (!p)
			SysEx_LOG(RC::NOMEM, "failed to HeapAlloc %u bytes (%d), heap=%p", bytes, osGetLastError(), (void*)heap);
		return p;
	}

	void WinMem::Free(void *prior)
	{
		winMemAssertMagic();
		HANDLE heap = winMemGetHeap();
		_assert(heap != 0);
		_assert(heap != INVALID_HANDLE_VALUE);
#if !OS_WINRT && defined(WIN32_MALLOC_VALIDATE)
		_assert(osHeapValidate(heap, WIN32_HEAP_FLAGS, prior));
#endif
		if (!prior) return; // Passing NULL to HeapFree is undefined.
		if (!osHeapFree(heap, WIN32_HEAP_FLAGS, prior))
			SysEx_LOG(RC::NOMEM, "failed to HeapFree block %p (%d), heap=%p", prior, osGetLastError(), (void*)heap);
	}

	void *WinMem::Realloc(void *prior, int bytes)
	{
		winMemAssertMagic();
		HANDLE heap = winMemGetHeap();
		_assert(hHeap != 0 );
		_assert(hHeap != INVALID_HANDLE_VALUE);
#if !OS_WINRT && defined(SQLITE_WIN32_MALLOC_VALIDATE)
		_assert(osHeapValidate(heap, WIN32_HEAP_FLAGS, prior));
#endif
		_assert(bytes >= 0);
		void *p;
		if (!prior)
			p = osHeapAlloc(heap, WIN32_HEAP_FLAGS, (SIZE_T)bytes);
		else
			p = osHeapReAlloc(heap, WIN32_HEAP_FLAGS, prior, (SIZE_T)bytes);
		if (!p)
			SysEx_LOG(RC::NOMEM, "failed to %s %u bytes (%d), heap=%p", (prior ? "HeapReAlloc" : "HeapAlloc"), bytes, osGetLastError(), (void*)heap);
		return p;
	}

	int WinMem::Size(void *p)
	{
		winMemAssertMagic();
		HANDLE heap = winMemGetHeap();
		vassert(heap != 0);
		_assert(heap != INVALID_HANDLE_VALUE);
#if !OS_WINRT && defined(SQLITE_WIN32_MALLOC_VALIDATE)
		_assert(osHeapValidate(heap, WIN32_HEAP_FLAGS, NULL));
#endif
		if (!p) return 0;
		SIZE_T n = osHeapSize(heap, WIN32_HEAP_FLAGS, p);
		if (n == (SIZE_T)-1)
		{
			SysEx_LOG(RC::NOMEM, "failed to HeapSize block %p (%d), heap=%p", p, osGetLastError(), (void*)heap);
			return 0;
		}
		return (int)n;
	}

	int WinMem::Roundup(int bytes)
	{
		return bytes;
	}

	RC WinMem::Init(void *appData)
	{
		WinMemData *winMemData = (winMemData *)appData;
		if (!winMemData) return RC::ERROR;
		_assert(winMemData->Magic == WINMEM_MAGIC);
#if !OS_WINRT && WIN32_HEAP_CREATE
		if (!winMemData->Heap)
		{
			winMemData->Heap = osHeapCreate(WIN32_HEAP_FLAGS, WIN32_HEAP_INIT_SIZE, WIN32_HEAP_MAX_SIZE);
			if (!winMemData->Heap)
			{
				SysEx_LOG(RC::NOMEM, "failed to HeapCreate (%d), flags=%u, initSize=%u, maxSize=%u", osGetLastError(), WIN32_HEAP_FLAGS, WIN32_HEAP_INIT_SIZE, WIN32_HEAP_MAX_SIZE);
				return RC::NOMEM;
			}
			winMemData->Owned = TRUE;
			_assert(winMemData->Owned);
		}
#else
		winMemData->Heap = osGetProcessHeap();
		if (!winMemData->Heap)
		{
			SysEx_LOG(RC::NOMEM, "failed to GetProcessHeap (%d)", osGetLastError());
			return RC::NOMEM;
		}
		winMemData->Owned = FALSE;
		_assert(!winMemData->Owned);
#endif
		_assert(winMemData->Heap != 0);
		_assert(winMemData->Heap != INVALID_HANDLE_VALUE);
#if !OS_WINRT && defined(WIN32_MALLOC_VALIDATE)
		_assert(osHeapValidate(winMemData->Heap, WIN32_HEAP_FLAGS, NULL));
#endif
		return RC::OK;
	}

	void WinMem::Shutdown(void *appData)
	{
		WinMemData *winMemData = (winMemData *)appData;
		if (!winMemData) return;
		if (winMemData->Heap)
		{
			_assert(winMemData->Heap != INVALID_HANDLE_VALUE);
#if !OS_WINRT && defined(WIN32_MALLOC_VALIDATE)
			_assert(osHeapValidate(winMemData->Heap, WIN32_HEAP_FLAGS, NULL));
#endif
			if (winMemData->Owned)
			{
				if (!osHeapDestroy(winMemData->Heap))
					SysEx_LOG(RC::NOMEM, "failed to HeapDestroy (%d), heap=%p", osGetLastError(), (void*)winMemData->Heap);
				winMemData->Owned = FALSE;
			}
			winMemData->Heap = NULL;
		}
	}

#endif
#pragma endregion

#pragma region String Converters

	static LPWSTR Utf8ToUnicode(const char *name)
	{
		int c = osMultiByteToWideChar(CP_UTF8, 0, name, -1, NULL, 0);
		if (!c)
			return nullptr;
		LPWSTR wideName = SysEx::Alloc(c*sizeof(wideName[0]), true);
		if (!wideName)
			return nullptr;
		c = osMultiByteToWideChar(CP_UTF8, 0, name, -1, wideName, c);
		if (!c)
		{
			SysEx::Free(wideName);
			wideName = nullptr;
		}
		return wideName;
	}

	static char *UnicodeToUtf8(LPCWSTR wideName)
	{
		int c = osWideCharToMultiByte(CP_UTF8, 0, wideName, -1, 0, 0, 0, 0);
		if (!c)
			return nullptr;
		char *name = SysEx::Alloc(c, true);
		if (!name)
			return nullptr;
		c = osWideCharToMultiByte(CP_UTF8, 0, wideName, -1, filename, c, 0, 0);
		if (!c)
		{
			SysEx::Free(name);
			name = nullptr;
		}
		return name;
	}

	static LPWSTR MbcsToUnicode(const char *name)
	{
		int codepage = (osAreFileApisANSI() ? CP_ACP : CP_OEMCP);
		int c = osMultiByteToWideChar(codepage, 0, name, -1, NULL, 0)*sizeof(WCHAR);
		if (!c)
			return nullptr;
		LPWSTR mbcsName = SysEx::Alloc(c*sizeof(mbcsName[0]), true);
		if (!mbcsName)
			return nullptr;
		c = osMultiByteToWideChar(codepage, 0, name, -1, mbcsName, c);
		if (!c)
		{
			SysEx::Free(mbcsName);
			mbcsName = nullptr;
		}
		return mbcsName;
	}

	static char *UnicodeToMbcs(LPCWSTR wideName)
	{
		int codepage = (osAreFileApisANSI() ? CP_ACP : CP_OEMCP);
		int c = osWideCharToMultiByte(codepage, 0, wideName, -1, 0, 0, 0, 0);
		if (!c)
			return nullptr;
		char *name = SysEx::Alloc(c, true);
		if (!ame)
			return nullptr;
		c = osWideCharToMultiByte(codepage, 0, wideName, -1, filename, c, 0, 0);
		if (!c)
		{
			SysEx::Free(name);
			name = nullptr;
		}
		return name;
	}

	char *win32_MbcsToUtf8(const char *name)
	{
		LPWSTR tmpWide = MbcsToUnicode(name);
		if (!tmpWide)
			return nullptr;
		char *nameUtf8 = UnicodeToUtf8(tmpWide);
		SysEx::Free(tmpWide);
		return nameUtf8;
	}

	char *win32_Utf8ToMbcs(const char *name)
	{
		LPWSTR tmpWide = Utf8ToUnicode(name);
		if (!tmpWide)
			return nullptr;
		char *nameMbcs = UnicodeToMbcs(tmpWide);
		SysEx::Free(tmpWide);
		return nameMbcs;
	}

#pragma endregion

#pragma region Win32

	RC win32_SetDirectory(DWORD type, LPCWSTR value)
	{
#ifndef OMIT_AUTOINIT
		RC rc = sqlite3_initialize();
		if (rc) return rc;
#endif
		char **directory = nullptr;
		if (type == WIN32_DATA_DIRECTORY_TYPE)
			directory = &data_directory;
		else if (type == WIN32_TEMP_DIRECTORY_TYPE)
			directory = &temp_directory;
		_assert(!directory || type == WIN32_DATA_DIRECTORY_TYPE || type == WIN32_TEMP_DIRECTORY_TYPE);
		_assert(!directory || SysEx::MemdebugHasType(*directory, SysEx::MEMTYPE::HEAP));
		if (directory)
		{
			char *valueUtf8 = nullptr;
			if (value && value[0])
			{
				valueUtf8 = UnicodeToUtf8(value);
				if (!valueUtf8)
					return RC::NOMEM;
			}
			SysEx::Free(*directory);
			*directory = valueUtf8;
			return RC::OK;
		}
		return RC::ERROR;
	}

#pragma endregion

#pragma region OS Errors

	static RC getLastErrorMsg(DWORD lastErrno, int bufLength, char *buf)
	{
		// FormatMessage returns 0 on failure.  Otherwise it returns the number of TCHARs written to the output
		// buffer, excluding the terminating null char.
		DWORD dwLen = 0;
		char *out = nullptr;
		if (isNT())
		{
#if OS_WINRT
			WCHAR tempWide[MAX_PATH + 1]; // NOTE: Somewhat arbitrary.
			dwLen = osFormatMessageW(FORMAT_MESSAGE_FROM_SYSTEM | FORMAT_MESSAGE_IGNORE_INSERTS, NULL, lastErrno, 0, tempWide, MAX_PATH, 0);
#else
			LPWSTR tempWide = NULL;
			dwLen = osFormatMessageW(FORMAT_MESSAGE_ALLOCATE_BUFFER | FORMAT_MESSAGE_FROM_SYSTEM | FORMAT_MESSAGE_IGNORE_INSERTS, NULL, lastErrno, 0, (LPWSTR)&tempWide, 0, 0);
#endif
			if (dwLen > 0)
			{
				// allocate a buffer and convert to UTF8
				SysEx::BeginBenignAlloc();
				out = UnicodeToUtf8(tempWide);
				SysEx::EndBenignAlloc();
#if !OS_WINRT
				// free the system buffer allocated by FormatMessage
				osLocalFree(tempWide);
#endif
			}
		}
#ifdef WIN32_HAS_ANSI
		else
		{
			char *temp = NULL;
			dwLen = osFormatMessageA(FORMAT_MESSAGE_ALLOCATE_BUFFER | FORMAT_MESSAGE_FROM_SYSTEM | FORMAT_MESSAGE_IGNORE_INSERTS, NULL, lastErrno, 0, (LPSTR)&temp, 0, 0);
			if (dwLen > 0)
			{
				// allocate a buffer and convert to UTF8
				SysEx::BeginBenignAlloc();
				out = win32MbcsToUtf8(temp);
				SysEx::EndBenignAlloc();
				// free the system buffer allocated by FormatMessage
				osLocalFree(temp);
			}
		}
#endif
		if (!dwLen)
			_snprintf(bufLength, buf, "OsError 0x%x (%u)", lastErrno, lastErrno);
		else
		{
			// copy a maximum of nBuf chars to output buffer
			_snprintf(bufLength, buf, "%s", out);
			// free the UTF8 buffer
			SysEx::Free(out);
		}
		return RC::OK;
	}

#define winLogError(a,b,c,d) winLogErrorAtLine(a,b,c,d,__LINE__)
	static RC winLogErrorAtLine(RC errcode, DWORD lastErrno, const char *func, const char *path, int line)
	{
		char msg[500]; // Human readable error text
		msg[0] = 0;
		getLastErrorMsg(lastErrno, sizeof(msg), msg);
		_assert(errcode != RC::OK);
		if (!path) path = "";
		int i;
		for (i = 0; msg[i] && mMsg[i] != '\r' && msg[i] != '\n'; i++) { }
		msg[i] = 0;
		SysEx_LOG(errcode, "os_win.c:%d: (%d) %s(%s) - %s", line, lastErrno, func, path, msg);
		return errcode;
	}

#ifndef WIN32_IOERR_RETRY
#define WIN32_IOERR_RETRY 10
#endif
#ifndef WIN32_IOERR_RETRY_DELAY
#define WIN32_IOERR_RETRY_DELAY 25
#endif
	static int retryIoerr(int *retry, DWORD *error)
	{
		static int win32IoerrRetry = WIN32_IOERR_RETRY;
		static int win32IoerrRetryDelay = WIN32_IOERR_RETRY_DELAY;

		DWORD e = osGetLastError();
		if (*retry >= win32IoerrRetry)
		{
			if (error)
				*error = e;
			return 0;
		}
		if (e == ERROR_ACCESS_DENIED || e == ERROR_LOCK_VIOLATION || e == ERROR_SHARING_VIOLATION)
		{
			win32_Sleep(win32IoerrRetryDelay*(1+*retry));
			++*retry;
			return 1;
		}
		if (error)
			*error = e;
		return 0;
	}

	static void logIoerr(int retry)
	{
		if (retry)
			SysEx_LOG(RC::IOERR, "delayed %dms for lock/sharing conflict", win32IoerrRetryDelay*nRetry*(retry+1)/2);
	}

#pragma endregion

#pragma region WinCE Only
#if OS_WINCE

#define HANDLE_TO_WINFILE(a) (WinVFile*)&((char*)a)[-(int)offsetof(WinVFile,h)]

#if !defined(MSVC_LOCALTIME_API) || !MSVC_LOCALTIME_API
	// The MSVC CRT on Windows CE may not have a localtime() function.  So create a substitute.
#include <time.h>
	struct tm *__cdecl localtime(const time_t *t)
	{
		static struct tm y;
		FILETIME uTm, lTm;
		SYSTEMTIME pTm;
		sqlite3_int64 t64;
		t64 = *t;
		t64 = (t64 + 11644473600)*10000000;
		uTm.dwLowDateTime = (DWORD)(t64 & 0xFFFFFFFF);
		uTm.dwHighDateTime= (DWORD)(t64 >> 32);
		osFileTimeToLocalFileTime(&uTm,&lTm);
		osFileTimeToSystemTime(&lTm,&pTm);
		y.tm_year = pTm.wYear - 1900;
		y.tm_mon = pTm.wMonth - 1;
		y.tm_wday = pTm.wDayOfWeek;
		y.tm_mday = pTm.wDay;
		y.tm_hour = pTm.wHour;
		y.tm_min = pTm.wMinute;
		y.tm_sec = pTm.wSecond;
		return &y;
	}
#endif

	static void winceMutexAcquire(HANDLE h)
	{
		DWORD err;
		do
		{
			err = osWaitForSingleObject(h, INFINITE);
		} while (err != WAIT_OBJECT_0 && err != WAIT_ABANDONED);
	}

#define winceMutexRelease(h) ReleaseMutex(h)

	static RC winceCreateLock(const char *filename, WinVFile *file)
	{
		LPWSTR name = Utf8ToUnicode(filename);
		if (!name)
			return RC::IOERR_NOMEM;
		// Initialize the local lockdata
		memset(&file->Local, 0, sizeof(file->Local));
		// Replace the backslashes from the filename and lowercase it to derive a mutex name.
		LPWSTR tok = osCharLowerW(name);
		for (; *tok; tok++)
			if (*tok == '\\') *tok = '_';
		// Create/open the named mutex
		file->Mutex = osCreateMutexW(NULL, FALSE, name);
		if (!file->Mutex)
		{
			file->LastErrno = osGetLastError();
			winLogError(RC::IOERR, file->LastErrno, "winceCreateLock1", filename);
			SysEx::Free(name);
			return RC::IOERR;
		}
		// Acquire the mutex before continuing
		winceMutexAcquire(file->Mutex);
		// Since the names of named mutexes, semaphores, file mappings etc are case-sensitive, take advantage of that by uppercasing the mutex name
		// and using that as the shared filemapping name.
		osCharUpperW(name);
		file->SharedHandle = osCreateFileMappingW(INVALID_HANDLE_VALUE, NULL, PAGE_READWRITE, 0, sizeof(winceLock), name);  
		// Set a flag that indicates we're the first to create the memory so it must be zero-initialized
		bool init = true;
		DWORD lastErrno = osGetLastError();
		if (lastErrno == ERROR_ALREADY_EXISTS)
			init = false;
		SysEx::Free(name);

		// If we succeeded in making the shared memory handle, map it.
		bool logged = false;
		if (file->SharedHandle)
		{
			file->Shared = (winceLock *)osMapViewOfFile(file->SharedHandle, FILE_MAP_READ | FILE_MAP_WRITE, 0, 0, sizeof(winceLock));
			// If mapping failed, close the shared memory handle and erase it
			if (!file->Shared)
			{
				file->LastErrno = osGetLastError();
				winLogError(RC::IOERR, file->LastErrno, "winceCreateLock2", filename);
				logged = true;
				osCloseHandle(file->SharedHandle);
				file->SharedHandle = NULL;
			}
		}
		// If shared memory could not be created, then close the mutex and fail
		if (!file->SharedHandle)
		{
			if (!logged)
			{
				file->LastErrno = lastErrno;
				winLogError(RC::IOERR, file->LastErrno, "winceCreateLock3", filename);
				logged = true;
			}
			winceMutexRelease(file->Mutex);
			osCloseHandle(file->Mutex);
			file->Mutex = NULL;
			return RC::IOERR;
		}
		// Initialize the shared memory if we're supposed to
		if (init)
			memset(file->Shared, 0, sizeof(winceLock));
		winceMutexRelease(file->Mutex);
		return RC::OK;
	}

	static void winceDestroyLock(WinVFile *file)
	{
		if (file->Mutex)
		{
			// Acquire the mutex
			winceMutexAcquire(file->Mutex);
			// The following blocks should probably assert in debug mode, but they are to cleanup in case any locks remained open
			if (file->Local.Readers)
				file->Shared->Readers--;
			if (file->Local.Reserved)
				file->Shared->Reserved = FALSE;
			if (file->Local.Pending)
				file->Shared->Pending = FALSE;
			if (file->Local.Exclusive)
				file->Shared->Exclusive = FALSE;
			// De-reference and close our copy of the shared memory handle
			osUnmapViewOfFile(file->Shared);
			osCloseHandle(file->SharedHandle);
			// Done with the mutex
			winceMutexRelease(file->Mutex);    
			osCloseHandle(file->Mutex);
			file->Mutex = NULL;
		}
	}

	static BOOL winceLockFile(LPHANDLE fileHandle, DWORD fileOffsetLow, DWORD fileOffsetHigh, DWORD numberOfBytesToLockLow, DWORD numberOfBytesToLockHigh)
	{
		WinVFile *file = HANDLE_TO_WINFILE(fileHandle);
		BOOL r = FALSE;
		if (!file->Mutex) return true;
		winceMutexAcquire(file->Mutex);
		// Wanting an exclusive lock?
		if (fileOffsetLow == (DWORD)SHARED_FIRST && numberOfBytesToLockLow == (DWORD)SHARED_SIZE)
		{
			if (file->Shared->Readers == 0 && !file->Shared->Exclusive)
			{
				file->Shared->Exclusive = true;
				file->Local.Exclusive = true;
				r = TRUE;
			}
		}
		// Want a read-only lock? 
		else if (fileOffsetLow == (DWORD)SHARED_FIRST && numberOfBytesToLockLow == 1)
		{
			if (!file->Shared->Exclusive)
			{
				file->Local.Readers++;
				if (file->Local.Readers == 1)
					file->Shared->Readers++;
				r = TRUE;
			}
		}
		// Want a pending lock?
		else if (fFileOffsetLow == (DWORD)PENDING_BYTE && numberOfBytesToLockLow == 1)
		{
			// If no pending lock has been acquired, then acquire it
			if (!file->Shared->Pending) 
			{
				file->Shared->Pending = true;
				file->Local.Pending = true;
				r = TRUE;
			}
		}
		// Want a reserved lock?
		else if (fileOffsetLow == (DWORD)RESERVED_BYTE && numberOfBytesToLockLow == 1)
		{
			if (!file->Shared->Reserved)
			{
				file->Shared->Reserved = true;
				file->Local.Reserved = true;
				r = TRUE;
			}
		}
		winceMutexRelease(file->Mutex);
		return r;
	}

	static BOOL winceUnlockFile(LPHANDLE fileHandle, DWORD fileOffsetLow, DWORD fileOffsetHigh, DWORD numberOfBytesToUnlockLow, DWORD numberOfBytesToUnlockHigh)
	{
		WinVFile *file = HANDLE_TO_WINFILE(fileHandle);
		BOOL r = FALSE;
		if (!file->Mutex) return true;
		winceMutexAcquire(file->Mutex);
		// Releasing a reader lock or an exclusive lock
		if (fileOffsetLow == (DWORD)SHARED_FIRST)
		{
			// Did we have an exclusive lock?
			if (file->Local.Exclusive)
			{
				_assert(numberOfBytesToUnlockLow == (DWORD)SHARED_SIZE);
				file->Local.Exclusive = false;
				file->Shared->Exclusive = false;
				r = TRUE;
			}
			// Did we just have a reader lock?
			else if (file->Local.Readers)
			{
				_assert(numberOfBytesToUnlockLow == (DWORD)SHARED_SIZE || numberOfBytesToUnlockLow == 1);
				file->Local.Readers--;
				if (file->Local.Readers == 0)
					file->Shared->Readers--;
				r = TRUE;
			}
		}
		// Releasing a pending lock
		else if (fileOffsetLow == (DWORD)PENDING_BYTE && numberOfBytesToUnlockLow == 1)
		{
			if (file->Local.Pending)
			{
				file->Local.Pending = false;
				file->Shared->Pending = false;
				r = TRUE;
			}
		}
		// Releasing a reserved lock
		else if (fileOffsetLow == (DWORD)RESERVED_BYTE && numberOfBytesToUnlockLow == 1)
		{
			if (file->Local.Reserved)
			{
				file->Local.Reserved = false;
				file->Shared->Reserved = false;
				r = TRUE;
			}
		}
		winceMutexRelease(file->Mutex);
		return r;
	}

#endif
#pragma endregion

#pragma region Locking

	static BOOL winLockFile(LPHANDLE fileHandle, DWORD flags, DWORD offsetLow, DWORD offsetHigh, DWORD numBytesLow, DWORD numBytesHigh)
	{
#if OS_WINCE
		// NOTE: Windows CE is handled differently here due its lack of the Win32 API LockFile.
		return winceLockFile(fileHandle, offsetLow, offsetHigh, numBytesLow, numBytesHigh);
#else
		if (isNT())
		{
			OVERLAPPED ovlp;
			memset(&ovlp, 0, sizeof(OVERLAPPED));
			ovlp.Offset = offsetLow;
			ovlp.OffsetHigh = offsetHigh;
			return osLockFileEx(*fileHandle, flags, 0, numBytesLow, numBytesHigh, &ovlp);
		}
		else
			return osLockFile(*fileHandle, offsetLow, offsetHigh, numBytesLow, numBytesHigh);
#endif
	}

	static BOOL winUnlockFile(LPHANDLE fileHandle, DWORD offsetLow, DWORD offsetHigh, DWORD numBytesLow, DWORD numBytesHigh)
	{
#if OS_WINCE
		// NOTE: Windows CE is handled differently here due its lack of the Win32 API UnlockFile.
		return winceUnlockFile(fileHandle, offsetLow, offsetHigh, numBytesLow, numBytesHigh);
#else
		if (isNT())
		{
			OVERLAPPED ovlp;
			memset(&ovlp, 0, sizeof(OVERLAPPED));
			ovlp.Offset = offsetLow;
			ovlp.OffsetHigh = offsetHigh;
			return osUnlockFileEx(*fileHandle, 0, numBytesLow, numBytesHigh, &ovlp);
		}
		else
			return osUnlockFile(*fileHandle, offsetLow, offsetHigh, numBytesLow, numBytesHigh);
#endif
	}

#pragma endregion

#pragma region WinVFile

	static int seekWinFile(WinVFile *file, int64 offset)
	{
#if !OS_WINRT
		LONG upperBits = (LONG)((iOffset>>32) & 0x7fffffff); // Most sig. 32 bits of new offset
		LONG lowerBits = (LONG)(iOffset & 0xffffffff); // Least sig. 32 bits of new offset
		// API oddity: If successful, SetFilePointer() returns a dword containing the lower 32-bits of the new file-offset. Or, if it fails,
		// it returns INVALID_SET_FILE_POINTER. However according to MSDN, INVALID_SET_FILE_POINTER may also be a valid new offset. So to determine 
		// whether an error has actually occurred, it is also necessary to call GetLastError().
		DWORD dwRet = osSetFilePointer(file->H, lowerBits, &upperBits, FILE_BEGIN); // Value returned by SetFilePointer()
		DWORD lastErrno; // Value returned by GetLastError()
		if ((dwRet == INVALID_SET_FILE_POINTER && ((lastErrno = osGetLastError()) != NO_ERROR)))
		{
			file->LastErrno = lastErrno;
			winLogError(RC::IOERR_SEEK, file->LastErrno, "seekWinFile", file->Path);
			return 1;
		}
		return 0;
#else
		// Same as above, except that this implementation works for WinRT.
		LARGE_INTEGER x; // The new offset
		x.QuadPart = offset; 
		BOOL ret = osSetFilePointerEx(file->H, x, 0, FILE_BEGIN); // Value returned by SetFilePointerEx()
		if (!ret)
		{
			file->LastErrno = osGetLastError();
			winLogError(RC::IOERR_SEEK, file->LastErrno, "seekWinFile", file->Path);
			return 1;
		}
		return 0;
#endif
	}

#define MX_CLOSE_ATTEMPT 3
	RC WinVFile::Close()
	{
#ifndef OMIT_WAL
		_assert(Shm == 0);
#endif
		TRACE("CLOSE %d\n", file->H);
		_assert(H != NULL && H != INVALID_HANDLE_VALUE);
		int rc;
		int cnt = 0;
		do
		{
			rc = osCloseHandle(H);
		} while (!rc && ++cnt < MX_CLOSE_ATTEMPT && win32_Sleep(100));
#if OS_WINCE
#define WINCE_DELETION_ATTEMPTS 3
		winceDestroyLock(this);
		if (DeleteOnClose)
		{
			int cnt = 0;
			while (osDeleteFileW(DeleteOnClose) == 0 && osGetFileAttributesW(DeleteOnClose) != 0xffffffff && cnt++ < WINCE_DELETION_ATTEMPTS)
				win32_Sleep(100);  // Wait a little before trying again
			SysEx::Free(DeleteOnClose);
		}
#endif
		TRACE("CLOSE %d %s\n", H, rc ? "ok" : "failed");
		if (rc)
			H = NULL;
		OpenCounter(-1);
		return (rc ? RC::OK : winLogError(RC::IOERR_CLOSE, osGetLastError(), "winClose", Path));
	}

	RC WinVFile::Read(void *buffer, int amount, int64 offset)
	{
#if !OS_WINCE
		OVERLAPPED overlapped; // The offset for ReadFile.
#endif
		int retry = 0; // Number of retrys
		SimulateIOError(return RC::IOERR_READ);
		TRACE("READ %d lock=%d\n", H, Lock);
		DWORD read; // Number of bytes actually read from file
#if OS_WINCE
		if (seekWinFile(this, offset))
			return RC::FULL;
		while (!osReadFile(H, buffer, amount, &read, 0))
		{
#else
		memset(&overlapped, 0, sizeof(OVERLAPPED));
		overlapped.Offset = (LONG)(offset & 0xffffffff);
		overlapped.OffsetHigh = (LONG)((offset>>32) & 0x7fffffff);
		while (!osReadFile(H, buffer, amount, &read, &overlapped) && osGetLastError() != ERROR_HANDLE_EOF)
		{
#endif
			DWORD lastErrno;
			if (retryIoerr(&retry, &lastErrno)) continue;
			LastErrno = lastErrno;
			return winLogError(RC::IOERR_READ, LastErrno, "winRead", Path);
		}
		logIoerr(retry);
		if (read < (DWORD)amount)
		{
			// Unread parts of the buffer must be zero-filled
			memset(&((char *)buffer)[read], 0, amount - read);
			return RC::IOERR_SHORT_READ;
		}
		return RC::OK;
	}

	RC WinVFile::Write(const void *buffer, int amount, int64 offset)
	{
		_assert(amount > 0);
		SimulateIOError(return RC::IOERR_WRITE);
		SimulateDiskfullError(return RC::FULL);
		TRACE("WRITE %d lock=%d\n", H, Lock);
		int rc = 0; // True if error has occurred, else false
#if OS_WINCE
		rc = seekWinFile(this, offset);
		if (!rc)
		{
#else
		{
#endif
#if !OS_WINCE
			OVERLAPPED overlapped; // The offset for WriteFile.
			memset(&overlapped, 0, sizeof(OVERLAPPED));
			overlapped.Offset = (LONG)(offset & 0xffffffff);
			overlapped.OffsetHigh = (LONG)((offset>>32) & 0x7fffffff);
#endif
			uint8 *remain = (uint8 *)buffer; // Data yet to be written
			int remainLength = amount; // Number of bytes yet to be written
			DWORD write; // Bytes written by each WriteFile() call
			DWORD lastErrno = NO_ERROR; // Value returned by GetLastError()
			int retry = 0; // Number of retries
			while (remainLength > 0)
			{
#if OS_WINCE
				if (!osWriteFile(H, remain, remainLength, &write, 0)) {
#else
				if (!osWriteFile(H, remain, remainLength, &write, &overlapped)) {
#endif
					if (retryIoerr(&retry, &lastErrno)) continue;
					break;
				}
				_assert(write == 0 || write <= (DWORD)remainLength);
				if (write == 0 || write > (DWORD)remainLength)
				{
					lastErrno = osGetLastError();
					break;
				}
#if !OS_WINCE
				offset += write;
				overlapped.Offset = (LONG)(offset & 0xffffffff);
				overlapped.OffsetHigh = (LONG)((offset>>32) & 0x7fffffff);
#endif
				remain += write;
				remainLength -= write;
			}
			if (remainLength > 0)
			{
				LastErrno = lastErrno;
				rc = 1;
			}
		}
		if (rc)
		{
			if (LastErrno == OSTRACE ||  LastErrno == ERROR_DISK_FULL)
				return RC::FULL;
			return winLogError(RC::IOERR_WRITE, LastErrno, "winWrite", Path);
		}
		else
			logIoerr(retry);
		return RC::OK;
	}

	RC WinVFile::Truncate(int64 size)
	{
		RC rc = RC::OK;
		TRACE("TRUNCATE %d %lld\n", H, size);
		SimulateIOError(return RC::IOERR_TRUNCATE);
		// If the user has configured a chunk-size for this file, truncate the file so that it consists of an integer number of chunks (i.e. the
		// actual file size after the operation may be larger than the requested size).
		if (SizeChunk > 0)
			size = ((size+SizeChunk-1)/SizeChunk)*SizeChunk;
		// SetEndOfFile() returns non-zero when successful, or zero when it fails.
		if (seekWinFile(this, size))
			rc = winLogError(RC::IOERR_TRUNCATE, LastErrno, "winTruncate1", Path);
		else if (!osSetEndOfFile(H))
		{
			LastErrno = osGetLastError();
			rc = winLogError(RC::IOERR_TRUNCATE, LastErrno, "winTruncate2", Path);
		}
		TRACE("TRUNCATE %d %lld %s\n", H, size, rc ? "failed" : "ok");
		return rc;
	}

#ifdef TEST
	// Count the number of fullsyncs and normal syncs.  This is used to test that syncs and fullsyncs are occuring at the right times.
	int sync_count = 0;
	int fullsync_count = 0;
#endif

	RC WinVFile::Sync(int flags)
	{
		// Check that one of SQLITE_SYNC_NORMAL or FULL was passed
		_assert((flags&0x0F) == SYNC::NORMAL || (flags&0x0F) == SYNC::FULL);
		TRACE("SYNC %d lock=%d\n", H, Lock);
		// Unix cannot, but some systems may return SQLITE_FULL from here. This line is to test that doing so does not cause any problems.
		SimulateDiskfullError(return RC::FULL);
#ifdef TEST
		if ((flags&0x0F) == SYNC::FULL)
			fullsync_count++;
		sync_count++;
#endif
#ifdef NO_SYNC // If we compiled with the SQLITE_NO_SYNC flag, then syncing is a no-op
		return RC::OK;
#else
		BOOL rc = osFlushFileBuffers(H);
		SimulateIOError(rc = FALSE);
		if (rc)
			return RC::OK;
		LastErrno = osGetLastError();
		return winLogError(RC::IOERR_FSYNC, LastErrno, "winSync", Path);
#endif
	}

	RC WinVFile::get_FileSize(int64 *size)
	{
		RC rc = RC::OK;
		SimulateIOError(return RC::IOERR_FSTAT);
#if OS_WINRT
		{
			FILE_STANDARD_INFO info;
			if (osGetFileInformationByHandleEx(H, FileStandardInfo, &info, sizeof(info)))
				*size = info.EndOfFile.QuadPart;
			else
			{
				LastErrno = osGetLastError();
				rc = winLogError(RC::IOERR_FSTAT, LastErrno, "winFileSize", Path);
			}
		}
#else
		{
			DWORD upperBits;
			DWORD lowerBits = osGetFileSize(H, &upperBits);
			*size = (((int64)upperBits)<<32) + lowerBits;
			DWORD lastErrno;
			if (lowerBits == INVALID_FILE_SIZE && (lastErrno = osGetLastError()) != NO_ERROR)
			{
				LastErrno = lastErrno;
				rc = winLogError(RC::IOERR_FSTAT, LastErrno, "winFileSize", Path);
			}
		}
#endif
		return rc;
	}

	static int getReadLock(WinVFile *file)
	{
		int res;
		if (isNT())
		{
#if OS_WINCE
			// NOTE: Windows CE is handled differently here due its lack of the Win32 API LockFileEx.
			res = winceLockFile(&file->H, SHARED_FIRST, 0, 1, 0);
#else
			res = winLockFile(&file->H, SQLITE_LOCKFILEEX_FLAGS, SHARED_FIRST, 0, SHARED_SIZE, 0);
#endif
		}
#ifdef WIN32_HAS_ANSI
		else
		{
			int lock;
			sqlite3_randomness(sizeof(lock), &lock);
			file->SharedLockByte = (short)((lock & 0x7fffffff)%(SHARED_SIZE - 1));
			res = winLockFile(&pFile->h, LOCKFILE_FLAGS, SHARED_FIRST + file->SharedLockByte, 0, 1, 0);
		}
#endif
		if (res == 0)
			file->LastErrno = osGetLastError();
		// No need to log a failure to lock
		return res;
	}

	static int unlockReadLock(WinVFile *file)
	{
		int res;
		if (isNT())
			res = winUnlockFile(&file->H, SHARED_FIRST, 0, SHARED_SIZE, 0);
#ifdef WIN32_HAS_ANSI
		else
			res = winUnlockFile(&file->H, SHARED_FIRST + file->SharedLockByte, 0, 1, 0);
#endif
		DWORD lastErrno;
		if (res == 0 && (lastErrno = osGetLastError()) != ERROR_NOT_LOCKED)
		{
			file->LastErrno = lastErrno;
			winLogError(RC::IOERR_UNLOCK, file->LastErrno, "unlockReadLock", file->Path);
		}
		return res;
	}

	RC WinVFile::Lock(LOCK lock)
	{
		OSTRACE("LOCK %d %d was %d(%d)\n", H, lock, Lock, SharedLockByte);

		// If there is already a lock of this type or more restrictive on the OsFile, do nothing. Don't use the end_lock: exit path, as
		// sqlite3OsEnterMutex() hasn't been called yet.
		if (Lock >= lock)
			return RC::OK;

		// Make sure the locking sequence is correct
		_assert(Lock != LOCK::NO || lock == LOCK::SHARED);
		_assert(lock != LOCK::PENDING);
		_assert(lock != LOCK::RESERVED || Lock == LOCK::SHARED);

		// Lock the PENDING_LOCK byte if we need to acquire a PENDING lock or a SHARED lock.  If we are acquiring a SHARED lock, the acquisition of
		// the PENDING_LOCK byte is temporary.
		LOCK newLock = Lock; // Set pFile->locktype to this value before exiting
		int res = 1; // Result of a Windows lock call
		bool gotPendingLock = false; // True if we acquired a PENDING lock this time
		DWORD lastErrno = NO_ERROR;
		if (Lock == LOCK::NO || (lock == LOCK::EXCLUSIVE && Lock == LOCK::RESERVED))
		{
			int cnt = 3;
			while (cnt-- > 0 && (res = winLockFile(&H, LOCKFILE_FLAGS, PENDING_BYTE, 0, 1, 0)) == 0)
			{
				// Try 3 times to get the pending lock.  This is needed to work around problems caused by indexing and/or anti-virus software on Windows systems.
				// If you are using this code as a model for alternative VFSes, do not copy this retry logic.  It is a hack intended for Windows only.
				OSTRACE("could not get a PENDING lock. cnt=%d\n", cnt);
				if (cnt) win32_Sleep(1);
			}
			gotPendingLock = res;
			if (!res)
				lastErrno = osGetLastError();
		}

		// Acquire a SHARED lock
		if (lock == LOCK::SHARED && res)
		{
			_assert(Lock == LOCK::NO);
			res = getReadLock(this);
			if (res)
				newLock = LOCK::SHARED;
			else
				lastErrno = osGetLastError();
		}

		// Acquire a RESERVED lock
		if (lock == LOCK::RESERVED && res)
		{
			_assert(Lock == LOCK::SHARED);
			res = winLockFile(&H, LOCKFILE_FLAGS, RESERVED_BYTE, 0, 1, 0);
			if (res)
				newLock = LOCK::RESERVED;
			else
				lastErrno = osGetLastError();
		}

		// Acquire a PENDING lock
		if (lock == LOCK::EXCLUSIVE && res)
		{
			newLock = LOCK::PENDING;
			gotPendingLock = false;
		}

		// Acquire an EXCLUSIVE lock
		if (lock == LOCK::EXCLUSIVE && res)
		{
			_assert(Lock >= LOCK::SHARED);
			res = unlockReadLock(this);
			OSTRACE("unreadlock = %d\n", res);
			res = winLockFile(&H, LOCKFILE_FLAGS, SHARED_FIRST, 0, SHARED_SIZE, 0);
			if (res)
				newLock = LOCK::EXCLUSIVE;
			else
			{
				lastErrno = osGetLastError();
				OSTRACE("error-code = %d\n", lastErrno);
				getReadLock(this);
			}
		}

		// If we are holding a PENDING lock that ought to be released, then release it now.
		if (gotPendingLock && lock == LOCK::SHARED)
			winUnlockFile(&H, PENDING_BYTE, 0, 1, 0);

		// Update the state of the lock has held in the file descriptor then return the appropriate result code.
		RC rc;
		if (res)
			rc = RC::OK;
		else
		{
			OSTRACE("LOCK FAILED %d trying for %d but got %d\n", H, lock, newLock);
			LastErrno = lastErrno;
			rc = RC::BUSY;
		}
		Lock = newLock;
		return rc;
	}

	RC WinVFile::CheckReservedLock(int *resOut)
	{
		SimulateIOError(return RC::IOERR_CHECKRESERVEDLOCK;);
		int rc;
		if (Lock >= LOCK::RESERVED)
		{
			rc = 1;
			OSTRACE("TEST WR-LOCK %d %d (local)\n", H, rc);
		}
		else
		{
			rc = winLockFile(&H, LOCKFILEEX_FLAGS,RESERVED_BYTE, 0, 1, 0);
			if (rc)
				winUnlockFile(&H, RESERVED_BYTE, 0, 1, 0);
			rc = !rc;
			OSTRACE("TEST WR-LOCK %d %d (remote)\n", H, rc);
		}
		*resOut = rc;
		return RC::OK;
	}

	/*
	** Lower the locking level on file descriptor id to locktype.  locktype
	** must be either NO_LOCK or SHARED_LOCK.
	**
	** If the locking level of the file descriptor is already at or below
	** the requested locking level, this routine is a no-op.
	**
	** It is not possible for this routine to fail if the second argument
	** is NO_LOCK.  If the second argument is SHARED_LOCK then this routine
	** might return SQLITE_IOERR;
	*/
	static int winUnlock(sqlite3_file *id, int locktype){
		int type;
		winFile *pFile = (winFile*)id;
		int rc = SQLITE_OK;
		assert( pFile!=0 );
		assert( locktype<=SHARED_LOCK );
		OSTRACE(("UNLOCK %d to %d was %d(%d)\n", pFile->h, locktype,
			pFile->locktype, pFile->sharedLockByte));
		type = pFile->locktype;
		if( type>=EXCLUSIVE_LOCK ){
			winUnlockFile(&pFile->h, SHARED_FIRST, 0, SHARED_SIZE, 0);
			if( locktype==SHARED_LOCK && !getReadLock(pFile) ){
				/* This should never happen.  We should always be able to
				** reacquire the read lock */
				rc = winLogError(SQLITE_IOERR_UNLOCK, osGetLastError(),
					"winUnlock", pFile->zPath);
			}
		}
		if( type>=RESERVED_LOCK ){
			winUnlockFile(&pFile->h, RESERVED_BYTE, 0, 1, 0);
		}
		if( locktype==NO_LOCK && type>=SHARED_LOCK ){
			unlockReadLock(pFile);
		}
		if( type>=PENDING_LOCK ){
			winUnlockFile(&pFile->h, PENDING_BYTE, 0, 1, 0);
		}
		pFile->locktype = (u8)locktype;
		return rc;
	}

	/*
	** If *pArg is inititially negative then this is a query.  Set *pArg to
	** 1 or 0 depending on whether or not bit mask of pFile->ctrlFlags is set.
	**
	** If *pArg is 0 or 1, then clear or set the mask bit of pFile->ctrlFlags.
	*/
	static void winModeBit(winFile *pFile, unsigned char mask, int *pArg){
		if( *pArg<0 ){
			*pArg = (pFile->ctrlFlags & mask)!=0;
		}else if( (*pArg)==0 ){
			pFile->ctrlFlags &= ~mask;
		}else{
			pFile->ctrlFlags |= mask;
		}
	}

	/* Forward declaration */
	static int getTempname(int nBuf, char *zBuf);

	/*
	** Control and query of the open file handle.
	*/
	static int winFileControl(sqlite3_file *id, int op, void *pArg){
		winFile *pFile = (winFile*)id;
		switch( op ){
		case SQLITE_FCNTL_LOCKSTATE: {
			*(int*)pArg = pFile->locktype;
			return SQLITE_OK;
									 }
		case SQLITE_LAST_ERRNO: {
			*(int*)pArg = (int)pFile->lastErrno;
			return SQLITE_OK;
								}
		case SQLITE_FCNTL_CHUNK_SIZE: {
			pFile->szChunk = *(int *)pArg;
			return SQLITE_OK;
									  }
		case SQLITE_FCNTL_SIZE_HINT: {
			if( pFile->szChunk>0 ){
				sqlite3_int64 oldSz;
				int rc = winFileSize(id, &oldSz);
				if( rc==SQLITE_OK ){
					sqlite3_int64 newSz = *(sqlite3_int64*)pArg;
					if( newSz>oldSz ){
						SimulateIOErrorBenign(1);
						rc = winTruncate(id, newSz);
						SimulateIOErrorBenign(0);
					}
				}
				return rc;
			}
			return SQLITE_OK;
									 }
		case SQLITE_FCNTL_PERSIST_WAL: {
			winModeBit(pFile, WINFILE_PERSIST_WAL, (int*)pArg);
			return SQLITE_OK;
									   }
		case SQLITE_FCNTL_POWERSAFE_OVERWRITE: {
			winModeBit(pFile, WINFILE_PSOW, (int*)pArg);
			return SQLITE_OK;
											   }
		case SQLITE_FCNTL_VFSNAME: {
			*(char**)pArg = sqlite3_mprintf("win32");
			return SQLITE_OK;
								   }
		case SQLITE_FCNTL_WIN32_AV_RETRY: {
			int *a = (int*)pArg;
			if( a[0]>0 ){
				win32IoerrRetry = a[0];
			}else{
				a[0] = win32IoerrRetry;
			}
			if( a[1]>0 ){
				win32IoerrRetryDelay = a[1];
			}else{
				a[1] = win32IoerrRetryDelay;
			}
			return SQLITE_OK;
										  }
		case SQLITE_FCNTL_TEMPFILENAME: {
			char *zTFile = sqlite3MallocZero( pFile->pVfs->mxPathname );
			if( zTFile ){
				getTempname(pFile->pVfs->mxPathname, zTFile);
				*(char**)pArg = zTFile;
			}
			return SQLITE_OK;
										}
		}
		return SQLITE_NOTFOUND;
	}

	/*
	** Return the sector size in bytes of the underlying block device for
	** the specified file. This is almost always 512 bytes, but may be
	** larger for some devices.
	**
	** SQLite code assumes this function cannot fail. It also assumes that
	** if two files are created in the same file-system directory (i.e.
	** a database and its journal file) that the sector size will be the
	** same for both.
	*/
	static int winSectorSize(sqlite3_file *id){
		(void)id;
		return SQLITE_DEFAULT_SECTOR_SIZE;
	}

	/*
	** Return a vector of device characteristics.
	*/
	static int winDeviceCharacteristics(sqlite3_file *id){
		winFile *p = (winFile*)id;
		return SQLITE_IOCAP_UNDELETABLE_WHEN_OPEN |
			((p->ctrlFlags & WINFILE_PSOW)?SQLITE_IOCAP_POWERSAFE_OVERWRITE:0);
	}

#ifndef SQLITE_OMIT_WAL

	/* 
	** Windows will only let you create file view mappings
	** on allocation size granularity boundaries.
	** During sqlite3_os_init() we do a GetSystemInfo()
	** to get the granularity size.
	*/
	SYSTEM_INFO winSysInfo;

	/*
	** Helper functions to obtain and relinquish the global mutex. The
	** global mutex is used to protect the winLockInfo objects used by 
	** this file, all of which may be shared by multiple threads.
	**
	** Function winShmMutexHeld() is used to assert() that the global mutex 
	** is held when required. This function is only used as part of assert() 
	** statements. e.g.
	**
	**   winShmEnterMutex()
	**     assert( winShmMutexHeld() );
	**   winShmLeaveMutex()
	*/
	static void winShmEnterMutex(void){
		sqlite3_mutex_enter(sqlite3MutexAlloc(SQLITE_MUTEX_STATIC_MASTER));
	}
	static void winShmLeaveMutex(void){
		sqlite3_mutex_leave(sqlite3MutexAlloc(SQLITE_MUTEX_STATIC_MASTER));
	}
#ifdef SQLITE_DEBUG
	static int winShmMutexHeld(void) {
		return sqlite3_mutex_held(sqlite3MutexAlloc(SQLITE_MUTEX_STATIC_MASTER));
	}
#endif

	/*
	** Object used to represent a single file opened and mmapped to provide
	** shared memory.  When multiple threads all reference the same
	** log-summary, each thread has its own winFile object, but they all
	** point to a single instance of this object.  In other words, each
	** log-summary is opened only once per process.
	**
	** winShmMutexHeld() must be true when creating or destroying
	** this object or while reading or writing the following fields:
	**
	**      nRef
	**      pNext 
	**
	** The following fields are read-only after the object is created:
	** 
	**      fid
	**      zFilename
	**
	** Either winShmNode.mutex must be held or winShmNode.nRef==0 and
	** winShmMutexHeld() is true when reading or writing any other field
	** in this structure.
	**
	*/
	struct winShmNode {
		sqlite3_mutex *mutex;      /* Mutex to access this object */
		char *zFilename;           /* Name of the file */
		winFile hFile;             /* File handle from winOpen */

		int szRegion;              /* Size of shared-memory regions */
		int nRegion;               /* Size of array apRegion */
		struct ShmRegion {
			HANDLE hMap;             /* File handle from CreateFileMapping */
			void *pMap;
		} *aRegion;
		DWORD lastErrno;           /* The Windows errno from the last I/O error */

		int nRef;                  /* Number of winShm objects pointing to this */
		winShm *pFirst;            /* All winShm objects pointing to this */
		winShmNode *pNext;         /* Next in list of all winShmNode objects */
#ifdef SQLITE_DEBUG
		u8 nextShmId;              /* Next available winShm.id value */
#endif
	};

	/*
	** A global array of all winShmNode objects.
	**
	** The winShmMutexHeld() must be true while reading or writing this list.
	*/
	static winShmNode *winShmNodeList = 0;

	/*
	** Structure used internally by this VFS to record the state of an
	** open shared memory connection.
	**
	** The following fields are initialized when this object is created and
	** are read-only thereafter:
	**
	**    winShm.pShmNode
	**    winShm.id
	**
	** All other fields are read/write.  The winShm.pShmNode->mutex must be held
	** while accessing any read/write fields.
	*/
	struct winShm {
		winShmNode *pShmNode;      /* The underlying winShmNode object */
		winShm *pNext;             /* Next winShm with the same winShmNode */
		u8 hasMutex;               /* True if holding the winShmNode mutex */
		u16 sharedMask;            /* Mask of shared locks held */
		u16 exclMask;              /* Mask of exclusive locks held */
#ifdef SQLITE_DEBUG
		u8 id;                     /* Id of this connection with its winShmNode */
#endif
	};

	/*
	** Constants used for locking
	*/
#define WIN_SHM_BASE   ((22+SQLITE_SHM_NLOCK)*4)        /* first lock byte */
#define WIN_SHM_DMS    (WIN_SHM_BASE+SQLITE_SHM_NLOCK)  /* deadman switch */

	/*
	** Apply advisory locks for all n bytes beginning at ofst.
	*/
#define _SHM_UNLCK  1
#define _SHM_RDLCK  2
#define _SHM_WRLCK  3
	static int winShmSystemLock(
		winShmNode *pFile,    /* Apply locks to this open shared-memory segment */
		int lockType,         /* _SHM_UNLCK, _SHM_RDLCK, or _SHM_WRLCK */
		int ofst,             /* Offset to first byte to be locked/unlocked */
		int nByte             /* Number of bytes to lock or unlock */
		){
			int rc = 0;           /* Result code form Lock/UnlockFileEx() */

			/* Access to the winShmNode object is serialized by the caller */
			assert( sqlite3_mutex_held(pFile->mutex) || pFile->nRef==0 );

			/* Release/Acquire the system-level lock */
			if( lockType==_SHM_UNLCK ){
				rc = winUnlockFile(&pFile->hFile.h, ofst, 0, nByte, 0);
			}else{
				/* Initialize the locking parameters */
				DWORD dwFlags = LOCKFILE_FAIL_IMMEDIATELY;
				if( lockType == _SHM_WRLCK ) dwFlags |= LOCKFILE_EXCLUSIVE_LOCK;
				rc = winLockFile(&pFile->hFile.h, dwFlags, ofst, 0, nByte, 0);
			}

			if( rc!= 0 ){
				rc = SQLITE_OK;
			}else{
				pFile->lastErrno =  osGetLastError();
				rc = SQLITE_BUSY;
			}

			OSTRACE(("SHM-LOCK %d %s %s 0x%08lx\n", 
				pFile->hFile.h,
				rc==SQLITE_OK ? "ok" : "failed",
				lockType==_SHM_UNLCK ? "UnlockFileEx" : "LockFileEx",
				pFile->lastErrno));

			return rc;
	}

	/* Forward references to VFS methods */
	static int winOpen(sqlite3_vfs*,const char*,sqlite3_file*,int,int*);
	static int winDelete(sqlite3_vfs *,const char*,int);

	/*
	** Purge the winShmNodeList list of all entries with winShmNode.nRef==0.
	**
	** This is not a VFS shared-memory method; it is a utility function called
	** by VFS shared-memory methods.
	*/
	static void winShmPurge(sqlite3_vfs *pVfs, int deleteFlag){
		winShmNode **pp;
		winShmNode *p;
		BOOL bRc;
		assert( winShmMutexHeld() );
		pp = &winShmNodeList;
		while( (p = *pp)!=0 ){
			if( p->nRef==0 ){
				int i;
				if( p->mutex ) sqlite3_mutex_free(p->mutex);
				for(i=0; i<p->nRegion; i++){
					bRc = osUnmapViewOfFile(p->aRegion[i].pMap);
					OSTRACE(("SHM-PURGE pid-%d unmap region=%d %s\n",
						(int)osGetCurrentProcessId(), i,
						bRc ? "ok" : "failed"));
					bRc = osCloseHandle(p->aRegion[i].hMap);
					OSTRACE(("SHM-PURGE pid-%d close region=%d %s\n",
						(int)osGetCurrentProcessId(), i,
						bRc ? "ok" : "failed"));
				}
				if( p->hFile.h!=NULL && p->hFile.h!=INVALID_HANDLE_VALUE ){
					SimulateIOErrorBenign(1);
					winClose((sqlite3_file *)&p->hFile);
					SimulateIOErrorBenign(0);
				}
				if( deleteFlag ){
					SimulateIOErrorBenign(1);
					sqlite3BeginBenignMalloc();
					winDelete(pVfs, p->zFilename, 0);
					sqlite3EndBenignMalloc();
					SimulateIOErrorBenign(0);
				}
				*pp = p->pNext;
				sqlite3_free(p->aRegion);
				sqlite3_free(p);
			}else{
				pp = &p->pNext;
			}
		}
	}

	/*
	** Open the shared-memory area associated with database file pDbFd.
	**
	** When opening a new shared-memory file, if no other instances of that
	** file are currently open, in this process or in other processes, then
	** the file must be truncated to zero length or have its header cleared.
	*/
	static int winOpenSharedMemory(winFile *pDbFd){
		struct winShm *p;                  /* The connection to be opened */
		struct winShmNode *pShmNode = 0;   /* The underlying mmapped file */
		int rc;                            /* Result code */
		struct winShmNode *pNew;           /* Newly allocated winShmNode */
		int nName;                         /* Size of zName in bytes */

		assert( pDbFd->pShm==0 );    /* Not previously opened */

		/* Allocate space for the new sqlite3_shm object.  Also speculatively
		** allocate space for a new winShmNode and filename.
		*/
		p = sqlite3MallocZero( sizeof(*p) );
		if( p==0 ) return SQLITE_IOERR_NOMEM;
		nName = sqlite3Strlen30(pDbFd->zPath);
		pNew = sqlite3MallocZero( sizeof(*pShmNode) + nName + 17 );
		if( pNew==0 ){
			sqlite3_free(p);
			return SQLITE_IOERR_NOMEM;
		}
		pNew->zFilename = (char*)&pNew[1];
		sqlite3_snprintf(nName+15, pNew->zFilename, "%s-shm", pDbFd->zPath);
		sqlite3FileSuffix3(pDbFd->zPath, pNew->zFilename); 

		/* Look to see if there is an existing winShmNode that can be used.
		** If no matching winShmNode currently exists, create a new one.
		*/
		winShmEnterMutex();
		for(pShmNode = winShmNodeList; pShmNode; pShmNode=pShmNode->pNext){
			/* TBD need to come up with better match here.  Perhaps
			** use FILE_ID_BOTH_DIR_INFO Structure.
			*/
			if( sqlite3StrICmp(pShmNode->zFilename, pNew->zFilename)==0 ) break;
		}
		if( pShmNode ){
			sqlite3_free(pNew);
		}else{
			pShmNode = pNew;
			pNew = 0;
			((winFile*)(&pShmNode->hFile))->h = INVALID_HANDLE_VALUE;
			pShmNode->pNext = winShmNodeList;
			winShmNodeList = pShmNode;

			pShmNode->mutex = sqlite3_mutex_alloc(SQLITE_MUTEX_FAST);
			if( pShmNode->mutex==0 ){
				rc = SQLITE_IOERR_NOMEM;
				goto shm_open_err;
			}

			rc = winOpen(pDbFd->pVfs,
				pShmNode->zFilename,             /* Name of the file (UTF-8) */
				(sqlite3_file*)&pShmNode->hFile,  /* File handle here */
				SQLITE_OPEN_WAL | SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE,
				0);
			if( SQLITE_OK!=rc ){
				goto shm_open_err;
			}

			/* Check to see if another process is holding the dead-man switch.
			** If not, truncate the file to zero length. 
			*/
			if( winShmSystemLock(pShmNode, _SHM_WRLCK, WIN_SHM_DMS, 1)==SQLITE_OK ){
				rc = winTruncate((sqlite3_file *)&pShmNode->hFile, 0);
				if( rc!=SQLITE_OK ){
					rc = winLogError(SQLITE_IOERR_SHMOPEN, osGetLastError(),
						"winOpenShm", pDbFd->zPath);
				}
			}
			if( rc==SQLITE_OK ){
				winShmSystemLock(pShmNode, _SHM_UNLCK, WIN_SHM_DMS, 1);
				rc = winShmSystemLock(pShmNode, _SHM_RDLCK, WIN_SHM_DMS, 1);
			}
			if( rc ) goto shm_open_err;
		}

		/* Make the new connection a child of the winShmNode */
		p->pShmNode = pShmNode;
#ifdef SQLITE_DEBUG
		p->id = pShmNode->nextShmId++;
#endif
		pShmNode->nRef++;
		pDbFd->pShm = p;
		winShmLeaveMutex();

		/* The reference count on pShmNode has already been incremented under
		** the cover of the winShmEnterMutex() mutex and the pointer from the
		** new (struct winShm) object to the pShmNode has been set. All that is
		** left to do is to link the new object into the linked list starting
		** at pShmNode->pFirst. This must be done while holding the pShmNode->mutex 
		** mutex.
		*/
		sqlite3_mutex_enter(pShmNode->mutex);
		p->pNext = pShmNode->pFirst;
		pShmNode->pFirst = p;
		sqlite3_mutex_leave(pShmNode->mutex);
		return SQLITE_OK;

		/* Jump here on any error */
shm_open_err:
		winShmSystemLock(pShmNode, _SHM_UNLCK, WIN_SHM_DMS, 1);
		winShmPurge(pDbFd->pVfs, 0);      /* This call frees pShmNode if required */
		sqlite3_free(p);
		sqlite3_free(pNew);
		winShmLeaveMutex();
		return rc;
	}

	/*
	** Close a connection to shared-memory.  Delete the underlying 
	** storage if deleteFlag is true.
	*/
	static int winShmUnmap(
		sqlite3_file *fd,          /* Database holding shared memory */
		int deleteFlag             /* Delete after closing if true */
		){
			winFile *pDbFd;       /* Database holding shared-memory */
			winShm *p;            /* The connection to be closed */
			winShmNode *pShmNode; /* The underlying shared-memory file */
			winShm **pp;          /* For looping over sibling connections */

			pDbFd = (winFile*)fd;
			p = pDbFd->pShm;
			if( p==0 ) return SQLITE_OK;
			pShmNode = p->pShmNode;

			/* Remove connection p from the set of connections associated
			** with pShmNode */
			sqlite3_mutex_enter(pShmNode->mutex);
			for(pp=&pShmNode->pFirst; (*pp)!=p; pp = &(*pp)->pNext){}
			*pp = p->pNext;

			/* Free the connection p */
			sqlite3_free(p);
			pDbFd->pShm = 0;
			sqlite3_mutex_leave(pShmNode->mutex);

			/* If pShmNode->nRef has reached 0, then close the underlying
			** shared-memory file, too */
			winShmEnterMutex();
			assert( pShmNode->nRef>0 );
			pShmNode->nRef--;
			if( pShmNode->nRef==0 ){
				winShmPurge(pDbFd->pVfs, deleteFlag);
			}
			winShmLeaveMutex();

			return SQLITE_OK;
	}

	/*
	** Change the lock state for a shared-memory segment.
	*/
	static int winShmLock(
		sqlite3_file *fd,          /* Database file holding the shared memory */
		int ofst,                  /* First lock to acquire or release */
		int n,                     /* Number of locks to acquire or release */
		int flags                  /* What to do with the lock */
		){
			winFile *pDbFd = (winFile*)fd;        /* Connection holding shared memory */
			winShm *p = pDbFd->pShm;              /* The shared memory being locked */
			winShm *pX;                           /* For looping over all siblings */
			winShmNode *pShmNode = p->pShmNode;
			int rc = SQLITE_OK;                   /* Result code */
			u16 mask;                             /* Mask of locks to take or release */

			assert( ofst>=0 && ofst+n<=SQLITE_SHM_NLOCK );
			assert( n>=1 );
			assert( flags==(SQLITE_SHM_LOCK | SQLITE_SHM_SHARED)
				|| flags==(SQLITE_SHM_LOCK | SQLITE_SHM_EXCLUSIVE)
				|| flags==(SQLITE_SHM_UNLOCK | SQLITE_SHM_SHARED)
				|| flags==(SQLITE_SHM_UNLOCK | SQLITE_SHM_EXCLUSIVE) );
			assert( n==1 || (flags & SQLITE_SHM_EXCLUSIVE)!=0 );

			mask = (u16)((1U<<(ofst+n)) - (1U<<ofst));
			assert( n>1 || mask==(1<<ofst) );
			sqlite3_mutex_enter(pShmNode->mutex);
			if( flags & SQLITE_SHM_UNLOCK ){
				u16 allMask = 0; /* Mask of locks held by siblings */

				/* See if any siblings hold this same lock */
				for(pX=pShmNode->pFirst; pX; pX=pX->pNext){
					if( pX==p ) continue;
					assert( (pX->exclMask & (p->exclMask|p->sharedMask))==0 );
					allMask |= pX->sharedMask;
				}

				/* Unlock the system-level locks */
				if( (mask & allMask)==0 ){
					rc = winShmSystemLock(pShmNode, _SHM_UNLCK, ofst+WIN_SHM_BASE, n);
				}else{
					rc = SQLITE_OK;
				}

				/* Undo the local locks */
				if( rc==SQLITE_OK ){
					p->exclMask &= ~mask;
					p->sharedMask &= ~mask;
				} 
			}else if( flags & SQLITE_SHM_SHARED ){
				u16 allShared = 0;  /* Union of locks held by connections other than "p" */

				/* Find out which shared locks are already held by sibling connections.
				** If any sibling already holds an exclusive lock, go ahead and return
				** SQLITE_BUSY.
				*/
				for(pX=pShmNode->pFirst; pX; pX=pX->pNext){
					if( (pX->exclMask & mask)!=0 ){
						rc = SQLITE_BUSY;
						break;
					}
					allShared |= pX->sharedMask;
				}

				/* Get shared locks at the system level, if necessary */
				if( rc==SQLITE_OK ){
					if( (allShared & mask)==0 ){
						rc = winShmSystemLock(pShmNode, _SHM_RDLCK, ofst+WIN_SHM_BASE, n);
					}else{
						rc = SQLITE_OK;
					}
				}

				/* Get the local shared locks */
				if( rc==SQLITE_OK ){
					p->sharedMask |= mask;
				}
			}else{
				/* Make sure no sibling connections hold locks that will block this
				** lock.  If any do, return SQLITE_BUSY right away.
				*/
				for(pX=pShmNode->pFirst; pX; pX=pX->pNext){
					if( (pX->exclMask & mask)!=0 || (pX->sharedMask & mask)!=0 ){
						rc = SQLITE_BUSY;
						break;
					}
				}

				/* Get the exclusive locks at the system level.  Then if successful
				** also mark the local connection as being locked.
				*/
				if( rc==SQLITE_OK ){
					rc = winShmSystemLock(pShmNode, _SHM_WRLCK, ofst+WIN_SHM_BASE, n);
					if( rc==SQLITE_OK ){
						assert( (p->sharedMask & mask)==0 );
						p->exclMask |= mask;
					}
				}
			}
			sqlite3_mutex_leave(pShmNode->mutex);
			OSTRACE(("SHM-LOCK shmid-%d, pid-%d got %03x,%03x %s\n",
				p->id, (int)osGetCurrentProcessId(), p->sharedMask, p->exclMask,
				rc ? "failed" : "ok"));
			return rc;
	}

	/*
	** Implement a memory barrier or memory fence on shared memory.  
	**
	** All loads and stores begun before the barrier must complete before
	** any load or store begun after the barrier.
	*/
	static void winShmBarrier(
		sqlite3_file *fd          /* Database holding the shared memory */
		){
			UNUSED_PARAMETER(fd);
			/* MemoryBarrier(); // does not work -- do not know why not */
			winShmEnterMutex();
			winShmLeaveMutex();
	}

	/*
	** This function is called to obtain a pointer to region iRegion of the 
	** shared-memory associated with the database file fd. Shared-memory regions 
	** are numbered starting from zero. Each shared-memory region is szRegion 
	** bytes in size.
	**
	** If an error occurs, an error code is returned and *pp is set to NULL.
	**
	** Otherwise, if the isWrite parameter is 0 and the requested shared-memory
	** region has not been allocated (by any client, including one running in a
	** separate process), then *pp is set to NULL and SQLITE_OK returned. If 
	** isWrite is non-zero and the requested shared-memory region has not yet 
	** been allocated, it is allocated by this function.
	**
	** If the shared-memory region has already been allocated or is allocated by
	** this call as described above, then it is mapped into this processes 
	** address space (if it is not already), *pp is set to point to the mapped 
	** memory and SQLITE_OK returned.
	*/
	static int winShmMap(
		sqlite3_file *fd,               /* Handle open on database file */
		int iRegion,                    /* Region to retrieve */
		int szRegion,                   /* Size of regions */
		int isWrite,                    /* True to extend file if necessary */
		void volatile **pp              /* OUT: Mapped memory */
		){
			winFile *pDbFd = (winFile*)fd;
			winShm *p = pDbFd->pShm;
			winShmNode *pShmNode;
			int rc = SQLITE_OK;

			if( !p ){
				rc = winOpenSharedMemory(pDbFd);
				if( rc!=SQLITE_OK ) return rc;
				p = pDbFd->pShm;
			}
			pShmNode = p->pShmNode;

			sqlite3_mutex_enter(pShmNode->mutex);
			assert( szRegion==pShmNode->szRegion || pShmNode->nRegion==0 );

			if( pShmNode->nRegion<=iRegion ){
				struct ShmRegion *apNew;           /* New aRegion[] array */
				int nByte = (iRegion+1)*szRegion;  /* Minimum required file size */
				sqlite3_int64 sz;                  /* Current size of wal-index file */

				pShmNode->szRegion = szRegion;

				/* The requested region is not mapped into this processes address space.
				** Check to see if it has been allocated (i.e. if the wal-index file is
				** large enough to contain the requested region).
				*/
				rc = winFileSize((sqlite3_file *)&pShmNode->hFile, &sz);
				if( rc!=SQLITE_OK ){
					rc = winLogError(SQLITE_IOERR_SHMSIZE, osGetLastError(),
						"winShmMap1", pDbFd->zPath);
					goto shmpage_out;
				}

				if( sz<nByte ){
					/* The requested memory region does not exist. If isWrite is set to
					** zero, exit early. *pp will be set to NULL and SQLITE_OK returned.
					**
					** Alternatively, if isWrite is non-zero, use ftruncate() to allocate
					** the requested memory region.
					*/
					if( !isWrite ) goto shmpage_out;
					rc = winTruncate((sqlite3_file *)&pShmNode->hFile, nByte);
					if( rc!=SQLITE_OK ){
						rc = winLogError(SQLITE_IOERR_SHMSIZE, osGetLastError(),
							"winShmMap2", pDbFd->zPath);
						goto shmpage_out;
					}
				}

				/* Map the requested memory region into this processes address space. */
				apNew = (struct ShmRegion *)sqlite3_realloc(
					pShmNode->aRegion, (iRegion+1)*sizeof(apNew[0])
					);
				if( !apNew ){
					rc = SQLITE_IOERR_NOMEM;
					goto shmpage_out;
				}
				pShmNode->aRegion = apNew;

				while( pShmNode->nRegion<=iRegion ){
					HANDLE hMap = NULL;         /* file-mapping handle */
					void *pMap = 0;             /* Mapped memory region */

#if SQLITE_OS_WINRT
					hMap = osCreateFileMappingFromApp(pShmNode->hFile.h,
						NULL, PAGE_READWRITE, nByte, NULL
						);
#elif defined(SQLITE_WIN32_HAS_WIDE)
					hMap = osCreateFileMappingW(pShmNode->hFile.h, 
						NULL, PAGE_READWRITE, 0, nByte, NULL
						);
#elif defined(SQLITE_WIN32_HAS_ANSI)
					hMap = osCreateFileMappingA(pShmNode->hFile.h, 
						NULL, PAGE_READWRITE, 0, nByte, NULL
						);
#endif
					OSTRACE(("SHM-MAP pid-%d create region=%d nbyte=%d %s\n",
						(int)osGetCurrentProcessId(), pShmNode->nRegion, nByte,
						hMap ? "ok" : "failed"));
					if( hMap ){
						int iOffset = pShmNode->nRegion*szRegion;
						int iOffsetShift = iOffset % winSysInfo.dwAllocationGranularity;
#if SQLITE_OS_WINRT
						pMap = osMapViewOfFileFromApp(hMap, FILE_MAP_WRITE | FILE_MAP_READ,
							iOffset - iOffsetShift, szRegion + iOffsetShift
							);
#else
						pMap = osMapViewOfFile(hMap, FILE_MAP_WRITE | FILE_MAP_READ,
							0, iOffset - iOffsetShift, szRegion + iOffsetShift
							);
#endif
						OSTRACE(("SHM-MAP pid-%d map region=%d offset=%d size=%d %s\n",
							(int)osGetCurrentProcessId(), pShmNode->nRegion, iOffset,
							szRegion, pMap ? "ok" : "failed"));
					}
					if( !pMap ){
						pShmNode->lastErrno = osGetLastError();
						rc = winLogError(SQLITE_IOERR_SHMMAP, pShmNode->lastErrno,
							"winShmMap3", pDbFd->zPath);
						if( hMap ) osCloseHandle(hMap);
						goto shmpage_out;
					}

					pShmNode->aRegion[pShmNode->nRegion].pMap = pMap;
					pShmNode->aRegion[pShmNode->nRegion].hMap = hMap;
					pShmNode->nRegion++;
				}
			}

shmpage_out:
			if( pShmNode->nRegion>iRegion ){
				int iOffset = iRegion*szRegion;
				int iOffsetShift = iOffset % winSysInfo.dwAllocationGranularity;
				char *p = (char *)pShmNode->aRegion[iRegion].pMap;
				*pp = (void *)&p[iOffsetShift];
			}else{
				*pp = 0;
			}
			sqlite3_mutex_leave(pShmNode->mutex);
			return rc;
	}

#else
# define winShmMap     0
# define winShmLock    0
# define winShmBarrier 0
# define winShmUnmap   0
#endif /* #ifndef SQLITE_OMIT_WAL */

	/*
	** This vector defines all the methods that can operate on an
	** sqlite3_file for win32.
	*/
	static const sqlite3_io_methods winIoMethod = {
		2,                              /* iVersion */
		winClose,                       /* xClose */
		winRead,                        /* xRead */
		winWrite,                       /* xWrite */
		winTruncate,                    /* xTruncate */
		winSync,                        /* xSync */
		winFileSize,                    /* xFileSize */
		winLock,                        /* xLock */
		winUnlock,                      /* xUnlock */
		winCheckReservedLock,           /* xCheckReservedLock */
		winFileControl,                 /* xFileControl */
		winSectorSize,                  /* xSectorSize */
		winDeviceCharacteristics,       /* xDeviceCharacteristics */
		winShmMap,                      /* xShmMap */
		winShmLock,                     /* xShmLock */
		winShmBarrier,                  /* xShmBarrier */
		winShmUnmap                     /* xShmUnmap */
	};

#pragma endregion

#pragma region VSystem


	/*
	** Convert a UTF-8 filename into whatever form the underlying
	** operating system wants filenames in.  Space to hold the result
	** is obtained from malloc and must be freed by the calling
	** function.
	*/
	static void *convertUtf8Filename(const char *zFilename){
		void *zConverted = 0;
		if( isNT() ){
			zConverted = utf8ToUnicode(zFilename);
		}
#ifdef SQLITE_WIN32_HAS_ANSI
		else{
			zConverted = sqlite3_win32_utf8_to_mbcs(zFilename);
		}
#endif
		/* caller will handle out of memory */
		return zConverted;
	}

	/*
	** Create a temporary file name in zBuf.  zBuf must be big enough to
	** hold at pVfs->mxPathname characters.
	*/
	static int getTempname(int nBuf, char *zBuf){
		static char zChars[] =
			"abcdefghijklmnopqrstuvwxyz"
			"ABCDEFGHIJKLMNOPQRSTUVWXYZ"
			"0123456789";
		size_t i, j;
		int nTempPath;
		char zTempPath[MAX_PATH+2];

		/* It's odd to simulate an io-error here, but really this is just
		** using the io-error infrastructure to test that SQLite handles this
		** function failing. 
		*/
		SimulateIOError( return SQLITE_IOERR );

		memset(zTempPath, 0, MAX_PATH+2);

		if( sqlite3_temp_directory ){
			sqlite3_snprintf(MAX_PATH-30, zTempPath, "%s", sqlite3_temp_directory);
		}
#if !SQLITE_OS_WINRT
		else if( isNT() ){
			char *zMulti;
			WCHAR zWidePath[MAX_PATH];
			osGetTempPathW(MAX_PATH-30, zWidePath);
			zMulti = unicodeToUtf8(zWidePath);
			if( zMulti ){
				sqlite3_snprintf(MAX_PATH-30, zTempPath, "%s", zMulti);
				sqlite3_free(zMulti);
			}else{
				return SQLITE_IOERR_NOMEM;
			}
		}
#ifdef SQLITE_WIN32_HAS_ANSI
		else{
			char *zUtf8;
			char zMbcsPath[MAX_PATH];
			osGetTempPathA(MAX_PATH-30, zMbcsPath);
			zUtf8 = sqlite3_win32_mbcs_to_utf8(zMbcsPath);
			if( zUtf8 ){
				sqlite3_snprintf(MAX_PATH-30, zTempPath, "%s", zUtf8);
				sqlite3_free(zUtf8);
			}else{
				return SQLITE_IOERR_NOMEM;
			}
		}
#endif
#endif

		/* Check that the output buffer is large enough for the temporary file 
		** name. If it is not, return SQLITE_ERROR.
		*/
		nTempPath = sqlite3Strlen30(zTempPath);

		if( (nTempPath + sqlite3Strlen30(SQLITE_TEMP_FILE_PREFIX) + 18) >= nBuf ){
			return SQLITE_ERROR;
		}

		for(i=nTempPath; i>0 && zTempPath[i-1]=='\\'; i--){}
		zTempPath[i] = 0;

		sqlite3_snprintf(nBuf-18, zBuf, (nTempPath > 0) ?
			"%s\\"SQLITE_TEMP_FILE_PREFIX : SQLITE_TEMP_FILE_PREFIX,
			zTempPath);
		j = sqlite3Strlen30(zBuf);
		sqlite3_randomness(15, &zBuf[j]);
		for(i=0; i<15; i++, j++){
			zBuf[j] = (char)zChars[ ((unsigned char)zBuf[j])%(sizeof(zChars)-1) ];
		}
		zBuf[j] = 0;
		zBuf[j+1] = 0;

		OSTRACE(("TEMP FILENAME: %s\n", zBuf));
		return SQLITE_OK; 
	}

	/*
	** Return TRUE if the named file is really a directory.  Return false if
	** it is something other than a directory, or if there is any kind of memory
	** allocation failure.
	*/
	static int winIsDir(const void *zConverted){
		DWORD attr;
		int rc = 0;
		DWORD lastErrno;

		if( isNT() ){
			int cnt = 0;
			WIN32_FILE_ATTRIBUTE_DATA sAttrData;
			memset(&sAttrData, 0, sizeof(sAttrData));
			while( !(rc = osGetFileAttributesExW((LPCWSTR)zConverted,
				GetFileExInfoStandard,
				&sAttrData)) && retryIoerr(&cnt, &lastErrno) ){}
			if( !rc ){
				return 0; /* Invalid name? */
			}
			attr = sAttrData.dwFileAttributes;
#if SQLITE_OS_WINCE==0
		}else{
			attr = osGetFileAttributesA((char*)zConverted);
#endif
		}
		return (attr!=INVALID_FILE_ATTRIBUTES) && (attr&FILE_ATTRIBUTE_DIRECTORY);
	}

	/*
	** Open a file.
	*/
	static int winOpen(
		sqlite3_vfs *pVfs,        /* Not used */
		const char *zName,        /* Name of the file (UTF-8) */
		sqlite3_file *id,         /* Write the SQLite file handle here */
		int flags,                /* Open mode flags */
		int *pOutFlags            /* Status return flags */
		){
			HANDLE h;
			DWORD lastErrno;
			DWORD dwDesiredAccess;
			DWORD dwShareMode;
			DWORD dwCreationDisposition;
			DWORD dwFlagsAndAttributes = 0;
#if SQLITE_OS_WINCE
			int isTemp = 0;
#endif
			winFile *pFile = (winFile*)id;
			void *zConverted;              /* Filename in OS encoding */
			const char *zUtf8Name = zName; /* Filename in UTF-8 encoding */
			int cnt = 0;

			/* If argument zPath is a NULL pointer, this function is required to open
			** a temporary file. Use this buffer to store the file name in.
			*/
			char zTmpname[MAX_PATH+2];     /* Buffer used to create temp filename */

			int rc = SQLITE_OK;            /* Function Return Code */
#if !defined(NDEBUG) || SQLITE_OS_WINCE
			int eType = flags&0xFFFFFF00;  /* Type of file to open */
#endif

			int isExclusive  = (flags & SQLITE_OPEN_EXCLUSIVE);
			int isDelete     = (flags & SQLITE_OPEN_DELETEONCLOSE);
			int isCreate     = (flags & SQLITE_OPEN_CREATE);
#ifndef NDEBUG
			int isReadonly   = (flags & SQLITE_OPEN_READONLY);
#endif
			int isReadWrite  = (flags & SQLITE_OPEN_READWRITE);

#ifndef NDEBUG
			int isOpenJournal = (isCreate && (
				eType==SQLITE_OPEN_MASTER_JOURNAL 
				|| eType==SQLITE_OPEN_MAIN_JOURNAL 
				|| eType==SQLITE_OPEN_WAL
				));
#endif

			/* Check the following statements are true: 
			**
			**   (a) Exactly one of the READWRITE and READONLY flags must be set, and 
			**   (b) if CREATE is set, then READWRITE must also be set, and
			**   (c) if EXCLUSIVE is set, then CREATE must also be set.
			**   (d) if DELETEONCLOSE is set, then CREATE must also be set.
			*/
			assert((isReadonly==0 || isReadWrite==0) && (isReadWrite || isReadonly));
			assert(isCreate==0 || isReadWrite);
			assert(isExclusive==0 || isCreate);
			assert(isDelete==0 || isCreate);

			/* The main DB, main journal, WAL file and master journal are never 
			** automatically deleted. Nor are they ever temporary files.  */
			assert( (!isDelete && zName) || eType!=SQLITE_OPEN_MAIN_DB );
			assert( (!isDelete && zName) || eType!=SQLITE_OPEN_MAIN_JOURNAL );
			assert( (!isDelete && zName) || eType!=SQLITE_OPEN_MASTER_JOURNAL );
			assert( (!isDelete && zName) || eType!=SQLITE_OPEN_WAL );

			/* Assert that the upper layer has set one of the "file-type" flags. */
			assert( eType==SQLITE_OPEN_MAIN_DB      || eType==SQLITE_OPEN_TEMP_DB 
				|| eType==SQLITE_OPEN_MAIN_JOURNAL || eType==SQLITE_OPEN_TEMP_JOURNAL 
				|| eType==SQLITE_OPEN_SUBJOURNAL   || eType==SQLITE_OPEN_MASTER_JOURNAL 
				|| eType==SQLITE_OPEN_TRANSIENT_DB || eType==SQLITE_OPEN_WAL
				);

			assert( pFile!=0 );
			memset(pFile, 0, sizeof(winFile));
			pFile->h = INVALID_HANDLE_VALUE;

#if SQLITE_OS_WINRT
			if( !sqlite3_temp_directory ){
				sqlite3_log(SQLITE_ERROR,
					"sqlite3_temp_directory variable should be set for WinRT");
			}
#endif

			/* If the second argument to this function is NULL, generate a 
			** temporary file name to use 
			*/
			if( !zUtf8Name ){
				assert(isDelete && !isOpenJournal);
				memset(zTmpname, 0, MAX_PATH+2);
				rc = getTempname(MAX_PATH+2, zTmpname);
				if( rc!=SQLITE_OK ){
					return rc;
				}
				zUtf8Name = zTmpname;
			}

			/* Database filenames are double-zero terminated if they are not
			** URIs with parameters.  Hence, they can always be passed into
			** sqlite3_uri_parameter().
			*/
			assert( (eType!=SQLITE_OPEN_MAIN_DB) || (flags & SQLITE_OPEN_URI) ||
				zUtf8Name[strlen(zUtf8Name)+1]==0 );

			/* Convert the filename to the system encoding. */
			zConverted = convertUtf8Filename(zUtf8Name);
			if( zConverted==0 ){
				return SQLITE_IOERR_NOMEM;
			}

			if( winIsDir(zConverted) ){
				sqlite3_free(zConverted);
				return SQLITE_CANTOPEN_ISDIR;
			}

			if( isReadWrite ){
				dwDesiredAccess = GENERIC_READ | GENERIC_WRITE;
			}else{
				dwDesiredAccess = GENERIC_READ;
			}

			/* SQLITE_OPEN_EXCLUSIVE is used to make sure that a new file is 
			** created. SQLite doesn't use it to indicate "exclusive access" 
			** as it is usually understood.
			*/
			if( isExclusive ){
				/* Creates a new file, only if it does not already exist. */
				/* If the file exists, it fails. */
				dwCreationDisposition = CREATE_NEW;
			}else if( isCreate ){
				/* Open existing file, or create if it doesn't exist */
				dwCreationDisposition = OPEN_ALWAYS;
			}else{
				/* Opens a file, only if it exists. */
				dwCreationDisposition = OPEN_EXISTING;
			}

			dwShareMode = FILE_SHARE_READ | FILE_SHARE_WRITE;

			if( isDelete ){
#if SQLITE_OS_WINCE
				dwFlagsAndAttributes = FILE_ATTRIBUTE_HIDDEN;
				isTemp = 1;
#else
				dwFlagsAndAttributes = FILE_ATTRIBUTE_TEMPORARY
					| FILE_ATTRIBUTE_HIDDEN
					| FILE_FLAG_DELETE_ON_CLOSE;
#endif
			}else{
				dwFlagsAndAttributes = FILE_ATTRIBUTE_NORMAL;
			}
			/* Reports from the internet are that performance is always
			** better if FILE_FLAG_RANDOM_ACCESS is used.  Ticket #2699. */
#if SQLITE_OS_WINCE
			dwFlagsAndAttributes |= FILE_FLAG_RANDOM_ACCESS;
#endif

			if( isNT() ){
#if SQLITE_OS_WINRT
				CREATEFILE2_EXTENDED_PARAMETERS extendedParameters;
				extendedParameters.dwSize = sizeof(CREATEFILE2_EXTENDED_PARAMETERS);
				extendedParameters.dwFileAttributes =
					dwFlagsAndAttributes & FILE_ATTRIBUTE_MASK;
				extendedParameters.dwFileFlags = dwFlagsAndAttributes & FILE_FLAG_MASK;
				extendedParameters.dwSecurityQosFlags = SECURITY_ANONYMOUS;
				extendedParameters.lpSecurityAttributes = NULL;
				extendedParameters.hTemplateFile = NULL;
				while( (h = osCreateFile2((LPCWSTR)zConverted,
					dwDesiredAccess,
					dwShareMode,
					dwCreationDisposition,
					&extendedParameters))==INVALID_HANDLE_VALUE &&
					retryIoerr(&cnt, &lastErrno) ){
						/* Noop */
				}
#else
				while( (h = osCreateFileW((LPCWSTR)zConverted,
					dwDesiredAccess,
					dwShareMode, NULL,
					dwCreationDisposition,
					dwFlagsAndAttributes,
					NULL))==INVALID_HANDLE_VALUE &&
					retryIoerr(&cnt, &lastErrno) ){
						/* Noop */
				}
#endif
			}
#ifdef SQLITE_WIN32_HAS_ANSI
			else{
				while( (h = osCreateFileA((LPCSTR)zConverted,
					dwDesiredAccess,
					dwShareMode, NULL,
					dwCreationDisposition,
					dwFlagsAndAttributes,
					NULL))==INVALID_HANDLE_VALUE &&
					retryIoerr(&cnt, &lastErrno) ){
						/* Noop */
				}
			}
#endif
			logIoerr(cnt);

			OSTRACE(("OPEN %d %s 0x%lx %s\n", 
				h, zName, dwDesiredAccess, 
				h==INVALID_HANDLE_VALUE ? "failed" : "ok"));

			if( h==INVALID_HANDLE_VALUE ){
				pFile->lastErrno = lastErrno;
				winLogError(SQLITE_CANTOPEN, pFile->lastErrno, "winOpen", zUtf8Name);
				sqlite3_free(zConverted);
				if( isReadWrite && !isExclusive ){
					return winOpen(pVfs, zName, id, 
						((flags|SQLITE_OPEN_READONLY) &
						~(SQLITE_OPEN_CREATE|SQLITE_OPEN_READWRITE)),
						pOutFlags);
				}else{
					return SQLITE_CANTOPEN_BKPT;
				}
			}

			if( pOutFlags ){
				if( isReadWrite ){
					*pOutFlags = SQLITE_OPEN_READWRITE;
				}else{
					*pOutFlags = SQLITE_OPEN_READONLY;
				}
			}

#if SQLITE_OS_WINCE
			if( isReadWrite && eType==SQLITE_OPEN_MAIN_DB
				&& (rc = winceCreateLock(zName, pFile))!=SQLITE_OK
				){
					osCloseHandle(h);
					sqlite3_free(zConverted);
					return rc;
			}
			if( isTemp ){
				pFile->zDeleteOnClose = zConverted;
			}else
#endif
			{
				sqlite3_free(zConverted);
			}

			pFile->pMethod = &winIoMethod;
			pFile->pVfs = pVfs;
			pFile->h = h;
			if( sqlite3_uri_boolean(zName, "psow", SQLITE_POWERSAFE_OVERWRITE) ){
				pFile->ctrlFlags |= WINFILE_PSOW;
			}
			pFile->lastErrno = NO_ERROR;
			pFile->zPath = zName;

			OpenCounter(+1);
			return rc;
	}

	/*
	** Delete the named file.
	**
	** Note that Windows does not allow a file to be deleted if some other
	** process has it open.  Sometimes a virus scanner or indexing program
	** will open a journal file shortly after it is created in order to do
	** whatever it does.  While this other process is holding the
	** file open, we will be unable to delete it.  To work around this
	** problem, we delay 100 milliseconds and try to delete again.  Up
	** to MX_DELETION_ATTEMPTs deletion attempts are run before giving
	** up and returning an error.
	*/
	static int winDelete(
		sqlite3_vfs *pVfs,          /* Not used on win32 */
		const char *zFilename,      /* Name of file to delete */
		int syncDir                 /* Not used on win32 */
		){
			int cnt = 0;
			int rc;
			DWORD attr;
			DWORD lastErrno;
			void *zConverted;
			UNUSED_PARAMETER(pVfs);
			UNUSED_PARAMETER(syncDir);

			SimulateIOError(return SQLITE_IOERR_DELETE);
			zConverted = convertUtf8Filename(zFilename);
			if( zConverted==0 ){
				return SQLITE_IOERR_NOMEM;
			}
			if( isNT() ){
				do {
#if SQLITE_OS_WINRT
					WIN32_FILE_ATTRIBUTE_DATA sAttrData;
					memset(&sAttrData, 0, sizeof(sAttrData));
					if ( osGetFileAttributesExW(zConverted, GetFileExInfoStandard,
						&sAttrData) ){
							attr = sAttrData.dwFileAttributes;
					}else{
						lastErrno = osGetLastError();
						if( lastErrno==ERROR_FILE_NOT_FOUND
							|| lastErrno==ERROR_PATH_NOT_FOUND ){
								rc = SQLITE_IOERR_DELETE_NOENT; /* Already gone? */
						}else{
							rc = SQLITE_ERROR;
						}
						break;
					}
#else
					attr = osGetFileAttributesW(zConverted);
#endif
					if ( attr==INVALID_FILE_ATTRIBUTES ){
						lastErrno = osGetLastError();
						if( lastErrno==ERROR_FILE_NOT_FOUND
							|| lastErrno==ERROR_PATH_NOT_FOUND ){
								rc = SQLITE_IOERR_DELETE_NOENT; /* Already gone? */
						}else{
							rc = SQLITE_ERROR;
						}
						break;
					}
					if ( attr&FILE_ATTRIBUTE_DIRECTORY ){
						rc = SQLITE_ERROR; /* Files only. */
						break;
					}
					if ( osDeleteFileW(zConverted) ){
						rc = SQLITE_OK; /* Deleted OK. */
						break;
					}
					if ( !retryIoerr(&cnt, &lastErrno) ){
						rc = SQLITE_ERROR; /* No more retries. */
						break;
					}
				} while(1);
			}
#ifdef SQLITE_WIN32_HAS_ANSI
			else{
				do {
					attr = osGetFileAttributesA(zConverted);
					if ( attr==INVALID_FILE_ATTRIBUTES ){
						lastErrno = osGetLastError();
						if( lastErrno==ERROR_FILE_NOT_FOUND
							|| lastErrno==ERROR_PATH_NOT_FOUND ){
								rc = SQLITE_IOERR_DELETE_NOENT; /* Already gone? */
						}else{
							rc = SQLITE_ERROR;
						}
						break;
					}
					if ( attr&FILE_ATTRIBUTE_DIRECTORY ){
						rc = SQLITE_ERROR; /* Files only. */
						break;
					}
					if ( osDeleteFileA(zConverted) ){
						rc = SQLITE_OK; /* Deleted OK. */
						break;
					}
					if ( !retryIoerr(&cnt, &lastErrno) ){
						rc = SQLITE_ERROR; /* No more retries. */
						break;
					}
				} while(1);
			}
#endif
			if( rc && rc!=SQLITE_IOERR_DELETE_NOENT ){
				rc = winLogError(SQLITE_IOERR_DELETE, lastErrno,
					"winDelete", zFilename);
			}else{
				logIoerr(cnt);
			}
			sqlite3_free(zConverted);
			OSTRACE(("DELETE \"%s\" %s\n", zFilename, (rc ? "failed" : "ok" )));
			return rc;
	}

	/*
	** Check the existence and status of a file.
	*/
	static int winAccess(
		sqlite3_vfs *pVfs,         /* Not used on win32 */
		const char *zFilename,     /* Name of file to check */
		int flags,                 /* Type of test to make on this file */
		int *pResOut               /* OUT: Result */
		){
			DWORD attr;
			int rc = 0;
			DWORD lastErrno;
			void *zConverted;
			UNUSED_PARAMETER(pVfs);

			SimulateIOError( return SQLITE_IOERR_ACCESS; );
			zConverted = convertUtf8Filename(zFilename);
			if( zConverted==0 ){
				return SQLITE_IOERR_NOMEM;
			}
			if( isNT() ){
				int cnt = 0;
				WIN32_FILE_ATTRIBUTE_DATA sAttrData;
				memset(&sAttrData, 0, sizeof(sAttrData));
				while( !(rc = osGetFileAttributesExW((LPCWSTR)zConverted,
					GetFileExInfoStandard, 
					&sAttrData)) && retryIoerr(&cnt, &lastErrno) ){}
				if( rc ){
					/* For an SQLITE_ACCESS_EXISTS query, treat a zero-length file
					** as if it does not exist.
					*/
					if(    flags==SQLITE_ACCESS_EXISTS
						&& sAttrData.nFileSizeHigh==0 
						&& sAttrData.nFileSizeLow==0 ){
							attr = INVALID_FILE_ATTRIBUTES;
					}else{
						attr = sAttrData.dwFileAttributes;
					}
				}else{
					logIoerr(cnt);
					if( lastErrno!=ERROR_FILE_NOT_FOUND && lastErrno!=ERROR_PATH_NOT_FOUND ){
						winLogError(SQLITE_IOERR_ACCESS, lastErrno, "winAccess", zFilename);
						sqlite3_free(zConverted);
						return SQLITE_IOERR_ACCESS;
					}else{
						attr = INVALID_FILE_ATTRIBUTES;
					}
				}
			}
#ifdef SQLITE_WIN32_HAS_ANSI
			else{
				attr = osGetFileAttributesA((char*)zConverted);
			}
#endif
			sqlite3_free(zConverted);
			switch( flags ){
			case SQLITE_ACCESS_READ:
			case SQLITE_ACCESS_EXISTS:
				rc = attr!=INVALID_FILE_ATTRIBUTES;
				break;
			case SQLITE_ACCESS_READWRITE:
				rc = attr!=INVALID_FILE_ATTRIBUTES &&
					(attr & FILE_ATTRIBUTE_READONLY)==0;
				break;
			default:
				assert(!"Invalid flags argument");
			}
			*pResOut = rc;
			return SQLITE_OK;
	}


	/*
	** Returns non-zero if the specified path name should be used verbatim.  If
	** non-zero is returned from this function, the calling function must simply
	** use the provided path name verbatim -OR- resolve it into a full path name
	** using the GetFullPathName Win32 API function (if available).
	*/
	static BOOL winIsVerbatimPathname(
		const char *zPathname
		){
			/*
			** If the path name starts with a forward slash or a backslash, it is either
			** a legal UNC name, a volume relative path, or an absolute path name in the
			** "Unix" format on Windows.  There is no easy way to differentiate between
			** the final two cases; therefore, we return the safer return value of TRUE
			** so that callers of this function will simply use it verbatim.
			*/
			if ( zPathname[0]=='/' || zPathname[0]=='\\' ){
				return TRUE;
			}

			/*
			** If the path name starts with a letter and a colon it is either a volume
			** relative path or an absolute path.  Callers of this function must not
			** attempt to treat it as a relative path name (i.e. they should simply use
			** it verbatim).
			*/
			if ( sqlite3Isalpha(zPathname[0]) && zPathname[1]==':' ){
				return TRUE;
			}

			/*
			** If we get to this point, the path name should almost certainly be a purely
			** relative one (i.e. not a UNC name, not absolute, and not volume relative).
			*/
			return FALSE;
	}

	/*
	** Turn a relative pathname into a full pathname.  Write the full
	** pathname into zOut[].  zOut[] will be at least pVfs->mxPathname
	** bytes in size.
	*/
	static int winFullPathname(
		sqlite3_vfs *pVfs,            /* Pointer to vfs object */
		const char *zRelative,        /* Possibly relative input path */
		int nFull,                    /* Size of output buffer in bytes */
		char *zFull                   /* Output buffer */
		){

#if defined(__CYGWIN__)
			SimulateIOError( return SQLITE_ERROR );
			UNUSED_PARAMETER(nFull);
			assert( pVfs->mxPathname>=MAX_PATH );
			assert( nFull>=pVfs->mxPathname );
			if ( sqlite3_data_directory && !winIsVerbatimPathname(zRelative) ){
				/*
				** NOTE: We are dealing with a relative path name and the data
				**       directory has been set.  Therefore, use it as the basis
				**       for converting the relative path name to an absolute
				**       one by prepending the data directory and a slash.
				*/
				char zOut[MAX_PATH+1];
				memset(zOut, 0, MAX_PATH+1);
				cygwin_conv_path(CCP_POSIX_TO_WIN_A|CCP_RELATIVE, zRelative, zOut,
					MAX_PATH+1);
				sqlite3_snprintf(MIN(nFull, pVfs->mxPathname), zFull, "%s\\%s",
					sqlite3_data_directory, zOut);
			}else{
				cygwin_conv_path(CCP_POSIX_TO_WIN_A, zRelative, zFull, nFull);
			}
			return SQLITE_OK;
#endif

#if (SQLITE_OS_WINCE || SQLITE_OS_WINRT) && !defined(__CYGWIN__)
			SimulateIOError( return SQLITE_ERROR );
			/* WinCE has no concept of a relative pathname, or so I am told. */
			/* WinRT has no way to convert a relative path to an absolute one. */
			if ( sqlite3_data_directory && !winIsVerbatimPathname(zRelative) ){
				/*
				** NOTE: We are dealing with a relative path name and the data
				**       directory has been set.  Therefore, use it as the basis
				**       for converting the relative path name to an absolute
				**       one by prepending the data directory and a backslash.
				*/
				sqlite3_snprintf(MIN(nFull, pVfs->mxPathname), zFull, "%s\\%s",
					sqlite3_data_directory, zRelative);
			}else{
				sqlite3_snprintf(MIN(nFull, pVfs->mxPathname), zFull, "%s", zRelative);
			}
			return SQLITE_OK;
#endif

#if !SQLITE_OS_WINCE && !SQLITE_OS_WINRT && !defined(__CYGWIN__)
			DWORD nByte;
			void *zConverted;
			char *zOut;

			/* If this path name begins with "/X:", where "X" is any alphabetic
			** character, discard the initial "/" from the pathname.
			*/
			if( zRelative[0]=='/' && sqlite3Isalpha(zRelative[1]) && zRelative[2]==':' ){
				zRelative++;
			}

			/* It's odd to simulate an io-error here, but really this is just
			** using the io-error infrastructure to test that SQLite handles this
			** function failing. This function could fail if, for example, the
			** current working directory has been unlinked.
			*/
			SimulateIOError( return SQLITE_ERROR );
			if ( sqlite3_data_directory && !winIsVerbatimPathname(zRelative) ){
				/*
				** NOTE: We are dealing with a relative path name and the data
				**       directory has been set.  Therefore, use it as the basis
				**       for converting the relative path name to an absolute
				**       one by prepending the data directory and a backslash.
				*/
				sqlite3_snprintf(MIN(nFull, pVfs->mxPathname), zFull, "%s\\%s",
					sqlite3_data_directory, zRelative);
				return SQLITE_OK;
			}
			zConverted = convertUtf8Filename(zRelative);
			if( zConverted==0 ){
				return SQLITE_IOERR_NOMEM;
			}
			if( isNT() ){
				LPWSTR zTemp;
				nByte = osGetFullPathNameW((LPCWSTR)zConverted, 0, 0, 0);
				if( nByte==0 ){
					winLogError(SQLITE_ERROR, osGetLastError(),
						"GetFullPathNameW1", zConverted);
					sqlite3_free(zConverted);
					return SQLITE_CANTOPEN_FULLPATH;
				}
				nByte += 3;
				zTemp = sqlite3MallocZero( nByte*sizeof(zTemp[0]) );
				if( zTemp==0 ){
					sqlite3_free(zConverted);
					return SQLITE_IOERR_NOMEM;
				}
				nByte = osGetFullPathNameW((LPCWSTR)zConverted, nByte, zTemp, 0);
				if( nByte==0 ){
					winLogError(SQLITE_ERROR, osGetLastError(),
						"GetFullPathNameW2", zConverted);
					sqlite3_free(zConverted);
					sqlite3_free(zTemp);
					return SQLITE_CANTOPEN_FULLPATH;
				}
				sqlite3_free(zConverted);
				zOut = unicodeToUtf8(zTemp);
				sqlite3_free(zTemp);
			}
#ifdef SQLITE_WIN32_HAS_ANSI
			else{
				char *zTemp;
				nByte = osGetFullPathNameA((char*)zConverted, 0, 0, 0);
				if( nByte==0 ){
					winLogError(SQLITE_ERROR, osGetLastError(),
						"GetFullPathNameA1", zConverted);
					sqlite3_free(zConverted);
					return SQLITE_CANTOPEN_FULLPATH;
				}
				nByte += 3;
				zTemp = sqlite3MallocZero( nByte*sizeof(zTemp[0]) );
				if( zTemp==0 ){
					sqlite3_free(zConverted);
					return SQLITE_IOERR_NOMEM;
				}
				nByte = osGetFullPathNameA((char*)zConverted, nByte, zTemp, 0);
				if( nByte==0 ){
					winLogError(SQLITE_ERROR, osGetLastError(),
						"GetFullPathNameA2", zConverted);
					sqlite3_free(zConverted);
					sqlite3_free(zTemp);
					return SQLITE_CANTOPEN_FULLPATH;
				}
				sqlite3_free(zConverted);
				zOut = sqlite3_win32_mbcs_to_utf8(zTemp);
				sqlite3_free(zTemp);
			}
#endif
			if( zOut ){
				sqlite3_snprintf(MIN(nFull, pVfs->mxPathname), zFull, "%s", zOut);
				sqlite3_free(zOut);
				return SQLITE_OK;
			}else{
				return SQLITE_IOERR_NOMEM;
			}
#endif
	}

#ifndef SQLITE_OMIT_LOAD_EXTENSION
	/*
	** Interfaces for opening a shared library, finding entry points
	** within the shared library, and closing the shared library.
	*/
	/*
	** Interfaces for opening a shared library, finding entry points
	** within the shared library, and closing the shared library.
	*/
	static void *winDlOpen(sqlite3_vfs *pVfs, const char *zFilename){
		HANDLE h;
		void *zConverted = convertUtf8Filename(zFilename);
		UNUSED_PARAMETER(pVfs);
		if( zConverted==0 ){
			return 0;
		}
		if( isNT() ){
#if SQLITE_OS_WINRT
			h = osLoadPackagedLibrary((LPCWSTR)zConverted, 0);
#else
			h = osLoadLibraryW((LPCWSTR)zConverted);
#endif
		}
#ifdef SQLITE_WIN32_HAS_ANSI
		else{
			h = osLoadLibraryA((char*)zConverted);
		}
#endif
		sqlite3_free(zConverted);
		return (void*)h;
	}
	static void winDlError(sqlite3_vfs *pVfs, int nBuf, char *zBufOut){
		UNUSED_PARAMETER(pVfs);
		getLastErrorMsg(osGetLastError(), nBuf, zBufOut);
	}
	static void (*winDlSym(sqlite3_vfs *pVfs,void *pH,const char *zSym))(void){
		UNUSED_PARAMETER(pVfs);
		return (void(*)(void))osGetProcAddressA((HANDLE)pH, zSym);
	}
	static void winDlClose(sqlite3_vfs *pVfs, void *pHandle){
		UNUSED_PARAMETER(pVfs);
		osFreeLibrary((HANDLE)pHandle);
	}
#else /* if SQLITE_OMIT_LOAD_EXTENSION is defined: */
#define winDlOpen  0
#define winDlError 0
#define winDlSym   0
#define winDlClose 0
#endif


	/*
	** Write up to nBuf bytes of randomness into zBuf.
	*/
	static int winRandomness(sqlite3_vfs *pVfs, int nBuf, char *zBuf){
		int n = 0;
		UNUSED_PARAMETER(pVfs);
#if defined(SQLITE_TEST)
		n = nBuf;
		memset(zBuf, 0, nBuf);
#else
		if( sizeof(SYSTEMTIME)<=nBuf-n ){
			SYSTEMTIME x;
			osGetSystemTime(&x);
			memcpy(&zBuf[n], &x, sizeof(x));
			n += sizeof(x);
		}
		if( sizeof(DWORD)<=nBuf-n ){
			DWORD pid = osGetCurrentProcessId();
			memcpy(&zBuf[n], &pid, sizeof(pid));
			n += sizeof(pid);
		}
#if SQLITE_OS_WINRT
		if( sizeof(ULONGLONG)<=nBuf-n ){
			ULONGLONG cnt = osGetTickCount64();
			memcpy(&zBuf[n], &cnt, sizeof(cnt));
			n += sizeof(cnt);
		}
#else
		if( sizeof(DWORD)<=nBuf-n ){
			DWORD cnt = osGetTickCount();
			memcpy(&zBuf[n], &cnt, sizeof(cnt));
			n += sizeof(cnt);
		}
#endif
		if( sizeof(LARGE_INTEGER)<=nBuf-n ){
			LARGE_INTEGER i;
			osQueryPerformanceCounter(&i);
			memcpy(&zBuf[n], &i, sizeof(i));
			n += sizeof(i);
		}
#endif
		return n;
	}


	/*
	** Sleep for a little while.  Return the amount of time slept.
	*/
	static int winSleep(sqlite3_vfs *pVfs, int microsec){
		sqlite3_win32_sleep((microsec+999)/1000);
		UNUSED_PARAMETER(pVfs);
		return ((microsec+999)/1000)*1000;
	}

	/*
	** The following variable, if set to a non-zero value, is interpreted as
	** the number of seconds since 1970 and is used to set the result of
	** sqlite3OsCurrentTime() during testing.
	*/
#ifdef SQLITE_TEST
	int sqlite3_current_time = 0;  /* Fake system time in seconds since 1970. */
#endif

	/*
	** Find the current time (in Universal Coordinated Time).  Write into *piNow
	** the current time and date as a Julian Day number times 86_400_000.  In
	** other words, write into *piNow the number of milliseconds since the Julian
	** epoch of noon in Greenwich on November 24, 4714 B.C according to the
	** proleptic Gregorian calendar.
	**
	** On success, return SQLITE_OK.  Return SQLITE_ERROR if the time and date 
	** cannot be found.
	*/
	static int winCurrentTimeInt64(sqlite3_vfs *pVfs, sqlite3_int64 *piNow){
		/* FILETIME structure is a 64-bit value representing the number of 
		100-nanosecond intervals since January 1, 1601 (= JD 2305813.5). 
		*/
		FILETIME ft;
		static const sqlite3_int64 winFiletimeEpoch = 23058135*(sqlite3_int64)8640000;
#ifdef SQLITE_TEST
		static const sqlite3_int64 unixEpoch = 24405875*(sqlite3_int64)8640000;
#endif
		/* 2^32 - to avoid use of LL and warnings in gcc */
		static const sqlite3_int64 max32BitValue = 
			(sqlite3_int64)2000000000 + (sqlite3_int64)2000000000 +
			(sqlite3_int64)294967296;

#if SQLITE_OS_WINCE
		SYSTEMTIME time;
		osGetSystemTime(&time);
		/* if SystemTimeToFileTime() fails, it returns zero. */
		if (!osSystemTimeToFileTime(&time,&ft)){
			return SQLITE_ERROR;
		}
#else
		osGetSystemTimeAsFileTime( &ft );
#endif

		*piNow = winFiletimeEpoch +
			((((sqlite3_int64)ft.dwHighDateTime)*max32BitValue) + 
			(sqlite3_int64)ft.dwLowDateTime)/(sqlite3_int64)10000;

#ifdef SQLITE_TEST
		if( sqlite3_current_time ){
			*piNow = 1000*(sqlite3_int64)sqlite3_current_time + unixEpoch;
		}
#endif
		UNUSED_PARAMETER(pVfs);
		return SQLITE_OK;
	}

	/*
	** Find the current time (in Universal Coordinated Time).  Write the
	** current time and date as a Julian Day number into *prNow and
	** return 0.  Return 1 if the time and date cannot be found.
	*/
	static int winCurrentTime(sqlite3_vfs *pVfs, double *prNow){
		int rc;
		sqlite3_int64 i;
		rc = winCurrentTimeInt64(pVfs, &i);
		if( !rc ){
			*prNow = i/86400000.0;
		}
		return rc;
	}

	/*
	** The idea is that this function works like a combination of
	** GetLastError() and FormatMessage() on Windows (or errno and
	** strerror_r() on Unix). After an error is returned by an OS
	** function, SQLite calls this function with zBuf pointing to
	** a buffer of nBuf bytes. The OS layer should populate the
	** buffer with a nul-terminated UTF-8 encoded error message
	** describing the last IO error to have occurred within the calling
	** thread.
	**
	** If the error message is too large for the supplied buffer,
	** it should be truncated. The return value of xGetLastError
	** is zero if the error message fits in the buffer, or non-zero
	** otherwise (if the message was truncated). If non-zero is returned,
	** then it is not necessary to include the nul-terminator character
	** in the output buffer.
	**
	** Not supplying an error message will have no adverse effect
	** on SQLite. It is fine to have an implementation that never
	** returns an error message:
	**
	**   int xGetLastError(sqlite3_vfs *pVfs, int nBuf, char *zBuf){
	**     assert(zBuf[0]=='\0');
	**     return 0;
	**   }
	**
	** However if an error message is supplied, it will be incorporated
	** by sqlite into the error message available to the user using
	** sqlite3_errmsg(), possibly making IO errors easier to debug.
	*/
	static int winGetLastError(sqlite3_vfs *pVfs, int nBuf, char *zBuf){
		UNUSED_PARAMETER(pVfs);
		return getLastErrorMsg(osGetLastError(), nBuf, zBuf);
	}

	/*
	** Initialize and deinitialize the operating system interface.
	*/
	int sqlite3_os_init(void){
		static sqlite3_vfs winVfs = {
			3,                   /* iVersion */
			sizeof(winFile),     /* szOsFile */
			MAX_PATH,            /* mxPathname */
			0,                   /* pNext */
			"win32",             /* zName */
			0,                   /* pAppData */
			winOpen,             /* xOpen */
			winDelete,           /* xDelete */
			winAccess,           /* xAccess */
			winFullPathname,     /* xFullPathname */
			winDlOpen,           /* xDlOpen */
			winDlError,          /* xDlError */
			winDlSym,            /* xDlSym */
			winDlClose,          /* xDlClose */
			winRandomness,       /* xRandomness */
			winSleep,            /* xSleep */
			winCurrentTime,      /* xCurrentTime */
			winGetLastError,     /* xGetLastError */
			winCurrentTimeInt64, /* xCurrentTimeInt64 */
			winSetSystemCall,    /* xSetSystemCall */
			winGetSystemCall,    /* xGetSystemCall */
			winNextSystemCall,   /* xNextSystemCall */
		};

		/* Double-check that the aSyscall[] array has been constructed
		** correctly.  See ticket [bb3a86e890c8e96ab] */
		assert( ArraySize(aSyscall)==74 );

#ifndef SQLITE_OMIT_WAL
		/* get memory map allocation granularity */
		memset(&winSysInfo, 0, sizeof(SYSTEM_INFO));
#if SQLITE_OS_WINRT
		osGetNativeSystemInfo(&winSysInfo);
#else
		osGetSystemInfo(&winSysInfo);
#endif
		assert(winSysInfo.dwAllocationGranularity > 0);
#endif

		sqlite3_vfs_register(&winVfs, 1);
		return SQLITE_OK; 
	}

	int sqlite3_os_end(void){ 
#if SQLITE_OS_WINRT
		if( sleepObj!=NULL ){
			osCloseHandle(sleepObj);
			sleepObj = NULL;
		}
#endif
		return SQLITE_OK;
	}


#pragma endregion

}}
#endif