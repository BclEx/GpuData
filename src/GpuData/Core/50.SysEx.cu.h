// os.h
namespace Core
{
#pragma region Log & Trace

#ifdef _DEBUG
	extern bool OSTrace;
	extern bool IOTrace;
#ifndef __CUDACC__
	__device__ inline static void SysEx_LOG(RC rc, const char *fmt, ...) { }
	__device__ inline static void SysEx_OSTRACE(const char *fmt, ...) { }
	__device__ inline static void SysEx_IOTRACE(const char *fmt, ...) { }
#else
	__device__ inline static void SysEx_LOG(RC rc, const char *fmt) { }
	template <typename T1> __device__ inline static void SysEx_LOG(RC rc, const char *fmt, T1 arg1) { }
	template <typename T1, typename T2> __device__ inline static void SysEx_LOG(RC rc, const char *fmt, T1 arg1, T2 arg2) { }
	template <typename T1, typename T2, typename T3> __device__ inline static void SysEx_LOG(RC rc, const char *fmt, T1 arg1, T2 arg2, T3 arg3) { }
	template <typename T1, typename T2, typename T3, typename T4> __device__ inline static void SysEx_LOG(RC rc, const char *fmt, T1 arg1, T2 arg2, T3 arg3, T4 arg4) { }
	__device__ inline static void SysEx_OSTRACE(const char *fmt) { }
	template <typename T1> __device__ inline static void SysEx_OSTRACE(const char *fmt, T1 arg1) { }
	template <typename T1, typename T2> __device__ inline static void SysEx_OSTRACE(const char *fmt, T1 arg1, T2 arg2) { }
	template <typename T1, typename T2, typename T3> __device__ inline static void SysEx_OSTRACE(const char *fmt, T1 arg1, T2 arg2, T3 arg3) { }
	template <typename T1, typename T2, typename T3, typename T4> __device__ inline static void SysEx_OSTRACE(const char *fmt, T1 arg1, T2 arg2, T3 arg3, T4 arg4) { }
	__device__ inline static void SysEx_IOTRACE(const char *fmt) { }
	template <typename T1> __device__ inline static void SysEx_IOTRACE(const char *fmt, T1 arg1) { }
	template <typename T1, typename T2> __device__ inline static void SysEx_IOTRACE(const char *fmt, T1 arg1, T2 arg2) { }
	template <typename T1, typename T2, typename T3> __device__ inline static void SysEx_IOTRACE(const char *fmt, T1 arg1, T2 arg2, T3 arg3) { }
	template <typename T1, typename T2, typename T3, typename T4> __device__ inline static void SysEx_IOTRACE(const char *fmt, T1 arg1, T2 arg2, T3 arg3, T4 arg4) { }
#endif
#else
#define SysEx_LOG(X)
#define SysEx_OSTRACE(X)
#define SysEx_IOTRACE(X)
#endif

#pragma endregion

#define SysEx_VERSION_NUMBER 3007016

#include <malloc.h>
	class SysEx
	{
	public:
#pragma region Memory Allocation

		enum MEMTYPE : uint8
		{
			MEMTYPE_HEAP = 0x01,         // General heap allocations
			MEMTYPE_LOOKASIDE = 0x02,    // Might have been lookaside memory
			MEMTYPE_SCRATCH = 0x04,      // Scratch allocations
			MEMTYPE_PCACHE = 0x08,       // Page cache allocations
			MEMTYPE_DB = 0x10,           // Uses sqlite3DbMalloc, not sqlite_malloc
		};
		__device__ inline static void BeginBenignAlloc() { }
		__device__ inline static void EndBenignAlloc() { }
		__device__ inline static void *Alloc(size_t size) { return (char *)malloc(size); }
		__device__ inline static void *Alloc(size_t size, bool clear) { char *b = (char *)malloc(size); if (clear) _memset(b, 0, size); return b; }
		__device__ inline static void *TagAlloc(void *tag, size_t size) { return (char *)malloc(size); }
		__device__ inline static void *TagAlloc(void *tag, size_t size, bool clear) { char *b = (char *)malloc(size); if (clear) _memset(b, 0, size); return b; }
		__device__ inline static int AllocSize(void *p)
		{
			_assert(MemdebugHasType(p, MEMTYPE_HEAP));
			_assert(MemdebugNoType(p, MEMTYPE_DB));
			return 0; 
		}
		__device__ inline static int TagAllocSize(void *tag, void *p)
		{
			_assert(MemdebugHasType(p, MEMTYPE_HEAP));
			_assert(MemdebugNoType(p, MEMTYPE_DB));
			return 0; 
		}
		__device__ inline static void Free(void *p) { free(p); }
		__device__ inline static void TagFree(void *tag, void *p) { free(p); }
#ifndef __CUDACC__
		__device__ inline static void *ScratchAlloc(size_t size) { return alloca(size); }
		__device__ inline static void ScratchFree(void *p) { }
#else
		__device__ inline static void *ScratchAlloc(size_t size) { return malloc(size); }
		__device__ inline static void ScratchFree(void *p) { free(p); }
#endif
		__device__ inline static bool HeapNearlyFull() { return false; }
		__device__ inline static void *Realloc(void *old, size_t newSize) { return nullptr; }
		__device__ inline static void *TagRealloc(void *tag, void *old, size_t newSize) { return nullptr; }
		//
#if MEMDEBUG
#else
		__device__ inline static void MemdebugSetType(void *p, MEMTYPE memType) { }
		__device__ inline static bool MemdebugHasType(void *p, MEMTYPE memType) { return true; }
		__device__ inline static bool MemdebugNoType(void *p, MEMTYPE memType) { return true; }
#endif

		__device__ inline static char *TagStrDup(void *tag, const char *z)
		{
			if (z == nullptr) return nullptr;
			size_t n = _strlen30(z) + 1;
			_assert((n & 0x7fffffff) == n);
			char *newZ = (char *)TagAlloc(tag, (int)n);
			if (newZ)
				_memcpy(newZ, z, n);
			return newZ;
		}
		__device__ inline static char *TagStrNDup(void *tag,  const char *z, int n)
		{
			if (z == nullptr) return nullptr;
			_assert((n & 0x7fffffff) == n);
			char *newZ = (char *)TagAlloc(tag, n + 1);
			if (newZ)
			{
				_memcpy(newZ, z, n);
				newZ[n] = 0;
			}
			return newZ;
		}

#pragma endregion
		__device__ static RC Initialize();
		__device__ static void Shutdown();
		__device__ static void PutRandom(int length, void *buffer);
	};

#define SysEx_ALWAYS(X) (X)
#define SysEx_NEVER(X) (X)

#define SysEx_ROUND8(x)     (((x)+7)&~7)
#define SysEx_ROUNDDOWN8(x) ((x)&~7)
#ifdef BYTEALIGNED4
#define SysEx_HASALIGNMENT8(X) ((((char *)(X) - (char *)0)&3) == 0)
#else
#define SysEx_HASALIGNMENT8(X) ((((char *)(X) - (char *)0)&7) == 0)
#endif

#if _DEBUG
	__device__ inline static RC CORRUPT_BKPT_(int line)
	{
		SysEx_LOG(RC::CORRUPT, "database corruption at line %d of [%.10s]", line, "src");
		return RC::CORRUPT;
	}
	__device__ inline static RC MISUSE_BKPT_(int line)
	{
		SysEx_LOG(RC::MISUSE, "misuse at line %d of [%.10s]", line, "src");
		return RC::MISUSE;
	}
	__device__ inline static RC CANTOPEN_BKPT_(int line)
	{
		SysEx_LOG(RC::CANTOPEN, "cannot open file at line %d of [%.10s]", line, "src");
		return RC::CANTOPEN;
	}
#define SysEx_CORRUPT_BKPT CORRUPT_BKPT_(__LINE__)
#define SysEx_MISUSE_BKPT MISUSE_BKPT_(__LINE__)
#define SysEx_CANTOPEN_BKPT CANTOPEN_BKPT_(__LINE__)
#else
#define SysEx_CORRUPT_BKPT RC::CORRUPT
#define SysEx_MISUSE_BKPT RC::MISUSE
#define SysEx_CANTOPEN_BKPT RC::CANTOPEN
#endif

}
