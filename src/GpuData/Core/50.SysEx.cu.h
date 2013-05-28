// os.h
namespace Core
{
#include <malloc.h>
	class SysEx
	{
	public:
		enum MEMTYPE : uint8
		{
			HEAP =      0x01,  // General heap allocations
			LOOKASIDE = 0x02,  // Might have been lookaside memory
			SCRATCH =   0x04,  // Scratch allocations
			PCACHE =    0x08,  // Page cache allocations
			DB =        0x10,  // Uses sqlite3DbMalloc, not sqlite_malloc
		};
		__device__ inline static void BeginBenignAlloc() { }
		__device__ inline static void EndBenignAlloc() { }
		__device__ inline static void *Alloc(size_t size) { return (char *)malloc(size); }
		__device__ inline static void *Alloc(size_t size, bool clear) { char *b = (char *)malloc(size); if (clear) _memset(b, 0, size); return b; }
		__device__ inline static int AllocSize(void *p)
		{
			_assert(MemdebugHasType(p, MEMTYPE::HEAP));
			_assert(MemdebugNoType(p, MEMTYPE::DB));
			return 0; 
		}
		__device__ inline static void Free(void *p) { free(p);}
		__device__ inline static void *StackAlloc(size_t size) { return alloca(size); }
		__device__ inline static void StackFree(void *p) { }
		__device__ inline static bool HeapNearlyFull() { return false; }
		//
#if MEMDEBUG
#else
		__device__ inline static void MemdebugSetType(void *p, MEMTYPE memType) { }
		__device__ inline static bool MemdebugHasType(void *p, MEMTYPE memType) { return true; }
		__device__ inline static bool MemdebugNoType(void *p, MEMTYPE memType) { return true; }
#endif
		//
		__device__ static void SetRandom(int n, void *buffer);
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
		//sqlite3_log(RC::CORRUPT, "database corruption at line %d of [%.10s]", line, "");
		return RC::CORRUPT;
	}
	__device__ inline static RC MISUSE_BKPT_(int line)
	{
		//sqlite3_log(RC::MISUSE, "misuse at line %d of [%.10s]", line, "");
		return RC::MISUSE;
	}
	__device__ inline static RC CANTOPEN_BKPT_(int line)
	{
		//sqlite3_log(RC::CANTOPEN, "cannot open file at line %d of [%.10s]", line, "");
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

#ifdef _DEBUG
	extern bool OSTrace;
	__device__ inline static void SysEx_OSTRACE(const char *, ...) { }
	extern bool IOTrace;
	__device__ inline static void SysEx_IOTRACE(const char *, ...) { }
#else
#define SysEx_OSTRACE(X)
#define SysEx_IOTRACE(X)
#endif

}
