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
		__device__ inline static void MemdebugSetType(void *p, MEMTYPE memType);
		__device__ inline static int MemdebugHasType(void *p, MEMTYPE memType);
		__device__ inline static int MemdebugNoType(void *p, MEMTYPE memType);
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

}
