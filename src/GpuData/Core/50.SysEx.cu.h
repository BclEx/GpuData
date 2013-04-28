// os.h
namespace Core
{
#include <malloc.h>
	class SysEx
	{
	public:
		__device__ inline static void *Alloc(size_t size) { return (char *)malloc(size); }
		__device__ inline static void *Alloc(size_t size, bool clear) { char *b = (char *)malloc(size); if (clear) _memset(b, 0, size); return b; }
		__device__ inline static void Free(void *p) { free(p);}
		__device__ inline static void *StackAlloc(size_t size) { return alloca(size); }
		__device__ inline static void StackFree(void *p) { }
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
