namespace Core
{
#include <malloc.h>
#define SysEx_ALWAYS(X) (X)
#define SysEx_NEVER(X) (X)

	class SysEx
	{
	public:
		__device__ inline static void *Alloc(size_t size, bool clear) { char *b = (char *)malloc(size); if (clear) _memset(b, 0, size); return b; }
		__device__ inline static void Free(void *p) { free(p);}
		__device__ inline static void *StackAlloc(size_t size) { return alloca(size); }
		__device__ inline static void StackFree(void *p) { }
		//
		__device__ static void SetRandom(int n, void *buffer);
	};
}
