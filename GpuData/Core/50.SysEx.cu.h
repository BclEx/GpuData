namespace Core
{
#define SysEx_ALWAYS(X) (X)
#define SysEx_NEVER(X) (X)

	class SysEx
	{
	public:
		__device__ inline static void *Alloc(size_t size, bool clear) { return nullptr; }
		__device__ inline static void Free(void *p) { }
		__device__ inline static void *StackAlloc(size_t size) { return nullptr; }
		__device__ inline static void StackFree(void *p) { }
		//
		__device__ static void SetRandom(int n, void *buffer);
	};
}
