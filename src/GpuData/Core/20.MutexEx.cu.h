namespace Core
{
	class MutexEx 
	{
	public:
		enum MUTEX
		{
			FAST = 0,
			RECURSIVE = 1,
			STATIC_MASTER = 2,
			STATIC_MEM = 3,  // sqlite3_malloc()
			STATIC_MEM2 = 4,  // NOT USED
			STATIC_OPEN = 4,  // sqlite3BtreeOpen()
			STATIC_PRNG = 5,  // sqlite3_random()
			STATIC_LRU = 6,   // lru page list
			STATIC_LRU2 = 7,  // NOT USED
			STATIC_PMEM = 7, // sqlite3PageMalloc()
		};

		__device__ inline static MutexEx Alloc(MUTEX id)
		{ 
			MutexEx m;
			return m;
		}
		__device__ inline static void Enter(MutexEx mutex) { }
		__device__ inline static void Leave(MutexEx mutex) { }
		__device__ inline static bool Held(MutexEx mutex) { return true; }
		__device__ inline static bool NotHeld(MutexEx mutex) { return true; }
		__device__ inline static void Free(MutexEx mutex) { }
	};
}
