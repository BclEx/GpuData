//bitvec.c
#include "Core.cu.h"

namespace Core
{
	enum Test
	{
	};

	struct Wal
	{
		__device__ inline static int Open(int a) { return 5; }
		__device__ inline void None() { }
	};

	extern "C" __device__ long long Test()
	{
		return MAX_TYPE(long long);
		//return Wal::Open(5);
	}

	//__global__ void GO()
	//{
	//}
}
