//bitvec.c
#include "..\..\Runtime.cu.h"

namespace Core
{
	struct Wal
	{
		__device__ inline static int Open(int a) { return 5; }
		__device__ inline void None() { }
	};

	__device__ int Test()
	{
		return Wal::Open(5);
	}

	//__global__ void GO()
	//{
	//}
}
