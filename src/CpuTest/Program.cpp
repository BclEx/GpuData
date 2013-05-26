//#include "..\GpuData\Core\Core.cu.h"
#include "..\GpuData\Core+Pager\Core+Pager.cu.h"
using namespace Core;

namespace Core
{
	int Bitvec_BuiltinTest(int size, int *aOp);
}

void main()
{
	PCache::Initialize();
	PCache::ReleaseMemory(5);
	//int outA;
	//Core::Bitvec_BuiltinTest(16, &outA);
}