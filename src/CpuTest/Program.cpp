//#include "..\GpuData\Core\Core.cu.h"
#include "..\GpuData\Core+Pager\Core+Pager.cu.h"
using namespace Core;

namespace Core
{
	int Bitvec_BuiltinTest(int size, int *ops);
}

void main()
{
	PCache::Initialize();
	PCache::ReleaseMemory(5);
	int ops[] = { 5, 1, 1, 1, 0 };
	Core::Bitvec_BuiltinTest(400, ops);
}