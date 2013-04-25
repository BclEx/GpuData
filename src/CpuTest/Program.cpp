#include "..\GpuData\Core\Core.cu.h"

namespace Core
{
	int Bitvec_BuiltinTest(int size, int *aOp);
}

void main()
{
	int outA;
	Core::Bitvec_BuiltinTest(16, &outA);
}