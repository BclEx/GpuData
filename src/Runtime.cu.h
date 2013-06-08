#if __CUDACC__
#include "..\packages\gpustructs.1.0.0\Runtime.src\Cuda.h"
#include "..\packages\gpustructs.1.0.0\Runtime.src\Runtime.cu.h"
#else
#include "..\packages\gpustructs.1.0.0\Runtime.src\Runtime.cpu.h"
#endif
