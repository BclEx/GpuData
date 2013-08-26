#ifndef __RUNTIME_CUDA_H__
#define __RUNTIME_CUDA_H__
#include <cuda_runtime.h>

extern cudaError _lastError;
#define checkCudaErrors(action, failure) if ((_lastError = action) != cudaSuccess) failure;

inline int _ConvertSMVer2Cores(int major, int minor)
{
	typedef struct // Defines for GPU Architecture types (using the SM version to determine the # of cores per SM
	{
		int SM; // 0xMm (hexidecimal notation), M = SM Major version, and m = SM minor version
		int Cores;
	} SMToCores;
	SMToCores gpuArchCoresPerSM[] = {
		{ 0x10,  8 }, // Tesla Generation (SM 1.0) G80 class
		{ 0x11,  8 }, // Tesla Generation (SM 1.1) G8x class
		{ 0x12,  8 }, // Tesla Generation (SM 1.2) G9x class
		{ 0x13,  8 }, // Tesla Generation (SM 1.3) GT200 class
		{ 0x20, 32 }, // Fermi Generation (SM 2.0) GF100 class
		{ 0x21, 48 }, // Fermi Generation (SM 2.1) GF10x class
		{ 0x30, 192}, // Kepler Generation (SM 3.0) GK10x class
		{ 0x35, 192}, // Kepler Generation (SM 3.5) GK11x class
		{   -1, -1 }
	};
	int index = 0;
	while (gpuArchCoresPerSM[index].SM != -1)
	{
		if (gpuArchCoresPerSM[index].SM == ((major << 4) + minor))
			return gpuArchCoresPerSM[index].Cores;
		index++;
	}
	//printf("MapSMtoCores SM %d.%d is undefined (please update to the latest SDK)!\n", major, minor);
	return -1;
}

inline int gpuGetMaxGflopsDeviceId()
{
	int deviceCount = 0;
	cudaDeviceProp deviceProp;
	cudaGetDeviceCount(&deviceCount);
	// Find the best major SM Architecture GPU device
	int bestMajor = 0;
	for (int i = 0; i < deviceCount; i++)
	{
		cudaGetDeviceProperties(&deviceProp, i);
		// If this GPU is not running on Compute Mode prohibited, then we can add it to the list
		if (deviceProp.computeMode != cudaComputeModeProhibited && deviceProp.major > 0 && deviceProp.major < 9999)
			bestMajor = (bestMajor > deviceProp.major ? bestMajor : deviceProp.major);
	}
	// Find the best CUDA capable GPU device
	int bestDevice = 0;
	int basePerformace = 0;
	for (int i = 0; i < deviceCount; i++ )
	{
		cudaGetDeviceProperties(&deviceProp, i);
		// If this GPU is not running on Compute Mode prohibited, then we can add it to the list
		if (deviceProp.computeMode != cudaComputeModeProhibited)
		{
			int sm_per_multiproc = (deviceProp.major == 9999 && deviceProp.minor == 9999 ? 1 : _ConvertSMVer2Cores(deviceProp.major, deviceProp.minor));
			int performace = (deviceProp.multiProcessorCount * sm_per_multiproc * deviceProp.clockRate);
			if (performace > basePerformace)
			{
				basePerformace = performace;
				bestDevice = i;
			}
		}
	}
	return bestDevice;
}

#endif // __RUNTIME_CUDA_H__