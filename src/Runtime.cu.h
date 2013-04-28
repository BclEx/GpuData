#if 0
#include "..\packages\gpustructs.1.0.0\Runtime.src\Cuda.h"
#include "..\packages\gpustructs.1.0.0\Runtime.src\Runtime.cu.h"
#else
#include <stdio.h>
#define __device__

__device__ inline void _assert(const bool condition)
{
	if (!condition)
		printf("assert");
}

// strcmp
template <typename T>
__device__ inline bool _strcmp(const T *dest, const T *src)
{
	return false;
}

// Memcpy
template <typename T>
__device__ inline void _memcpy(T *dest, const T *src, size_t length)
{
	char *dest2 = (char *)dest;
	char *src2 = (char *)src;
	for (size_t i = 0; i < length; ++i, ++src2, ++dest2)
		*dest2 = *src2;
}

// Memset
template <typename T>
__device__ inline void _memset(T *dest, const char value, size_t length)
{
	char *dest2 = (char *)dest;
	for (size_t i = 0; i < length; ++i, ++dest2)
		*dest2 = value;
}

#endif