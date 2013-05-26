#if 0
#include "..\packages\gpustructs.1.0.0\Runtime.src\Cuda.h"
#include "..\packages\gpustructs.1.0.0\Runtime.src\Runtime.cu.h"
#else
#include <stdio.h>
#define __device__

template <typename T>
__device__ inline T *__arraySet(T *symbol, int length) { return symbol; }
template <typename T>
__device__ inline T *__arrayClear(T *symbol) { return nullptr; }
#define __arrayLength(symbol) 0
#define _static_arraylength(symbol) (sizeof(symbol) / sizeof(symbol[0]))

#if !defined(DEBUG)
#define ASSERTONLY(X) X
#define ASSERTCOVERAGE(X)
#else
#define ASSERTONLY(X)
#define ASSERTCOVERAGE(X)
#endif

__device__ inline void _assert(const int condition)
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

// memcpy
template <typename T>
__device__ inline void _memcpy(T *dest, const T *src, size_t length)
{
	char *dest2 = (char *)dest;
	char *src2 = (char *)src;
	for (size_t i = 0; i < length; ++i, ++src2, ++dest2)
		*dest2 = *src2;
}

// memset
template <typename T>
__device__ inline void _memset(T *dest, const char value, size_t length)
{
	char *dest2 = (char *)dest;
	for (size_t i = 0; i < length; ++i, ++dest2)
		*dest2 = value;
}

// memcmp
template <typename T, typename Y>
__device__ inline int _memcmp(T *a, Y *b, size_t length)
{
	return 0;
}

#endif