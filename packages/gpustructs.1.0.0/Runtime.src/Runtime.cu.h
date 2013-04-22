#ifndef __RUNTIME_CU_H__
#define __RUNTIME_CU_H__

#if __CUDA_ARCH__ == 100
#error Atomics only used with > sm_10 architecture
#elif defined(__CUDA_ARCH__) & __CUDA_ARCH__ < 200
#include "Runtime.cu"
#else

///////////////////////////////////////////////////////////////////////////////
// DEVICE SIDE
// External function definitions for device-side code

//
//	cuRuntimeRestrict
//
//	Called to restrict output to a given thread/block. Pass the constant RUNTIME_UNRESTRICTED to unrestrict output
//	for thread/block IDs. Note you can therefore allow "all printfs from block 3" or "printfs from thread 2
//	on all blocks", or "printfs only from block 1, thread 5".
//
//	Arguments:
//		threadid - Thread ID to allow printfs from
//		blockid - Block ID to allow printfs from
//
//	NOTE: Restrictions last between invocations of kernels unless cudaRuntimeInit() is called again.
//

extern __device__ void runtimeSetHeap(void *heap);

#define RUNTIME_UNRESTRICTED -1
extern "C" __device__ void runtimeRestrict(int threadid, int blockid);

// Abuse of templates to simulate varargs
extern __device__ int _printf(const char *fmt);
template <typename T1> extern __device__ int _printf(const char *fmt, T1 arg1);
template <typename T1, typename T2> extern __device__ int _printf(const char *fmt, T1 arg1, T2 arg2);
template <typename T1, typename T2, typename T3> extern __device__ int _printf(const char *fmt, T1 arg1, T2 arg2, T3 arg3);
template <typename T1, typename T2, typename T3, typename T4> extern __device__ int _printf(const char *fmt, T1 arg1, T2 arg2, T3 arg3, T4 arg4);
template <typename T1, typename T2, typename T3, typename T4, typename T5> extern __device__ int _printf(const char *fmt, T1 arg1, T2 arg2, T3 arg3, T4 arg4, T5 arg5);
template <typename T1, typename T2, typename T3, typename T4, typename T5, typename T6> extern __device__ int _printf(const char *fmt, T1 arg1, T2 arg2, T3 arg3, T4 arg4, T5 arg5, T6 arg6);
template <typename T1, typename T2, typename T3, typename T4, typename T5, typename T6, typename T7> extern __device__ int _printf(const char *fmt, T1 arg1, T2 arg2, T3 arg3, T4 arg4, T5 arg5, T6 arg6, T7 arg7);
template <typename T1, typename T2, typename T3, typename T4, typename T5, typename T6, typename T7, typename T8> extern __device__ int _printf(const char *fmt, T1 arg1, T2 arg2, T3 arg3, T4 arg4, T5 arg5, T6 arg6, T7 arg7, T8 arg8);
template <typename T1, typename T2, typename T3, typename T4, typename T5, typename T6, typename T7, typename T8, typename T9> extern __device__ int _printf(const char *fmt, T1 arg1, T2 arg2, T3 arg3, T4 arg4, T5 arg5, T6 arg6, T7 arg7, T8 arg8, T9 arg9);
template <typename T1, typename T2, typename T3, typename T4, typename T5, typename T6, typename T7, typename T8, typename T9, typename T10> extern __device__ int _printf(const char *fmt, T1 arg1, T2 arg2, T3 arg3, T4 arg4, T5 arg5, T6 arg6, T7 arg7, T8 arg8, T9 arg9, T10 arg10);

// Assert
extern __device__ void _assert(const bool condition);
extern __device__ void _assert(const bool condition, const char *fmt);

// Abuse of templates to simulate varargs
extern __device__ void _throw(const char *fmt);
template <typename T1> extern __device__ void _throw(const char *fmt, T1 arg1);
template <typename T1, typename T2> extern __device__ void _throw(const char *fmt, T1 arg1, T2 arg2);
template <typename T1, typename T2, typename T3> extern __device__ void _throw(const char *fmt, T1 arg1, T2 arg2, T3 arg3);
template <typename T1, typename T2, typename T3, typename T4> extern __device__ void _throw(const char *fmt, T1 arg1, T2 arg2, T3 arg3, T4 arg4);

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
	for (size_t i = 0; i < length; ++i, ++dest)
		*dest2 = value;
}

#endif // __CUDA_ARCH__

#endif // __RUNTIME_CU_H__
