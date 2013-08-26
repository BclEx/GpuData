#ifndef __RUNTIME_CPU_H__
#define __RUNTIME_CPU_H__
#include <stdio.h>
#define __device__
#define __constant__
#include <assert.h>
//#include <string.h>

///////////////////////////////////////////////////////////////////////////////
// DEVICE SIDE
// External function definitions for device-side code

#define RUNTIME_UNRESTRICTED -1
extern __device__ inline void runtimeSetHeap(void *heap) { }
extern __device__ inline void runtimeRestrict(int threadid, int blockid) { }

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
#ifndef NASSERT
__device__ inline void _assert(const int condition) { if (!condition) assert(false); }
__device__ inline void _assert(const int condition, const char *fmt) { if (!condition) printf(fmt); }
#define ASSERTONLY(X) X
__device__ inline void Coverage(int line) { }
#define ASSERTCOVERAGE(X) if (X) { Coverage(__LINE__); }
#else
#define _assert(X, ...)
#define ASSERTONLY(X)
#define ASSERTCOVERAGE(X)
#endif

// Abuse of templates to simulate varargs
__device__ inline void _throw(const char *fmt) { printf(fmt); }
template <typename T1> __device__ inline void _throw(const char *fmt, T1 arg1) { printf(fmt, arg1); }
template <typename T1, typename T2> __device__ inline void _throw(const char *fmt, T1 arg1, T2 arg2) { printf(fmt, arg1, arg2); }
template <typename T1, typename T2, typename T3> __device__ inline void _throw(const char *fmt, T1 arg1, T2 arg2, T3 arg3) { printf(fmt, arg1, arg2, arg3); }
template <typename T1, typename T2, typename T3, typename T4> __device__ inline void _throw(const char *fmt, T1 arg1, T2 arg2, T3 arg3, T4 arg4) { printf(fmt, arg1, arg2, arg3, arg4); }

///////////////////////////////////////////////////////////////////////////////
// DEVICE SIDE
// External function definitions for device-side code

extern const unsigned char _runtimeUpperToLower[];
extern const unsigned char _runtimeCtypeMap[];

#define _toupperA(x) ((x)&~(_runtimeCtypeMap[(unsigned char)(x)]&0x20))
#define _isspaceA(x) (_runtimeCtypeMap[(unsigned char)(x)]&0x01)
#define _isalnumA(x) (_runtimeCtypeMap[(unsigned char)(x)]&0x06)
#define _isalphaA(x) (_runtimeCtypeMap[(unsigned char)(x)]&0x02)
#define _isdigitA(x) (_runtimeCtypeMap[(unsigned char)(x)]&0x04)
#define _isxdigitA(x) (_runtimeCtypeMap[(unsigned char)(x)]&0x08)
#define _tolowerA(x) (_runtimeUpperToLower[(unsigned char)(x)])

// array
#define __arrayAlloc(t,Ti,length) (Ti*)((int*)malloc(sizeof(Ti)*length+4)+1);*((int*)t&-1)=length
#define __arraySet(t,length) t;*((int*)&t-1)=length
#define __arrayLength(t) *((int*)&t-1)
#define __arraySetLength(t,length) *((int*)&t-1)=length
#define __arrayClear(t,length) nullptr;*((int*)&t-1)=0
#define __arrayStaticLength(symbol) (sizeof(symbol) / sizeof(symbol[0]))

// strcmp
template <typename T>
__device__ inline bool _strcmp(const T *left, const T *right)
{
	register unsigned char *a, *b;
	a = (unsigned char *)left;
	b = (unsigned char *)right;
	while (*a != 0 && _runtimeUpperToLower[*a] == _runtimeUpperToLower[*b]) { a++; b++; }
	return _runtimeUpperToLower[*a] - _runtimeUpperToLower[*b];
}

// strncmp
template <typename T>
__device__ inline bool _strncmp(const T *dest, const T *src, int n)
{
	register unsigned char *a, *b;
	a = (unsigned char *)dest;
	b = (unsigned char *)src;
	while (n-- > 0 && *a != 0 && _runtimeUpperToLower[*a] == _runtimeUpperToLower[*b]) { a++; b++; }
	return (n < 0 ? 0 : _runtimeUpperToLower[*a] - _runtimeUpperToLower[*b]);
}

// memcpy
template <typename T>
__device__ inline void _memcpy(T *dest, const T *src, size_t length)
{
	//memcpy(dest, src, length);
	register unsigned char *a, *b;
	a = (unsigned char *)dest;
	b = (unsigned char *)src;
	for (size_t i = 0; i < length; ++i, ++a, ++b)
		*a = *b;
}

// memset
template <typename T>
__device__ inline void _memset(T *dest, const char value, size_t length)
{
	register unsigned char *a;
	a = (unsigned char *)dest;
	for (size_t i = 0; i < length; ++i, ++a)
		*a = value;
}

// memcmp
template <typename T, typename Y>
__device__ inline int _memcmp(T *a, Y *b, size_t length)
{
	assert(false);
	return 0;
}

// strlen30
__device__ inline int _strlen30(const char *z)
{
	register const char *z2 = z;
	if (z == nullptr) return 0;
	while (*z2) { z2++; }
	return 0x3fffffff & (int)(z2 - z);
}

#endif // __RUNTIME_CPU_H__
