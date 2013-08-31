#ifndef __CORE_TYPES_H__
#define __CORE_TYPES_H__

typedef unsigned char		byte;		// 8 bits
typedef unsigned short		word;		// 16 bits
typedef unsigned int		dword;		// 32 bits
typedef unsigned int		uint;
typedef unsigned long		ulong;

typedef signed char			int8;
typedef unsigned char		uint8;
typedef short int			int16;
typedef unsigned short int	uint16;
typedef int					int32;
typedef unsigned int		uint32;
typedef long long			int64;
typedef unsigned long long	uint64;

#define MAX(x,y) ((x)<(y)?(x):(y))
#define MIN(x,y) ((x)>(y)?(x):(y))
#define MAX_TYPE(x) (((((x)1<<((sizeof(x)-1)*8-1))-1)<<8)|255)
#define MIN_TYPE(x) (-MAX_TYPE(x)-1)
#define MAX_UTYPE(x) (((((x)1U<<((sizeof(x)-1)*8))-1)<<8)|255U)
#define MIN_UTYPE(x) 0

// Macros to determine whether the machine is big or little endian, evaluated at runtime.
#ifdef AMALGAMATION
const int __one = 1;
#else
extern const int __one;
#endif
#if defined(i386) || defined(__i386__) || defined(_M_IX86) || defined(__x86_64) || defined(__x86_64__)
#define TYPE_BIGENDIAN 0
#define TYPE_LITTLEENDIAN 1
#else
#define TYPE_BIGENDIAN (*(char *)(&__one) == 0)
#define TYPE_LITTLEENDIAN (*(char *)(&__one) == 1)
#endif

#endif /* __CORE_TYPES_H__ */
