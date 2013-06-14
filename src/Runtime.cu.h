#if __CUDACC__
#include "..\packages\gpustructs.1.0.0\Runtime.src\Cuda.h"
#include "..\packages\gpustructs.1.0.0\Runtime.src\Runtime.cu.h"
#else
#include "..\packages\gpustructs.1.0.0\Runtime.src\Runtime.cpu.h"
#endif

#if defined(__GNUC__) && 0
#define likely(X)    __builtin_expect((X),1)
#define unlikely(X)  __builtin_expect((X),0)
#else
#define likely(X) !!(X)
#define unlikely(X) !!(X)
#endif

#if defined(__PTRDIFF_TYPE__)  // This case should work for GCC
# define INT_TO_PTR(X)  ((void*)(__PTRDIFF_TYPE__)(X))
# define PTR_TO_INT(X)  ((int)(__PTRDIFF_TYPE__)(X))
#elif !defined(__GNUC__)       // Works for compilers other than LLVM
# define INT_TO_PTR(X)  ((void*)&((char*)0)[X])
# define PTR_TO_INT(X)  ((int)(((char*)X)-(char*)0))
#elif defined(HAVE_STDINT_H)   // Use this case if we have ANSI headers
# define INT_TO_PTR(X)  ((void*)(intptr_t)(X))
# define PTR_TO_INT(X)  ((int)(intptr_t)(X))
#else                          // Generates a warning - but it always works
# define INT_TO_PTR(X)  ((void*)(X))
# define PTR_TO_INT(X)  ((int)(X))
#endif