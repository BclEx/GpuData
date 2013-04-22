#if __CUDA_ARCH__ == 100 
#error Atomics only used with > sm_10 architecture
#elif defined(_RTLIB) || __CUDA_ARCH__ < 200

#ifndef nullptr
#define nullptr NULL
#endif
//#define __THROW *(int*)0=0;
#include <string.h>
#include "Cuda.h"

#define RUNTIME_UNRESTRICTED -1

typedef struct __align__(8)
{
	int threadid; // RUNTIME_UNRESTRICTED for unrestricted
	int blockid;  // RUNTIME_UNRESTRICTED for unrestricted
} runtimeRestriction;

typedef struct __align__(8)
{
	volatile char *blockPtr; // current atomically-incremented non-wrapped offset
	void *reserved;
	runtimeRestriction restriction;
	size_t blockSize;
	size_t blocksLength; // size of circular buffer (set up by host)
	char *blocks; // start of circular buffer (set up by host)
} runtimeHeap;

typedef struct __align__(8)
{
	unsigned short magic;		// magic number says we're valid
	unsigned short type;		// type of block
	unsigned short fmtoffset;	// offset of fmt string into buffer
	unsigned short blockid;		// block ID of author
	unsigned short threadid;	// thread ID of author
} runtimeBlockHeader;

__device__ runtimeHeap *__runtimeHeap;
#if defined(_RTLIB) || __CUDA_ARCH__ < 200
// inlined
#define __static__ static
__device__ __static__ void runtimeSetHeap(void *heap) { __runtimeHeap = (runtimeHeap *)heap; }
extern "C" void cudaRuntimeSetHeap(void *heap) { }
#else
#define __static__
__device__ __static__ void runtimeSetHeap(void *heap) { }
extern "C" void cudaRuntimeSetHeap(void *heap) { cudaMemcpyToSymbol(__runtimeHeap, &heap, sizeof(void *)); }
#endif

///////////////////////////////////////////////////////////////////////////////
// HEAP
#pragma region HEAP

#define RUNTIME_MAGIC (unsigned short)0xC811
#define RUNTIME_ALIGNSIZE sizeof(long long)
#define RUNTIMETYPE_PRINTF 1
#define RUNTIMETYPE_ASSERT 2
#define RUNTIMETYPE_THROW 2

__device__ static char *moveNextPtr()
{
	if (!__runtimeHeap) __THROW;
	// thread/block restriction check
	runtimeRestriction restriction = __runtimeHeap->restriction;
	if (restriction.blockid != RUNTIME_UNRESTRICTED && restriction.blockid != (blockIdx.x + gridDim.x*blockIdx.y))
		return nullptr;
	if (restriction.threadid != RUNTIME_UNRESTRICTED && restriction.threadid != (threadIdx.x + blockDim.x*threadIdx.y + blockDim.x*blockDim.y*threadIdx.z))
		return nullptr;
	// advance pointer
	char *blocks = __runtimeHeap->blocks;
	size_t offset = atomicAdd((unsigned int *)&__runtimeHeap->blockPtr, __runtimeHeap->blockSize) - (size_t)blocks;
	offset %= __runtimeHeap->blocksLength;
	return blocks + offset;
}

extern "C" __device__ __static__ void runtimeRestrict(int threadid, int blockid)
{
	int threadMax = blockDim.x * blockDim.y * blockDim.z;
	if ((threadid < threadMax && threadid >= 0) || threadid == RUNTIME_UNRESTRICTED)
		__runtimeHeap->restriction.threadid = threadid;
	int blockMax = gridDim.x * gridDim.y;
	if ((blockid < blockMax && blockid >= 0) || blockid == RUNTIME_UNRESTRICTED)
		__runtimeHeap->restriction.blockid = blockid;
}

__inline__ __device__ static void writeBlockHeader(unsigned short type, char *ptr, char *fmtptr)
{
	runtimeBlockHeader header;
	header.magic = RUNTIME_MAGIC;
	header.type = type;
	header.fmtoffset = (unsigned short)(fmtptr - ptr);
	header.blockid = gridDim.x*blockIdx.y + blockIdx.x;
	header.threadid = blockDim.x*blockDim.y*threadIdx.z + blockDim.x*threadIdx.y + threadIdx.x;
	*(runtimeBlockHeader *)(void *)ptr = header;
}

__device__ static char *writeString(char *dest, const char *src, int maxLength, char *end)
{
	// initialization and overflow check
	if (!dest || dest >= end) //|| !src)
		return nullptr;
	// prepare to write the length specifier. We're guaranteed to have at least "RUNTIME_ALIGNSIZE" bytes left because we only write out in
	// chunks that size, and blockSize is aligned with RUNTIME_ALIGNSIZE.
	int *lenptr = (int *)(void *)dest;
	int len = 0;
	dest += RUNTIME_ALIGNSIZE;
	// now copy the string
	while (maxLength--)
	{
		if (dest >= end) // overflow check
			break;
		len++;
		*dest++ = *src;
		if (*src++ == '\0')
			break;
	}
	// now write out the padding bytes, and we have our length.
	while (dest < end && ((long)dest & (RUNTIME_ALIGNSIZE - 1)) != 0)
	{
		len++;
		*dest++ = 0;
	}
	*lenptr = len;
	return (dest < end ? dest : nullptr); // overflow means return nullptr
}

__device__ static char *copyArg(char *ptr, const char *arg, char *end)
{
	// initialization check
	if (!ptr) // || !arg)
		return nullptr;
	// strncpy does all our work. We just terminate.
	if ((ptr = writeString(ptr, arg, __runtimeHeap->blockSize, end)) != nullptr)
		*ptr = 0;
	return ptr;
}

template <typename T>
__device__ static char *copyArg(char *ptr, T &arg, char *end)
{
	// initialization and overflow check. Alignment rules mean that we're at least CUPRINTF_ALIGN_SIZE away from "end", so we only need to check that one offset.
	if (!ptr || (ptr + RUNTIME_ALIGNSIZE) >= end)
		return nullptr;
	// write the length and argument
	*(int *)(void *)ptr = sizeof(arg);
	ptr += RUNTIME_ALIGNSIZE;
	*(T *)(void *)ptr = arg;
	ptr += RUNTIME_ALIGNSIZE;
	*ptr = 0;
	return ptr;
}

#pragma endregion


//////////////////////
// PRINTF
#pragma region PRINTF

#define PRINTF_PREAMBLE \
	char *start, *end, *bufptr, *fmtstart; \
	if ((start = moveNextPtr()) == nullptr) return 0; \
	end = start + __runtimeHeap->blockSize; \
	bufptr = start + sizeof(runtimeBlockHeader);
#define PRINTF_ARG(argname) \
	bufptr = copyArg(bufptr, argname, end);
#define PRINTF_POSTAMBLE \
	fmtstart = bufptr; \
	end = writeString(bufptr, fmt, __runtimeHeap->blockSize, end); \
	writeBlockHeader(RUNTIMETYPE_PRINTF, start, (end ? fmtstart : nullptr)); \
	return (end ? (int)(end - start) : 0);

__device__ __static__ int _printf(const char *fmt)
{
	PRINTF_PREAMBLE;
	PRINTF_POSTAMBLE;
}
template <typename T1> __device__ __static__ int _printf(const char *fmt, T1 arg1)
{
	PRINTF_PREAMBLE;
	PRINTF_ARG(arg1);
	PRINTF_POSTAMBLE;
}
template <typename T1, typename T2> __device__ __static__ int _printf(const char *fmt, T1 arg1, T2 arg2)
{
	PRINTF_PREAMBLE;
	PRINTF_ARG(arg1);
	PRINTF_ARG(arg2);
	PRINTF_POSTAMBLE;
}
template <typename T1, typename T2, typename T3> __device__ __static__ int _printf(const char *fmt, T1 arg1, T2 arg2, T3 arg3)
{
	PRINTF_PREAMBLE;
	PRINTF_ARG(arg1);
	PRINTF_ARG(arg2);
	PRINTF_ARG(arg3);
	PRINTF_POSTAMBLE;
}
template <typename T1, typename T2, typename T3, typename T4> __device__ __static__ int _printf(const char *fmt, T1 arg1, T2 arg2, T3 arg3, T4 arg4)
{
	PRINTF_PREAMBLE;
	PRINTF_ARG(arg1);
	PRINTF_ARG(arg2);
	PRINTF_ARG(arg3);
	PRINTF_ARG(arg4);
	PRINTF_POSTAMBLE;
}
template <typename T1, typename T2, typename T3, typename T4, typename T5> __device__ __static__ int _printf(const char *fmt, T1 arg1, T2 arg2, T3 arg3, T4 arg4, T5 arg5)
{
	PRINTF_PREAMBLE;
	PRINTF_ARG(arg1);
	PRINTF_ARG(arg2);
	PRINTF_ARG(arg3);
	PRINTF_ARG(arg4);
	PRINTF_ARG(arg5);
	PRINTF_POSTAMBLE;
}
template <typename T1, typename T2, typename T3, typename T4, typename T5, typename T6> __device__ __static__ int _printf(const char *fmt, T1 arg1, T2 arg2, T3 arg3, T4 arg4, T5 arg5, T6 arg6)
{
	PRINTF_PREAMBLE;
	PRINTF_ARG(arg1);
	PRINTF_ARG(arg2);
	PRINTF_ARG(arg3);
	PRINTF_ARG(arg4);
	PRINTF_ARG(arg5);
	PRINTF_ARG(arg6);
	PRINTF_POSTAMBLE;
}
template <typename T1, typename T2, typename T3, typename T4, typename T5, typename T6, typename T7> __device__ __static__ int _printf(const char *fmt, T1 arg1, T2 arg2, T3 arg3, T4 arg4, T5 arg5, T6 arg6, T7 arg7)
{
	PRINTF_PREAMBLE;
	PRINTF_ARG(arg1);
	PRINTF_ARG(arg2);
	PRINTF_ARG(arg3);
	PRINTF_ARG(arg4);
	PRINTF_ARG(arg5);
	PRINTF_ARG(arg6);
	PRINTF_ARG(arg7);
	PRINTF_POSTAMBLE;
}
template <typename T1, typename T2, typename T3, typename T4, typename T5, typename T6, typename T7, typename T8> __device__ __static__ int _printf(const char *fmt, T1 arg1, T2 arg2, T3 arg3, T4 arg4, T5 arg5, T6 arg6, T7 arg7, T8 arg8)
{
	PRINTF_PREAMBLE;
	PRINTF_ARG(arg1);
	PRINTF_ARG(arg2);
	PRINTF_ARG(arg3);
	PRINTF_ARG(arg4);
	PRINTF_ARG(arg5);
	PRINTF_ARG(arg6);
	PRINTF_ARG(arg7);
	PRINTF_ARG(arg8);
	PRINTF_POSTAMBLE;
}
template <typename T1, typename T2, typename T3, typename T4, typename T5, typename T6, typename T7, typename T8, typename T9> __device__ __static__ int __printf(const char *fmt, T1 arg1, T2 arg2, T3 arg3, T4 arg4, T5 arg5, T6 arg6, T7 arg7, T8 arg8, T9 arg9)
{
	PRINTF_PREAMBLE;
	PRINTF_ARG(arg1);
	PRINTF_ARG(arg2);
	PRINTF_ARG(arg3);
	PRINTF_ARG(arg4);
	PRINTF_ARG(arg5);
	PRINTF_ARG(arg6);
	PRINTF_ARG(arg7);
	PRINTF_ARG(arg8);
	PRINTF_ARG(arg9);
	PRINTF_POSTAMBLE;
}
template <typename T1, typename T2, typename T3, typename T4, typename T5, typename T6, typename T7, typename T8, typename T9, typename T10> __device__ __static__ int _printf(const char *fmt, T1 arg1, T2 arg2, T3 arg3, T4 arg4, T5 arg5, T6 arg6, T7 arg7, T8 arg8, T9 arg9, T10 arg10)
{
	PRINTF_PREAMBLE;
	PRINTF_ARG(arg1);
	PRINTF_ARG(arg2);
	PRINTF_ARG(arg3);
	PRINTF_ARG(arg4);
	PRINTF_ARG(arg5);
	PRINTF_ARG(arg6);
	PRINTF_ARG(arg7);
	PRINTF_ARG(arg8);
	PRINTF_ARG(arg9);
	PRINTF_ARG(arg10);
	PRINTF_POSTAMBLE;
}

#undef PRINTF_PREAMBLE
#undef PRINTF_ARG
#undef PRINTF_POSTAMBLE

#pragma endregion


//////////////////////
// ASSERT
#pragma region ASSERT

#define ASSERT_PREAMBLE \
	char *start, *end, *bufptr, *fmtstart; \
	if ((start = moveNextPtr()) == nullptr) return; \
	end = start + __runtimeHeap->blockSize; \
	bufptr = start + sizeof(runtimeBlockHeader);
#define ASSERT_ARG(argname) \
	bufptr = copyArg(bufptr, argname, end);
#define ASSERT_POSTAMBLE \
	fmtstart = bufptr; \
	end = writeString(bufptr, fmt, __runtimeHeap->blockSize, end); \
	writeBlockHeader(RUNTIMETYPE_ASSERT, start, (end ? fmtstart : nullptr));

__device__ __static__ void _assert(const bool condition)
{
	const char *fmt = nullptr;
	if (condition)
	{
		ASSERT_PREAMBLE;
		ASSERT_POSTAMBLE;
	}
}
__device__ __static__ void _assert(const bool condition, const char *fmt)
{
	if (condition)
	{
		ASSERT_PREAMBLE;
		ASSERT_POSTAMBLE;
	}
}

#undef ASSERT_PREAMBLE
#undef ASSERT_ARG
#undef ASSERT_POSTAMBLE

#pragma endregion


//////////////////////
// THROW
#pragma region THROW

#define THROW_PREAMBLE \
	char *start, *end, *bufptr, *fmtstart; \
	if ((start = moveNextPtr()) == nullptr) return; \
	end = start + __runtimeHeap->blockSize; \
	bufptr = start + sizeof(runtimeBlockHeader);
#define THROW_ARG(argname) \
	bufptr = copyArg(bufptr, argname, end);
#define THROW_POSTAMBLE \
	fmtstart = bufptr; \
	end = writeString(bufptr, fmt, __runtimeHeap->blockSize, end); \
	writeBlockHeader(RUNTIMETYPE_THROW, start, (end ? fmtstart : nullptr)); \
	__THROW;

__device__ __static__ void _throw(const char *fmt)
{
	THROW_PREAMBLE;
	THROW_POSTAMBLE;
}
template <typename T1> __device__ __static__ void _throw(const char *fmt, T1 arg1)
{
	THROW_PREAMBLE;
	THROW_ARG(arg1);
	THROW_POSTAMBLE;
}
template <typename T1, typename T2> __device__ __static__ void _throw(const char *fmt, T1 arg1, T2 arg2)
{
	THROW_PREAMBLE;
	THROW_ARG(arg1);
	THROW_ARG(arg2);
	THROW_POSTAMBLE;
}
template <typename T1, typename T2, typename T3> __device__ __static__ void _throw(const char *fmt, T1 arg1, T2 arg2, T3 arg3)
{
	THROW_PREAMBLE;
	THROW_ARG(arg1);
	THROW_ARG(arg2);
	THROW_ARG(arg3);
	THROW_POSTAMBLE;
}
template <typename T1, typename T2, typename T3, typename T4> __device__ __static__ void _throw(const char *fmt, T1 arg1, T2 arg2, T3 arg3, T4 arg4)
{
	THROW_PREAMBLE;
	THROW_ARG(arg1);
	THROW_ARG(arg2);
	THROW_ARG(arg3);
	THROW_ARG(arg4);
	THROW_POSTAMBLE;
}

#undef THROW_PREAMBLE
#undef THROW_ARG
#undef THROW_POSTAMBLE

#pragma endregion


///////////////////////////////////////////////////////////////////////////////
// VISUAL
#pragma region VISUAL
#ifdef VISUAL
#include "Runtime.h"

#define MAX(a,b) (a > b ? a : b)
#define BLOCKPITCH 64
#define HEADERPITCH 4
#define BLOCKREFCOLOR make_float4(1, 0, 0, 1)
#define HEADERCOLOR make_float4(0, 1, 0, 1)
#define BLOCKCOLOR make_float4(0, 0, .7, 1)
#define BLOCK2COLOR make_float4(0, 0, 1, 1)
#define MARKERCOLOR make_float4(1, 1, 0, 1)

__global__ static void RenderHeap(quad4 *b, runtimeHeap *heap, unsigned int offset)
{
	int index = offset;
	// heap
	b[index] = make_quad4(
		make_float4(00, 1, 1, 1), HEADERCOLOR,
		make_float4(10, 1, 1, 1), HEADERCOLOR,
		make_float4(10, 0, 1, 1), HEADERCOLOR,
		make_float4(00, 0, 1, 1), HEADERCOLOR);
	// free
	float x1, y1;
	if (heap->blockPtr)
	{
		size_t offset = ((char *)heap->blockPtr - (char *)heap->blocks);
		offset %= heap->blocksLength;
		offset /= heap->blockSize;
		//
		unsigned int x = offset % BLOCKPITCH;
		unsigned int y = offset / BLOCKPITCH;
		x1 = x * 10; y1 = y * 20 + 2;
		b[index + 1] = make_quad4(
			make_float4(x1 + 0, y1 + 1, 1, 1), MARKERCOLOR,
			make_float4(x1 + 1, y1 + 1, 1, 1), MARKERCOLOR,
			make_float4(x1 + 1, y1 + 0, 1, 1), MARKERCOLOR,
			make_float4(x1 + 0, y1 + 0, 1, 1), MARKERCOLOR);
	}
}

__global__ static void RenderBlock(quad4 *b, size_t blocks, unsigned int blocksY, runtimeHeap *heap, unsigned int offset)
{
	unsigned int x = blockIdx.x * blockDim.x + threadIdx.x;
	unsigned int y = blockIdx.y * blockDim.y + threadIdx.y;
	int blockIndex = y * BLOCKPITCH + x;
	if (blockIndex >= blocks)
		return;
	runtimeBlockHeader *hdr = (runtimeBlockHeader *)(heap->blocks + blockIndex * heap->blockSize);
	int index = blockIndex * 2 + offset;
	// block
	float x2 = x * 10; float y2 = y * 20 + 2;
	if (hdr->magic != RUNTIME_MAGIC || hdr->fmtoffset >= heap->blockSize)
	{
		b[index] = make_quad4(
			make_float4(x2 + 0, y2 + 19, 1, 1), BLOCKCOLOR,
			make_float4(x2 + 9, y2 + 19, 1, 1), BLOCKCOLOR,
			make_float4(x2 + 9, y2 + 00, 1, 1), BLOCKCOLOR,
			make_float4(x2 + 0, y2 + 00, 1, 1), BLOCKCOLOR);
	}
	else
	{
		b[index] = make_quad4(
			make_float4(x2 + 0, y2 + 1, 1, 1), HEADERCOLOR,
			make_float4(x2 + 3.9, y2 + 1, 1, 1), HEADERCOLOR,
			make_float4(x2 + 3.9, y2 + 0, 1, 1), HEADERCOLOR,
			make_float4(x2 + 0, y2 + 0, 1, 1), HEADERCOLOR);
		b[index + 1] = make_quad4(
			make_float4(x2 + 0, y2 + 19, 1, 1), BLOCK2COLOR,
			make_float4(x2 + 9, y2 + 19, 1, 1), BLOCK2COLOR,
			make_float4(x2 + 9, y2 + 00, 1, 1), BLOCK2COLOR,
			make_float4(x2 + 0, y2 + 00, 1, 1), BLOCK2COLOR);
	}
}

__global__ static void Keypress(runtimeHeap *heap, unsigned char key)
{
	runtimeSetHeap(heap);
	switch (key)
	{
	case 'a':
		_printf("Test\n");
		break;
	case 'b':
		_printf("Test %d\n", threadIdx.x);
		break;
	case 'c':
		_assert(true, "Error");
		break;
	case 'd':
		_assert(false, "Error");
		break;
	}
}

static size_t GetRuntimeRenderQuads(size_t blocks)
{ 
	return 2 + (blocks * 2);
}

static void LaunchRuntimeRender(float4 *b, size_t blocks, runtimeHeap *heap)
{
	cudaRuntimeSetHeap(heap);
	dim3 heapBlock(1, 1, 1);
	dim3 heapGrid(1, 1, 1);
	RenderHeap<<<heapGrid, heapBlock>>>((quad4 *)b, heap, 0);
	//
	dim3 blockBlock(16, 16, 1);
	dim3 blockGrid((unsigned int)MAX(BLOCKPITCH / 16, 1), (unsigned int)MAX(blocks / BLOCKPITCH / 16, 1), 1);
	RenderBlock<<<blockGrid, blockBlock>>>((quad4 *)b, blocks, (unsigned int)blocks / BLOCKPITCH, heap, 2);
}

static void LaunchRuntimeKeypress(cudaRuntimeHost &host, unsigned char key)
{
	if (key == 'z')
	{
		cudaRuntimeExecute(host);
		return;
	}
	cudaRuntimeSetHeap(host.heap);
	dim3 heapBlock(1, 1, 1);
	dim3 heapGrid(1, 1, 1);
	Keypress<<<heapGrid, heapBlock>>>((runtimeHeap *)host.heap, key);
}

// _vbo variables
static GLuint _runtimeVbo;
static GLsizei _runtimeVboSize;
static struct cudaGraphicsResource *_runtimeVboResource;

static void RuntimeRunCuda(size_t blocks, runtimeHeap *heap, struct cudaGraphicsResource **resource)
{
	// map OpenGL buffer object for writing from CUDA
	checkCudaErrors(cudaGraphicsMapResources(1, resource, nullptr), exit(0));
	float4 *b;
	size_t size;
	checkCudaErrors(cudaGraphicsResourceGetMappedPointer((void **)&b, &size, *resource), exit(0));
	//printf("CUDA mapped VBO: May access %ld bytes\n", size);
	LaunchRuntimeRender(b, blocks, heap);
	// unmap buffer object
	checkCudaErrors(cudaGraphicsUnmapResources(1, resource, nullptr), exit(0));
}

static void RuntimeCreateVBO(size_t blocks, GLuint *vbo, struct cudaGraphicsResource **resource, unsigned int vbo_res_flags)
{
	// create buffer object
	glGenBuffers(1, vbo);
	glBindBuffer(GL_ARRAY_BUFFER, *vbo);
	// initialize buffer object
	_runtimeVboSize = (GLsizei)GetRuntimeRenderQuads(blocks) * 4;
	unsigned int size = _runtimeVboSize * 2 * sizeof(float4);
	glBufferData(GL_ARRAY_BUFFER, size, 0, GL_DYNAMIC_DRAW);
	glBindBuffer(GL_ARRAY_BUFFER, 0);
	// register this buffer object with CUDA
	checkCudaErrors(cudaGraphicsGLRegisterBuffer(resource, *vbo, vbo_res_flags), exit(0));
	SDK_CHECK_ERROR_GL();
}

static void RuntimeDeleteVBO(GLuint *vbo, struct cudaGraphicsResource *resource)
{
	// unregister this buffer object with CUDA
	cudaGraphicsUnregisterResource(resource);
	glBindBuffer(1, *vbo);
	glDeleteBuffers(1, vbo);
	*vbo = 0;
}

void RuntimeVisualRender::Dispose()
{
	if (_runtimeVbo)
		RuntimeDeleteVBO(&_runtimeVbo, _runtimeVboResource);
}

void RuntimeVisualRender::Keyboard(unsigned char key)
{
	switch (key)
	{
	case 'a':
	case 'b':
	case 'c':
	case 'd':
	case 'z':
		LaunchRuntimeKeypress(_runtimeHost, key);
		break;
	}
}

void RuntimeVisualRender::Display()
{
	size_t blocks = _runtimeHost.blocksLength / _runtimeHost.blockSize;
	// run CUDA kernel to generate vertex positions
	RuntimeRunCuda(blocks, (runtimeHeap *)_runtimeHost.heap, &_runtimeVboResource);

	//gluLookAt(0, 0, 200, 0, 0, 0, 0, 1, 0);
	//glScalef(.02, .02, .02);
	// set view matrix
	glMatrixMode(GL_MODELVIEW);
	glLoadIdentity();
	glTranslatef(Visual::TranslateX, Visual::TranslateY, Visual::TranslateZ);
	glRotatef(Visual::RotateX, 1.0, 0.0, 0.0);
	glRotatef(Visual::RotateY, 0.0, 1.0, 0.0);

	// render from the _vbo
	glBindBuffer(GL_ARRAY_BUFFER, _runtimeVbo);
	glVertexPointer(4, GL_FLOAT, sizeof(float4) * 2, 0);
	glColorPointer(4, GL_FLOAT, sizeof(float4) * 2, (GLvoid*)sizeof(float4));

	glEnableClientState(GL_VERTEX_ARRAY);
	glEnableClientState(GL_COLOR_ARRAY);
	glDrawArrays(GL_QUADS, 0, _runtimeVboSize);
	glDisableClientState(GL_COLOR_ARRAY);
	glDisableClientState(GL_VERTEX_ARRAY);
}

void RuntimeVisualRender::Initialize()
{
	size_t blocks = _runtimeHost.blocksLength / _runtimeHost.blockSize;
	// create VBO
	RuntimeCreateVBO(blocks, &_runtimeVbo, &_runtimeVboResource, cudaGraphicsMapFlagsWriteDiscard);
	// run the cuda part
	RuntimeRunCuda(blocks, (runtimeHeap *)_runtimeHost.heap, &_runtimeVboResource);
}

#undef MAX
#undef BLOCKPITCH
#undef HEADERPITCH
#undef BLOCKREFCOLOR
#undef HEADERCOLOR
#undef BLOCKCOLOR
#undef BLOCK2COLOR
#undef MARKERCOLOR

#endif
#pragma endregion

#endif // __CUDA_ARCH__
