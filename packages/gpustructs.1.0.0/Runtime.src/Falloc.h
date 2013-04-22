#ifndef __FALLOC_H__
#define __FALLOC_H__
#ifndef nullptr
#define nullptr NULL
#endif
#include <cuda_runtime.h>

/*
*	This is the header file supporting cuFalloc.cu and defining both the host and device-side interfaces. See that file for some more
*	explanation and sample use code. See also below for details of the host-side interfaces.
*
*  Quick sample code:
*
#include "cuFalloc.cu"

__global__ void TestFalloc(fallocHeap* heap)
{
// create/free heap
void* obj = fallocGetChunk(heap);
fallocFreeChunk(heap, obj);

// create/free alloc
fallocContext* ctx = fallocCreateCtx(heap);
char* testString = (char*)falloc(ctx, 10);
int* testInteger = falloc<int>(ctx);
fallocDisposeCtx(ctx);
}

int main()
{
cudaFallocHost fallocHost = cudaFallocInit(1);

// test
TestFalloc<<<1, 1>>>(fallocHost.heap);

// free and exit
cudaFallocEnd(fallocHost);
printf("\ndone.\n"); // char c; scanf("%c", &c);
return 0;
}
*/

///////////////////////////////////////////////////////////////////////////////
// HOST SIDE
// External function definitions for host-side code

typedef struct
{
	void *reserved;
	void *heap;
	size_t blockSize;
	size_t blocksLength;
	size_t length;
} cudaFallocHost;

//
//	cudaFallocInit
//
//	Call this to initialize a falloc heap. If the buffer size needs to be changed, call cudaFallocEnd()
//	before re-calling cudaFallocInit().
//
//	The default size for the buffer is 1 megabyte. The buffer is filled linearly and
//	is completely used.
//
//	Arguments:
//		length - Length, in bytes, of total space to reserve (in device global memory) for output.
//
//	Returns:
//		cudaFallocHost if all is well.
//
// default 2k blocks, 1-meg heap
extern "C" cudaFallocHost cudaFallocInit(size_t blockSize = 2046, size_t length = 1048576, cudaError_t *error = nullptr, void *reserved = nullptr);

//
//	cudaFallocEnd
//
//	Cleans up all memories allocated by cudaFallocInit() for a heap.
//	Call this at exit, or before calling cudaFallocInit() again.
//
extern "C" void cudaFallocEnd(cudaFallocHost &host);


///////////////////////////////////////////////////////////////////////////////
// VISUAL
// Visual render for host-side code
#ifdef VISUAL
#include "Visual.h"

class FallocVisualRender : public IVisualRender
{
private:
	cudaFallocHost _fallocHost;
public:
	FallocVisualRender(cudaFallocHost fallocHost)
		: _fallocHost(fallocHost) { }
	virtual void Dispose();
	virtual void Keyboard(unsigned char key);
	virtual void Display();
	virtual void Initialize();
};

#endif

#endif // __FALLOC_H__