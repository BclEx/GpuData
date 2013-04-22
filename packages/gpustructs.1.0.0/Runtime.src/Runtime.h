#ifndef __RUNTIME_H__
#define __RUNTIME_H__
#ifndef nullptr
#define nullptr NULL
#endif
#include <cuda_runtime.h>

///////////////////////////////////////////////////////////////////////////////
// HOST SIDE
// External function definitions for host-side code

typedef void (*cudaAssertHandler)();

typedef struct
{
	void *reserved;
	void *heap;
	char *blocks;
	char *blockStart;
	size_t blockSize;
	size_t blocksLength;
	size_t length;
	cudaAssertHandler assertHandler;
} cudaRuntimeHost;

//
//	cudaRuntimeSetHeap
//
extern "C" void cudaRuntimeSetHeap(void *heap);

//
//	cudaRuntimeInit
//
//	Call this to initialize a runtime heap. If the buffer size needs to be changed, call cudaRuntimeEnd()
//	before re-calling cudaRuntimeInit().
//
//	The default size for the buffer is 1 megabyte. The buffer is filled linearly and
//	is completely used.
//
//	Arguments:
//		length - Length, in bytes, of total space to reserve (in device global memory) for output.
//
// default 2k blocks, 1-meg heap
extern "C" cudaRuntimeHost cudaRuntimeInit(size_t blockSize = 256, size_t length = 1048576, cudaError_t* error = nullptr, void* reserved = nullptr);

//
//	cudaRuntimeEnd
//
//	Cleans up all memory allocated by cudaRuntimeInit() for a heap.
//	Call this at exit, or before calling cudaRuntimeInit() again.
//
extern "C" void cudaRuntimeEnd(cudaRuntimeHost &host);

//	cudaRuntimeSetHandler
//
//	Sets runtime handler for assert
//
extern "C" void cudaRuntimeSetHandler(cudaRuntimeHost &host, cudaAssertHandler handler);

//
//	cudaRuntimeExecute
//
//	Dumps all printfs in the output buffer to the specified file pointer. If the output pointer is not specified,
//	the default "stdout" is used.
//
//	Arguments:
//		output       - A file pointer to an output stream.
//		showThreadID - If "true", output strings are prefixed by "[blockid, threadid] " at output.
//
//	Returns:
//		cudaSuccess if all is well.
//
extern "C" cudaError_t cudaRuntimeExecute(cudaRuntimeHost &host, void *stream = nullptr, bool showThreadID = true);

///////////////////////////////////////////////////////////////////////////////
// VISUAL
// Visual render for host-side code
#ifdef VISUAL
#include "Visual.h"

class RuntimeVisualRender : public IVisualRender
{
private:
	cudaRuntimeHost _runtimeHost;
public:
	RuntimeVisualRender(cudaRuntimeHost runtimeHost)
		: _runtimeHost(runtimeHost) { }
	virtual void Dispose();
	virtual void Keyboard(unsigned char key);
	virtual void Display();
	virtual void Initialize();
};

#endif

#endif // __RUNTIME_H__
