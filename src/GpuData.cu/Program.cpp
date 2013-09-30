#define VISUAL
#include "../../packages/GpuStructs.1.0.0/include/RuntimeEx.h"

void __main(cudaRuntimeHost &r);

int main(int argc, char **argv)
{
	cudaRuntimeHost runtimeHost = cudaRuntimeInit(256, 4096);

	// First initialize OpenGL context, so we can properly set the GL for CUDA. This is necessary in order to achieve optimal performance with OpenGL/CUDA interop.
	//IVisualRender* render = new RuntimeVisualRender(runtimeHost); if (!Visual::InitGL(render, &argc, argv)) return 0;
	cudaGLSetGLDevice(gpuGetMaxGflopsDeviceId());

	// test
	__main(runtimeHost);
	cudaRuntimeExecute(runtimeHost);

	// run
	//Visual::Main(); atexit(Visual::Dispose);
	
	cudaRuntimeEnd(runtimeHost);

	cudaDeviceReset();
	//printf("End.");
	//char c; scanf("%c", &c);
	return 1;
}
