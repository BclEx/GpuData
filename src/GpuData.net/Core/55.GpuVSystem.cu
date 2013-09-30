// os_win.c
#define OS_GPU 1
#if OS_GPU
#include "Core.cu.h"
#include <new.h>

namespace Core
{
#pragma region Preamble

#if defined(TEST) || defined(_DEBUG)
	bool OsTrace = true;
#define OSTRACE(X, ...) if (OsTrace) { printf(X, __VA_ARGS__); }
#else
#define OSTRACE(X, ...)
#endif

#pragma endregion

#pragma region GpuVSystem

	class GpuVSystem : public VSystem
	{
	public:
		__device__ GpuVSystem() { }
		__device__ virtual RC Open(const char *path, VFile *file, OPEN flags, OPEN *outFlags);
		__device__ virtual RC Delete(const char *path, bool syncDirectory);
		__device__ virtual RC Access(const char *path, ACCESS flags, int *outRC);
		__device__ virtual RC FullPathname(const char *path, int pathOutLength, char *pathOut);

		__device__ virtual void *DlOpen(const char *filename);
		__device__ virtual void DlError(int bufLength, char *buf);
		__device__ virtual void (*DlSym(void *handle, const char *symbol))();
		__device__ virtual void DlClose(void *handle);

		__device__ virtual int Randomness(int bufLength, char *buf);
		__device__ virtual int Sleep(int microseconds);
		__device__ virtual RC CurrentTimeInt64(int64 *now);
		__device__ virtual RC CurrentTime(double *now);
		__device__ virtual RC GetLastError(int bufLength, char *buf);

		__device__ virtual RC SetSystemCall(const char *name, syscall_ptr newFunc);
		__device__ virtual syscall_ptr GetSystemCall(const char *name);
		__device__ virtual const char *NextSystemCall(const char *name);
	};

#pragma endregion

#pragma region GpuVSystem

	__device__ RC GpuVSystem::Open(const char *name, VFile *id, OPEN flags, OPEN *outFlags)
	{
		return RC::ERROR;
	}

	__device__ RC GpuVSystem::Delete(const char *filename, bool syncDir)
	{
		return RC::ERROR;
	}

	__device__ RC GpuVSystem::Access(const char *filename, ACCESS flags, int *resOut)
	{
		return RC::ERROR;
	}

	__device__ RC GpuVSystem::FullPathname(const char *relative, int fullLength, char *full)
	{
		return RC::ERROR;
	}

#ifndef OMIT_LOAD_EXTENSION
	__device__ void *GpuVSystem::DlOpen(const char *filename)
	{
		return nullptr;
	}

	__device__ void GpuVSystem::DlError(int bufLength, char *buf)
	{
	}

	__device__ void (*GpuVSystem::DlSym(void *handle, const char *symbol))()
	{
		return nullptr;
	}

	__device__ void GpuVSystem::DlClose(void *handle)
	{
	}
#else
#define winDlOpen  0
#define winDlError 0
#define winDlSym   0
#define winDlClose 0
#endif

	__device__ int GpuVSystem::Randomness(int bufLength, char *buf)
	{
		return 0;
	}

	__device__ int GpuVSystem::Sleep(int microseconds)
	{
		return 0;
	}

	__device__ RC GpuVSystem::CurrentTimeInt64(int64 *now)
	{
		return RC::ERROR;
	}

	__device__ RC GpuVSystem::CurrentTime(double *now)
	{
		return RC::ERROR;
	}

	__device__ RC GpuVSystem::GetLastError(int bufLength, char *buf)
	{
		return RC::ERROR;
	}


	__device__ RC GpuVSystem::SetSystemCall(const char *name, syscall_ptr newFunc)
	{
		return RC::ERROR;
	}
	__device__ syscall_ptr GpuVSystem::GetSystemCall(const char *name)
	{
		return nullptr;
	}
	__device__ const char *GpuVSystem::NextSystemCall(const char *name)
	{
		return nullptr;
	}

	__device__ static char _gpuVfsBuf[sizeof(GpuVSystem)];
	__device__ static GpuVSystem *_gpuVfs;
	__device__ RC VSystem::Initialize()
	{
		_gpuVfs = new (_gpuVfsBuf) GpuVSystem();
		_gpuVfs->SizeOsFile = 0;
		_gpuVfs->MaxPathname = 260;
		_gpuVfs->Name = "gpu";
		RegisterVfs(_gpuVfs, true);
		return RC::OK; 
	}

	__device__ void VSystem::Shutdown()
	{ 
	}

#pragma endregion

}
#endif