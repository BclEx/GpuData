// os.c
#include "Core.cu.h"
using namespace Core::IO;
namespace Core
{
	__device__ static VSystem *_vfsList = nullptr;

	__device__ VSystem *VSystem::Find(const char *name)
	{
		VSystem *vfs = nullptr;
		MutexEx mutex = MutexEx::Alloc(MutexEx::MUTEX_STATIC_MASTER);
		MutexEx::Enter(mutex);
		for (vfs = _vfsList; vfs && _strcmp(name, vfs->Name); vfs = vfs->Next) { }
		MutexEx::Leave(mutex);
		return vfs;
	}

	__device__ static void UnlinkVfs(VSystem *vfs)
	{
		_assert(MutexEx::Held(MutexEx::Alloc(MutexEx::MUTEX_STATIC_MASTER)));
		if (!vfs) { }
		else if (_vfsList == vfs)
			_vfsList = vfs->Next;
		else if (_vfsList)
		{
			VSystem *p = _vfsList;
			while (p->Next && p->Next != vfs)
				p = p->Next;
			if (p->Next == vfs)
				p->Next = vfs->Next;
		}
	}

	__device__ int VSystem::RegisterVfs(VSystem *vfs, bool _default)
	{
		MutexEx mutex = MutexEx::Alloc(MutexEx::MUTEX_STATIC_MASTER);
		MutexEx::Enter(mutex);
		UnlinkVfs(vfs);
		if (_default || !_vfsList)
		{
			vfs->Next = _vfsList;
			_vfsList = vfs;
		}
		else
		{
			vfs->Next = _vfsList->Next;
			_vfsList->Next = vfs;
		}
		_assert(_vfsList != nullptr);
		MutexEx::Leave(mutex);
		return RC::OK;
	}

	__device__ int VSystem::UnregisterVfs(VSystem *vfs)
	{
		MutexEx mutex = MutexEx::Alloc(MutexEx::MUTEX_STATIC_MASTER);
		MutexEx::Enter(mutex);
		UnlinkVfs(vfs);
		MutexEx::Leave(mutex);
		return RC::OK;
	}
}