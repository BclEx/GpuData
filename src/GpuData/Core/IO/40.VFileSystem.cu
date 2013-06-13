// os.c
#include "../Core.cu.h"

namespace Core { namespace IO
{
	__device__ static VFileSystem *_vfsList = nullptr;

	VFileSystem *VFileSystem::Find(const char *name)
	{
		VFileSystem *vfs = nullptr;
		MutexEx mutex = MutexEx::Alloc(MutexEx::MUTEX::STATIC_MASTER);
		MutexEx::Enter(mutex);
		for (vfs = _vfsList; vfs && _strcmp(name, vfs->Name); vfs = vfs->Next) { }
		MutexEx::Leave(mutex);
		return vfs;
	}

	__device__ static void UnlinkVfs(VFileSystem *vfs)
	{
		_assert(MutexEx::Held(MutexEx::Alloc(MutexEx::MUTEX::STATIC_MASTER)));
		if (!vfs) { }
		else if (_vfsList == vfs)
			_vfsList = vfs->Next;
		else if (_vfsList)
		{
			VFileSystem *p = _vfsList;
			while (p->Next && p->Next != vfs)
				p = p->Next;
			if (p->Next == vfs)
				p->Next = vfs->Next;
		}
	}

	int VFileSystem::RegisterVfs(VFileSystem *vfs, bool _default)
	{
		MutexEx mutex = MutexEx::Alloc(MutexEx::MUTEX::STATIC_MASTER);
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

	int VFileSystem::UnregisterVfs(VFileSystem *vfs)
	{
		MutexEx mutex = MutexEx::Alloc(MutexEx::MUTEX::STATIC_MASTER);
		MutexEx::Enter(mutex);
		UnlinkVfs(vfs);
		MutexEx::Leave(mutex);
		return RC::OK;
	}
}}