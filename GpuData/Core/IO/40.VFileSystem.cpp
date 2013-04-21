namespace Core
{
	static VFileSystem::VFileSystem *_vfsList = nullptr;

	VFileSystem *VFileSystem::Find(const char *name)
	{
		VFileSystem *vfs = nullptr;
#if THREADSAFE
		MutexEx *mutex = MutexEx::Alloc(MutexEx::MUTEX_STATIC_MASTER);
#endif
		MutexEx::Enter(mutex);
		for (vfs = _vfsList; vfs && strcmp(vfs, vfs->Name); vfs = vfs->Next) { }
		MutexEx::Leave(mutex);
		return vfs;
	}

	void VFileSystem::UnlinkVfs(VFileSystem *vfs)
	{
		assert(MutexEx::Held(MutexEx::Alloc(MutexEx::MUTEX_STATIC_MASTER)));
		if (!vfs) { }
		else if (_vfsList == vfs)
			vfsList = pVfs->pNext;
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
		MutexEx *mutex = MutexEx::Alloc(MutexEx::MUTEX_STATIC_MASTER);
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
		assert(_vfsList);
		MutexEx::Leave(mutex);
		return RC::OK;
	}

	int VFileSystem::UnregisterVfs(VFileSystem *vfs)
	{
#if THREADSAFE
		MutexEx *mutex = MutexEx::Alloc(MutexEx::MUTEX_STATIC_MASTER);
#endif
MutexEx:Enter(mutex);
		UnlinkVfs(vfs);
		MutexEx::Leave(mutex);
		return RC::OK;
	}
}
