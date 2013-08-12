#include "../Core.cu.h"

namespace Core { namespace IO
{

	RC VFile::Lock(LOCK lock) { return RC::OK; }
	RC VFile::Unlock(LOCK lock) { return RC::OK; }
	RC VFile::CheckReservedLock(int &lock) { return RC::OK; }
	RC VFile::FileControl(FCNTL op, void *arg) { return RC::NOTFOUND; }

	uint VFile::get_SectorSize() { return 0; }
	VFile::IOCAP VFile::get_DeviceCharacteristics() { return (VFile::IOCAP)0; }

	RC VFile::ShmLock(int offset, int n, SHM flags) { return RC::OK; }
	void VFile::ShmBarrier() { }
	RC VFile::ShmUnmap(bool deleteFlag) { return RC::OK; }
	RC VFile::ShmMap(int region, int sizeRegion, bool isWrite, void volatile **pp) { return RC::OK; }
}}