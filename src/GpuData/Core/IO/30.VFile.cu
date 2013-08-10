#include "../Core.cu.h"

namespace Core { namespace IO
{
	RC VFile::ShmLock(int offset, int n, SHM flags) { return RC::OK; }
	void VFile::ShmBarrier() { }
	RC VFile::ShmUnmap(bool deleteFlag) { return RC::OK; }
	RC VFile::ShmMap(int region, int sizeRegion, bool isWrite, void volatile **pp) { return RC::OK; }
}}