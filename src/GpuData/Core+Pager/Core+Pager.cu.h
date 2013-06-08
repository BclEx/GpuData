#include "../Core/Core.cu.h"

#define Pid uint32

#pragma region IBackup

class IBackup
{
public:
	virtual void Update(Pid id, byte data[]);
	virtual void Restart();
};

#pragma endregion

#include "Pager.cu.h"
#include "PCache.cu.h"
#include "Wal.cu.h"
