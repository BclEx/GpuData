#include "../Core/Core.cu.h"

#define Pid uint32

#pragma region IPage

struct IPage
{
	void *Buffer;	// The content of the page
	void *Extra;	// Extra information associated with the page
};

#pragma endregion

typedef struct Pager Pager;

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
#include "PCache1.cu.h"
