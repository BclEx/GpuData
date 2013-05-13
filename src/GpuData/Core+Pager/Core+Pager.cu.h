#include "../Core/Core.cu.h"

#define Pid uint32
struct IPage
{
	void *Buffer;	// The content of the page
	void *Extra;	// Extra information associated with the page
};

typedef struct Pager Pager;

#include "Pager.cu.h"
#include "PCache.cu.h"
#include "PCache1.cu.h"
