#include "../Core+Pager/Core+Pager.cu.h"

#pragma region IVdbe

class IVdbe
{
public:
	virtual Btree::UnpackedRecord RecordUnpack(Btree::KeyInfo *keyInfo, int keyLength, uint8 *key, Btree::UnpackedRecord *space, int count);
	virtual void DeleteUnpackedRecord(Btree::UnpackedRecord *r);
	virtual RC RecordCompare(int cells, uint8 *cellKey, Btree::UnpackedRecord idxKey);
};

#pragma endregion

#pragma region CollSeq

struct CollSeq
{
	char *Name;				// Name of the collating sequence, UTF-8 encoded
	uint8 Enc;				// Text encoding handled by xCmp()
	void *User;				// First argument to xCmp()
	int (*Cmp)(void *, int, const void *, int, const void *);
	void (*Del)(void *);	// Destructor for pUser
};

#pragma endregion

#pragma region ISchema

enum SCHEMA : uint8
{
	SchemaLoaded = 0x0001, // The schema has been loaded
	UnresetViews = 0x0002, // Some views have defined column names
	Empty = 0x0004, // The file is empty (length 0 bytes)
};

struct ISchema
{
	uint8 FileFormat;
	SCHEMA Flags;
};

#pragma endregion

#include "Context.cu.h"
#include "Btree.cu.h"