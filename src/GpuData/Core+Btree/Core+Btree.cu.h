#include "../Core+Pager/Core+Pager.cu.h"

#pragma region CollSeq

typedef struct CollSeq CollSeq;
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

enum SCHEMA_ : uint8
{
	SchemaLoaded = 0x0001, // The schema has been loaded
	UnresetViews = 0x0002, // Some views have defined column names
	Empty = 0x0004, // The file is empty (length 0 bytes)
};

typedef struct ISchema ISchema;
struct ISchema
{
	uint8 FileFormat;
	SCHEMA_ Flags;
};

#pragma endregion

#include "Context.cu.h"
#include "Btree.cu.h"

#pragma region IVdbe

class IVdbe
{
public:
	virtual UnpackedRecord *AllocUnpackedRecord(KeyInfo *keyInfo, char *space, int spaceLength, char **free);
	virtual void RecordUnpack(KeyInfo *keyInfo, int keyLength, const void *key, UnpackedRecord *p);
	virtual void DeleteUnpackedRecord(UnpackedRecord *r);
	virtual int RecordCompare(int cells, const void *cellKey, UnpackedRecord *idxKey);
};

#pragma endregion

