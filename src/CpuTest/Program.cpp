//#include "..\GpuData\Core\Core.cu.h"
#include "..\GpuData\Core+Pager\Core+Pager.cu.h"
#include <stdio.h>
#include <string.h>
using namespace Core;
using namespace Core::IO;

namespace Core
{
	int Bitvec_BuiltinTest(int size, int *ops);
}

static void TestVFS();
static void TestPager();

void main()
{
	SysEx::Initialize();
	//TestVFS();
	TestPager();
}

static void TestVFS()
{
	auto vfs = VSystem::Find("win32");
	auto file = (VFile *)SysEx::Alloc(vfs->SizeOsFile);
	auto rc = vfs->Open("C:\\T_\\Test.db", file, VSystem::OPEN::CREATE | VSystem::OPEN::OREADWRITE | VSystem::OPEN::MAIN_DB, nullptr);
	file->Write4(0, 123145);
	file->Close();
}

static int Busyhandler(void *x) { printf("BUSY"); return -1; }

static Pager *Open(VSystem *vfs)
{
	byte dbHeader[100]; // Database header content

	IPager::PAGEROPEN flags = (IPager::PAGEROPEN)0;
	VSystem::OPEN vfsFlags = VSystem::OPEN::CREATE | VSystem::OPEN::OREADWRITE | VSystem::OPEN::MAIN_DB;
	//
	Pager *pager;
	auto rc = Pager::Open(vfs, &pager, "C:\\T_\\Test.db", 0, flags, vfsFlags, nullptr);
	if (rc == RC::OK)
		rc = pager->ReadFileheader(sizeof(dbHeader), dbHeader);
	if (rc != RC::OK)
		goto _out;
	pager->SetBusyhandler(Busyhandler, nullptr);
	auto readOnly = pager->get_Readonly();
	//
	int reserves;
	auto pageSize = (uint)((dbHeader[16] << 8) | (dbHeader[17] << 16));
	if (pageSize < 512 || pageSize > MAX_PAGE_SIZE || ((pageSize - 1) & pageSize) != 0)
	{
		pageSize = 0;
		reserves = 0;
	}
	else
		reserves = dbHeader[20];
	rc = pager->SetPageSize(&pageSize, reserves);
	if (rc) goto _out;
_out:
	if (rc != RC::OK)
	{
		if (pager)
			pager->Close();
		pager = nullptr;
	}
	else
		pager->SetCacheSize(2000);
	return pager;
}

static void TestPager()
{
	auto vfs = VSystem::Find("win32");
	auto pager = Open(vfs);
	if (pager == nullptr)
		throw;
	auto rc = pager->SharedLock();
	if (rc != RC::OK)
		throw;
	//
	IPage *p = nullptr;
	rc = pager->Acquire(1, &p, false);
	if (rc != RC::OK)
		throw;
	rc = pager->Begin(0, false);
	if (rc != RC::OK)
		throw;
	char values[] = { 0, 1, 2, 3, 4, 5, 6, 7, 8, 9 };
	memcpy(values, p->Data, 10);
	pager->Write(p);
	pager->CommitPhaseOne(nullptr, false);
	pager->CommitPhaseTwo();
	//
	if (pager != nullptr)
		pager->Close();
}

void TestBitvec()
{
	int ops[] = { 5, 1, 1, 1, 0 };
	Core::Bitvec_BuiltinTest(400, ops);
}

#pragma region Runtime

const unsigned char _runtimeUpperToLower[] = {
	0,  1,  2,  3,  4,  5,  6,  7,  8,  9, 10, 11, 12, 13, 14, 15, 16, 17,
	18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32, 33, 34, 35,
	36, 37, 38, 39, 40, 41, 42, 43, 44, 45, 46, 47, 48, 49, 50, 51, 52, 53,
	54, 55, 56, 57, 58, 59, 60, 61, 62, 63, 64, 97, 98, 99,100,101,102,103,
	104,105,106,107,108,109,110,111,112,113,114,115,116,117,118,119,120,121,
	122, 91, 92, 93, 94, 95, 96, 97, 98, 99,100,101,102,103,104,105,106,107,
	108,109,110,111,112,113,114,115,116,117,118,119,120,121,122,123,124,125,
	126,127,128,129,130,131,132,133,134,135,136,137,138,139,140,141,142,143,
	144,145,146,147,148,149,150,151,152,153,154,155,156,157,158,159,160,161,
	162,163,164,165,166,167,168,169,170,171,172,173,174,175,176,177,178,179,
	180,181,182,183,184,185,186,187,188,189,190,191,192,193,194,195,196,197,
	198,199,200,201,202,203,204,205,206,207,208,209,210,211,212,213,214,215,
	216,217,218,219,220,221,222,223,224,225,226,227,228,229,230,231,232,233,
	234,235,236,237,238,239,240,241,242,243,244,245,246,247,248,249,250,251,
	252,253,254,255
};

const unsigned char _runtimeCtypeMap[256] = {
	0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,  /* 00..07    ........ */
	0x00, 0x01, 0x01, 0x01, 0x01, 0x01, 0x00, 0x00,  /* 08..0f    ........ */
	0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,  /* 10..17    ........ */
	0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,  /* 18..1f    ........ */
	0x01, 0x00, 0x00, 0x00, 0x40, 0x00, 0x00, 0x00,  /* 20..27     !"#$%&' */
	0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,  /* 28..2f    ()*+,-./ */
	0x0c, 0x0c, 0x0c, 0x0c, 0x0c, 0x0c, 0x0c, 0x0c,  /* 30..37    01234567 */
	0x0c, 0x0c, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,  /* 38..3f    89:;<=>? */

	0x00, 0x0a, 0x0a, 0x0a, 0x0a, 0x0a, 0x0a, 0x02,  /* 40..47    @ABCDEFG */
	0x02, 0x02, 0x02, 0x02, 0x02, 0x02, 0x02, 0x02,  /* 48..4f    HIJKLMNO */
	0x02, 0x02, 0x02, 0x02, 0x02, 0x02, 0x02, 0x02,  /* 50..57    PQRSTUVW */
	0x02, 0x02, 0x02, 0x00, 0x00, 0x00, 0x00, 0x40,  /* 58..5f    XYZ[\]^_ */
	0x00, 0x2a, 0x2a, 0x2a, 0x2a, 0x2a, 0x2a, 0x22,  /* 60..67    `abcdefg */
	0x22, 0x22, 0x22, 0x22, 0x22, 0x22, 0x22, 0x22,  /* 68..6f    hijklmno */
	0x22, 0x22, 0x22, 0x22, 0x22, 0x22, 0x22, 0x22,  /* 70..77    pqrstuvw */
	0x22, 0x22, 0x22, 0x00, 0x00, 0x00, 0x00, 0x00,  /* 78..7f    xyz{|}~. */

	0x40, 0x40, 0x40, 0x40, 0x40, 0x40, 0x40, 0x40,  /* 80..87    ........ */
	0x40, 0x40, 0x40, 0x40, 0x40, 0x40, 0x40, 0x40,  /* 88..8f    ........ */
	0x40, 0x40, 0x40, 0x40, 0x40, 0x40, 0x40, 0x40,  /* 90..97    ........ */
	0x40, 0x40, 0x40, 0x40, 0x40, 0x40, 0x40, 0x40,  /* 98..9f    ........ */
	0x40, 0x40, 0x40, 0x40, 0x40, 0x40, 0x40, 0x40,  /* a0..a7    ........ */
	0x40, 0x40, 0x40, 0x40, 0x40, 0x40, 0x40, 0x40,  /* a8..af    ........ */
	0x40, 0x40, 0x40, 0x40, 0x40, 0x40, 0x40, 0x40,  /* b0..b7    ........ */
	0x40, 0x40, 0x40, 0x40, 0x40, 0x40, 0x40, 0x40,  /* b8..bf    ........ */

	0x40, 0x40, 0x40, 0x40, 0x40, 0x40, 0x40, 0x40,  /* c0..c7    ........ */
	0x40, 0x40, 0x40, 0x40, 0x40, 0x40, 0x40, 0x40,  /* c8..cf    ........ */
	0x40, 0x40, 0x40, 0x40, 0x40, 0x40, 0x40, 0x40,  /* d0..d7    ........ */
	0x40, 0x40, 0x40, 0x40, 0x40, 0x40, 0x40, 0x40,  /* d8..df    ........ */
	0x40, 0x40, 0x40, 0x40, 0x40, 0x40, 0x40, 0x40,  /* e0..e7    ........ */
	0x40, 0x40, 0x40, 0x40, 0x40, 0x40, 0x40, 0x40,  /* e8..ef    ........ */
	0x40, 0x40, 0x40, 0x40, 0x40, 0x40, 0x40, 0x40,  /* f0..f7    ........ */
	0x40, 0x40, 0x40, 0x40, 0x40, 0x40, 0x40, 0x40   /* f8..ff    ........ */
};

#pragma endregion