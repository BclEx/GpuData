//#include "..\GpuData\Core\Core.cu.h"
#include "..\GpuData.net\Core+Pager\Core+Pager.cu.h"
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
	auto rc = vfs->Open("C:\\T_\\Test.db", file, (VSystem::OPEN)((int)VSystem::OPEN_CREATE | (int)VSystem::OPEN_READWRITE | (int)VSystem::OPEN_MAIN_DB), nullptr);
	file->Write4(0, 123145);
	file->Close();
}

static int Busyhandler(void *x) { printf("BUSY"); return -1; }

static Pager *Open(VSystem *vfs)
{
	byte dbHeader[100]; // Database header content

	auto flags = (IPager::PAGEROPEN)0;
	auto vfsFlags = (VSystem::OPEN)((int)VSystem::OPEN_CREATE | (int)VSystem::OPEN_READWRITE | (int)VSystem::OPEN_MAIN_DB);
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
	Pager::Write(p);
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
