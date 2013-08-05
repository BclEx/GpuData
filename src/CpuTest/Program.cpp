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

void main()
{
	PCache::Initialize();
	PCache::ReleaseMemory(5);
	TestVFS();
}

static void TestVFS()
{
	auto vfs = VSystem::Find(nullptr);
	VFile *file;
	auto rc = vfs->Open("Test", file, VSystem::OPEN::CREATE, nullptr);
}

static Pager *Open(VSystem *vfs)
{
//	byte dbHeader[100]; // Database header content
	Pager *pager = nullptr;
//	Pager::PAGEROPEN flags = 0;
//	VFileSystem::OPEN vfsFlags = VFileSystem::OPEN::CREATE | VFileSystem::OPEN::OREADWRITE | VFileSystem::OPEN::MAIN_DB;
//	//
//	var rc = Pager.Open(vfs, out pager, @"Test", 0, flags, vfsFlags, x => { }, null);
//	if (rc == RC.OK)
//		rc = pager.ReadFileHeader(zDbHeader.Length, zDbHeader);
//	pager.SetBusyHandler(BusyHandler, null);
//	var readOnly = pager.IsReadonly;
//	//
//	int nReserve;
//	var pageSize = (uint)((zDbHeader[16] << 8) | (zDbHeader[17] << 16));
//	if (pageSize < 512 || pageSize > Pager.SQLITE_MAX_PAGE_SIZE || ((pageSize - 1) & pageSize) != 0)
//	{
//		pageSize = 0;
//		nReserve = 0;
//	}
//	else
//		nReserve = zDbHeader[20];
//	rc = pager.SetPageSize(ref pageSize, nReserve);
//	if (rc != RC.OK)
//		goto _out;
//_out:
//	if (rc != RC.OK)
//	{
//		if (pager != null)
//			pager.Close();
//		pager = null;
//	}
//	pager.SetCacheSize(2000);
	return pager;
}

static void TestPager()
{
	auto vfs = VSystem::Find(nullptr);
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


static int BusyHandler(void *x) { printf("BUSY"); return -1; }

void TestBitvec()
{
	int ops[] = { 5, 1, 1, 1, 0 };
	Core::Bitvec_BuiltinTest(400, ops);
}