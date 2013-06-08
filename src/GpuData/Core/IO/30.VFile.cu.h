// sqlite3.h
namespace Core
{
#define PENDING_BYTE 0x40000000

	// sqliteInt.h
	typedef struct VFileSystem VFileSystem;
	typedef struct VFile VFile;
#ifdef ENABLE_ATOMIC_WRITE
	int VFile_JournalOpen(VFileSystem *vfs, const char *a, VFile *b, int c, int d);
	int VFile_JournalSize(VFileSystem *vfs);
	int VFile_JournalCreate(VFile *v);
	bool VFile_JournalExists(VFile *v);
#else
#define VFile_JournalSize(vfs) ((vfs)->SizeOsFile)
#define VFile_JournalExists(v) true
#endif

	class VFile
	{
	public:
		enum LOCK : char
		{
			NO = 0,
			SHARED = 1,
			RESERVED = 2,
			PENDING = 3,
			EXCLUSIVE = 4,
			UNKNOWN = 5,
		};

		// sqlite3.h
		enum SYNC : char
		{
			NORMAL = 0x00002,
			FULL = 0x00003,
			DATAONLY = 0x00010,
			// wal.h
			WAL_TRANSACTIONS = 0x20,    // Sync at the end of each transaction
			WAL_MASK = 0x13,            // Mask off the SQLITE_SYNC_* values
		};

		// sqlite3.h
		enum FCNTL : uint
		{
			LOCKSTATE = 1,
			GET_LOCKPROXYFILE = 2,
			SET_LOCKPROXYFILE = 3,
			LAST_ERRNO = 4,
			SIZE_HINT = 5,
			CHUNK_SIZE = 6,
			FILE_POINTER = 7,
			SYNC_OMITTED = 8,
			WIN32_AV_RETRY = 9,
			PERSIST_WAL = 10,
			OVERWRITE = 11,
			VFSNAME = 12,
			FCNTL_POWERSAFE_OVERWRITE = 13,
			PRAGMA = 14,
			BUSYHANDLER = 15,
			TEMPFILENAME = 16,
			MMAP_SIZE = 18,
			// os.h
			DB_UNCHANGED = 0xca093fa0,
		};

		// sqlite3.h
		enum IOCAP : uint
		{
			ATOMIC = 0x00000001,
			ATOMIC512 = 0x00000002,
			ATOMIC1K = 0x00000004,
			ATOMIC2K = 0x00000008,
			ATOMIC4K = 0x00000010,
			ATOMIC8K = 0x00000020,
			ATOMIC16K = 0x00000040,
			ATOMIC32K = 0x00000080,
			ATOMIC64K = 0x00000100,
			SAFE_APPEND = 0x00000200,
			SEQUENTIAL = 0x00000400,
			UNDELETABLE_WHEN_OPEN = 0x00000800,
			IOCAP_POWERSAFE_OVERWRITE = 0x00001000,
		};

		// sqlite3.h
		enum SHM : char
		{
			SHM_UNLOCK = 1,
			SHM_LOCK = 2,
			SHM_SHARED = 4,
			SHM_EXCLUSIVE = 8,
			SHM_MAX = 8,
		};

		bool Opened;

		__device__ virtual RC Read(void *buffer, int amount, int64 offset) = 0;
		__device__ virtual RC Write(const void *buffer, int amount, int64 offset) = 0;
		__device__ virtual RC Truncate(int64 size) = 0;
		__device__ virtual RC Close() = 0;
		__device__ virtual RC Sync(int flags) = 0;
		__device__ virtual RC get_FileSize(int64 &size) = 0;

		__device__ virtual RC Lock(int lock) = 0;
		__device__ virtual RC Unlock(int lock) = 0;
		__device__ virtual RC CheckReservedLock(int lock) = 0;
		__device__ virtual RC FileControl(int op, void *arg) = 0;

		__device__ virtual int SectorSize() = 0;
		__device__ virtual int get_DeviceCharacteristics() = 0;

		__device__ virtual RC ShmLock(int offset, int n, SHM flags) = 0;
		__device__ virtual void ShmBarrier() = 0;
		__device__ virtual RC ShmUnmap(int deleteFlag) = 0;
		__device__ virtual RC ShmMap(int page, int pageSize, int extend, void volatile **p) = 0;

		__device__ inline RC Read4(int64 offset, uint32 *valueOut)
		{
			unsigned char ac[4];
			RC rc = Read(ac, sizeof(ac), offset);
			if (rc == RC::OK)
				*valueOut = ConvertEx::Get4(ac);
			return rc;
		}

		__device__ inline RC Write4(int64 offset, uint32 value)
		{
			char ac[4];
			ConvertEx::Put4((uint8 *)ac, value);
			return Write(ac, 4, offset);
		}

		__device__ inline static bool IsMemoryVFile(VFile *file)
		{
			return true;
		}

		__device__ inline static void MemoryVFileOpen(VFile *file)
		{
			_assert(SysEx_HASALIGNMENT8(file));
			_memset(file, 0, MemoryVFileSize);
		}

		static int MemoryVFileSize;  //sizeof(MemoryVFile));
	};
}
