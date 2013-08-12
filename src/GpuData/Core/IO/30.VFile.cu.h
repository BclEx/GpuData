// sqlite3.h
namespace Core { namespace IO
{
#define PENDING_BYTE 0x40000000
#define RESERVED_BYTE (PENDING_BYTE+1)
#define SHARED_FIRST (PENDING_BYTE+2)
#define SHARED_SIZE 510

	// sqliteInt.h
	typedef struct VSystem VSystem;
	typedef struct VFile VFile;

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
			POWERSAFE_OVERWRITE = 13,
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

		char Type;
		bool Opened;

		__device__ virtual RC Read(void *buffer, int amount, int64 offset) = 0;
		__device__ virtual RC Write(const void *buffer, int amount, int64 offset) = 0;
		__device__ virtual RC Truncate(int64 size) = 0;
		__device__ virtual RC Close() = 0;
		__device__ virtual RC Sync(int flags) = 0;
		__device__ virtual RC get_FileSize(int64 &size) = 0;

		__device__ virtual RC Lock(LOCK lock);
		__device__ virtual RC Unlock(LOCK lock);
		__device__ virtual RC CheckReservedLock(int &lock);
		__device__ virtual RC FileControl(FCNTL op, void *arg);

		__device__ virtual uint get_SectorSize();
		__device__ virtual IOCAP get_DeviceCharacteristics();

		__device__ virtual RC ShmLock(int offset, int n, SHM flags);
		__device__ virtual void ShmBarrier();
		__device__ virtual RC ShmUnmap(bool deleteFlag);
		__device__ virtual RC ShmMap(int region, int sizeRegion, bool isWrite, void volatile **pp);

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

		// extensions
#ifdef ENABLE_ATOMIC_WRITE
		__device__ static RC JournalVFileOpen(VSystem *vfs, const char *name, VFile *file, VSystem::OPEN flags, int bufferLength);
		__device__ static int JournalVFileSize(VSystem *vfs);
		__device__ static RC JournalVFileCreate(VFile *file);
		__device__ static bool HasJournalVFile(VFile *file);
#else
		__device__ inline static int JournalVFileSize(VSystem *vfs) { return vfs->SizeOsFile; }
		__device__ inline bool HasJournalVFile(VFile *file) { return true; }
		//#define JournalSize(vfs) ((vfs)->SizeOsFile)
		//#define HasJournal(file) true
#endif
		__device__ static void MemoryVFileOpen(VFile *file);
		__device__ static bool HasMemoryVFile(VFile *file);
		__device__ static int MemoryVFileSize() ;
	};

	VFile::SYNC inline operator |= (VFile::SYNC a, VFile::SYNC b) { return (VFile::SYNC)(a | b); }
}}