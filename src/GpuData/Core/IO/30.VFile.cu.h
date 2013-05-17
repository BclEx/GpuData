// sqlite3.h
namespace Core
{
#define PENDING_BYTE 0x40000000

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

		//dontknowwhere.c
		enum SYNC : char
		{
			NORMAL = 0x00002,
			FULL = 0x00003,
			DATAONLY = 0x00010,
		};

		//dontknowwhere.c
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
			DB_UNCHANGED = 0xca093fa0,
		};

		//dontknowwhere.c
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
		};

		bool Opened;

		virtual RC Read(void *buffer, int amount, int64 offset) = 0;
		virtual RC Write(const void *buffer, int amount, int64 offset) = 0;
		virtual RC Truncate(int64 size) = 0;
		virtual RC Close() = 0;
		virtual RC Sync(int flags) = 0;
		virtual RC get_FileSize(int64 &size) = 0;

		virtual RC Lock(int lock) = 0;
		virtual RC Unlock(int lock) = 0;
		virtual RC CheckReservedLock(int lock) = 0;
		virtual RC FileControl(int op, void *arg) = 0;

		virtual int SectorSize() = 0;
		virtual int get_DeviceCharacteristics() = 0;

		virtual int ShmLock(int offset, int n, int flags) = 0;
		virtual void ShmBarrier() = 0;
		virtual int ShmUnmap(int deleteFlag) = 0;
		virtual int ShmMap(int page, int pageSize, int extend, void volatile **p) = 0;

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
	};
}
