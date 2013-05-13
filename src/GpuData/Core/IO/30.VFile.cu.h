// sqlite3.h
namespace Core
{
	class VFile
	{
	public:
		enum LOCK : byte
        {
            NO = 0,
            SHARED = 1,
            RESERVED = 2,
            PENDING = 3,
            EXCLUSIVE = 4,
            UNKNOWN = 5,
        };

		//dontknowwhere.c
		enum SYNC : byte
        {
            NORMAL = 0x00002,
            FULL = 0x00003,
            DATAONLY = 0x00010,
        };

		virtual int Read(void *buffer, int amount, int64 offset) = 0;
		virtual int Write(const void *buffer, int amount, int64 offset) = 0;
		virtual int Truncate(int64 size) = 0;
		virtual int Close() = 0;
		virtual int Sync(int flags) = 0;
		virtual int get_FileSize(int64 &size) = 0;
	};
}
