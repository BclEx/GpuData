// sqlite3.h
namespace Core
{
#define NO_LOCK         0
#define SHARED_LOCK     1
#define RESERVED_LOCK   2
#define PENDING_LOCK    3
#define EXCLUSIVE_LOCK  4

	class VFile
	{
	public:
		virtual int Read(void *buffer, int amount, int64 offset) = 0;
		virtual int Write(const void *buffer, int amount, int64 offset) = 0;
		virtual int Truncate(int64 size) = 0;
		virtual int Close() = 0;
		virtual int Sync(int flags) = 0;
		virtual int get_FileSize(int64 &size) = 0;
	};
}
