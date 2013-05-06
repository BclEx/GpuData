// status.c
namespace Core
{
	class StatusEx
	{
	public:
		enum STATUS
		{
			MEMORY_USED = 0,
			PAGECACHE_USED = 1,
			PAGECACHE_OVERFLOW = 2,
			SCRATCH_USED = 3,
			SCRATCH_OVERFLOW = 4,
			MALLOC_SIZE = 5,
			PARSER_STACK = 6,
			PAGECACHE_SIZE = 7,
			SCRATCH_SIZE = 8,
			MALLOC_COUNT = 9,
		};

		static int StatusValue(STATUS op);
		static void StatusAdd(STATUS op, int N);
		static void StatusSet(StatusEx::STATUS op, int X);
		static int Status(StatusEx::STATUS op, int *current, int *highwater, int resetFlag);
	};
}
