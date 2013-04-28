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

		int StatusValue(STATUS op)
	};
}
