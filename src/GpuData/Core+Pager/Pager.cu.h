// pager.h
namespace Core
{
#define PAGER_MJ_PID(x) ((Pgno)((VFile::PENDING_BYTE/((x)->pageSize))+1))

	class IPager
	{
	public:
		// NOTE: These values must match the corresponding BTREE_ values in btree.h.
		enum PAGEROPEN : char
		{
			OMIT_JOURNAL = 0x0001,	// Do not use a rollback journal
			MEMORY = 0x0002,		// In-memory database
		};

		enum LOCKINGMODE : char
		{
			QUERY = -1,
			NORMAL = 0,
			EXCLUSIVE = 1,
		};

		enum JOURNALMODE : char
		{
			JQUERY = -1,     // Query the value of journalmode
			DELETE = 0,     // Commit by deleting journal file
			PERSIST = 1,    // Commit by zeroing journal header
			OFF = 2,        // Journal omitted.
			TRUNCATE = 3,   // Commit by truncating journal
			JMEMORY = 4,     // In-memory journal file
			WAL = 5,        // Use write-ahead logging
		};
	};
}
