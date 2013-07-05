namespace Core { namespace Text
{
	class StringBuilder
	{
	public:
		void *Ctx;			// Optional database for lookaside.  Can be NULL
		char *Base;			// A base allocation.  Not from malloc.
		char *Text;			// The string collected so far
		int Chars;			// Length of the string so far
		int Alloc;			// Amount of space allocated in zText
		int MaxAlloc;		// Maximum allowed string length
		bool MallocFailed;  // Becomes true if any memory allocation fails
		uint8 UseMalloc;	// 0: none,  1: sqlite3DbMalloc,  2: sqlite3_malloc
		bool TooBig;        // Becomes true if string size exceeds limits
	};

}}