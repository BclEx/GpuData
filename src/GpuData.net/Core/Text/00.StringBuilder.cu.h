namespace Core { namespace Text
{
	class StringBuilder
	{
	public:
		void *Ctx;			// Optional database for lookaside.  Can be NULL
		char *Base;			// A base allocation.  Not from malloc.
		char *Text;			// The string collected so far
		int Index;			// Length of the string so far
		int Size;			// Amount of space allocated in zText
		int MaxSize;		// Maximum allowed string length
		bool MallocFailed;  // Becomes true if any memory allocation fails
		uint8 UseMalloc;	// 0: none,  1: sqlite3DbMalloc,  2: sqlite3_malloc
		bool Overflowed;    // Becomes true if string size exceeds limits

		__device__ void Append(const char *z, int length);
		__device__ char *ToString();
		__device__ void Reset();
		__device__ static void Init(StringBuilder *b, char *text, int capacity, int maxAlloc);
	};

}}