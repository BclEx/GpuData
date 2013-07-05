namespace Core.Text
{
    public class StringBuilder
    {
        object Ctx;			// Optional database for lookaside.  Can be NULL
        string Base;		// A base allocation.  Not from malloc.
        string Text;        // The string collected so far
        int Chars;			// Length of the string so far
        int Alloc;			// Amount of space allocated in zText
        int MaxAlloc;		// Maximum allowed string length
        bool MallocFailed;  // Becomes true if any memory allocation fails
        byte UseMalloc;	    // 0: none,  1: sqlite3DbMalloc,  2: sqlite3_malloc
        bool TooBig;        // Becomes true if string size exceeds limits
    }
}