using System.Diagnostics;
namespace Core.Text
{
    public class StringBuilder
    {
        object Ctx;			// Optional database for lookaside.  Can be NULL
        string Base;		// A base allocation.  Not from malloc.
        System.Text.StringBuilder Text; // The string collected so far
        int Size;			// Amount of space allocated in zText
        int MaxSize;		// Maximum allowed string length
        bool MallocFailed;  // Becomes true if any memory allocation fails
        bool Overflowed;        // Becomes true if string size exceeds limits

        public void Append(string z, int length)
        {
            Debug.Assert(z != null || length == 0);
            if (Overflowed)
            {
                //ASSERTCOVERAGE(Overflowed);
                return;
            }
            if (length < 0)
                length = z.Length;
            if (length == 0 || SysEx.NEVER(z == null))
                return;
            Text.Append(z.Substring(0, length <= z.Length ? length : z.Length));
        }

        public new string ToString()
        {
            return Text.ToString();
        }

        public void Reset()
        {
            Text.Length = 0;
        }

        public static void Init(StringBuilder b, int capacity, int maxSize)
        {
            b.Text.Length = 0;
            if (b.Text.Capacity < capacity)
                b.Text.Capacity = capacity;
            b.Ctx = null;
            b.MaxSize = maxSize;
        }
    }
}