namespace Core
{
    public class PCacheGlobal
    {
        public PGroup Group = new PGroup();   // The global PGroup for mode (2)
        
        // Variables related to CONFIG_PAGECACHE settings.  The szSlot, nSlot, pStart, pEnd, nReserve, and isInit values are all
        // fixed at sqlite3_initialize() time and do not require mutex protection. The nFreeSlot and pFree values do require mutex protection.
        public bool IsInit;         // True if initialized
        public int SizeSlot;        // Size of each free slot
        public int Slots;           // The number of pcache slots
        public int Reserves;        // Try to keep nFreeSlot above this
        public object Start, End;   // Bounds of pagecache malloc range
        // Above requires no mutex.  Use mutex below for variable that follow.
        public MutexEx Mutex;       // Mutex for accessing the following:
        public PgFreeslot Free;     // Free page blocks
        public int FreeSlots;       // Number of unused pcache slots
        // The following value requires a mutex to change.  We skip the mutex on reading because (1) most platforms read a 32-bit integer atomically and
        // (2) even if an incorrect value is read, no great harm is done since this is really just an optimization.
        public bool UnderPressure;         // True if low on PAGECACHE memory

        //public static void Touch() { }
    }
}
