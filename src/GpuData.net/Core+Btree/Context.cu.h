namespace Core
{
	typedef class Btree Btree;

	class Context
	{
	public:
		static const int MAX_ATTACHED = 10;

		struct BusyHandlerType
		{
			int (*Func)(void *, int);	// The busy callback
			void *Arg;					// First arg to busy callback
			int Busys;					// Incremented with each busy call
		};

		struct DB
		{
			char *Name;					// Name of this database
			Btree *Bt;					// The B*Tree structure for this database file
			uint8 InTrans;				// 0: not writable.  1: Transaction.  2: Checkpoint
			uint8 SafetyLevel;			// How aggressive at syncing data to disk
			ISchema *Schema;			// Pointer to database schema (possibly shared)
		};

		enum FLAG : uint32
		{
			VdbeTrace = 0x00000100,
			InternChanges = 0x00000200,
			FullColNames = 0x00000400,
			ShortColNames = 0x00000800,
			CountRows = 0x00001000,
			NullCallback = 0x00002000,
			SqlTrace = 0x00004000,
			VdbeListing = 0x00008000,
			WriteSchema = 0x00010000,
			NoReadlock = 0x00020000,
			IgnoreChecks = 0x00040000,
			ReadUncommitted = 0x0080000,
			LegacyFileFmt = 0x00100000,
			FullFSync = 0x00200000,
			CkptFullFSync = 0x00400000,
			RecoveryMode = 0x00800000,
			ReverseOrder = 0x01000000,
			RecTriggers = 0x02000000,
			ForeignKeys = 0x04000000,
			AutoIndex = 0x08000000,
			PreferBuiltin = 0x10000000,
			LoadExtension = 0x20000000,
			EnableTrigger = 0x40000000,
		};

		MutexEx Mutex;
		FLAG Flags;
		BusyHandlerType *BusyHandler;
		int Savepoints;					// Number of non-transaction savepoints
		int ActiveVdbeCnt;
		DB *DBs;						// All backends
		int DBsUsed;					// Number of backends currently in use

		__device__ int InvokeBusyHandler()
		{
			if (SysEx_NEVER(BusyHandler == nullptr) || BusyHandler->Func == nullptr || BusyHandler->Busys < 0)
				return 0;
			int rc = BusyHandler->Func(BusyHandler->Arg, BusyHandler->Busys);
			if (rc == 0)
				BusyHandler->Busys = -1;
			else
				BusyHandler->Busys++;
			return rc;
		}

		// HOOKS
#if ENABLE_UNLOCK_NOTIFY
		__device__ void ConnectionBlocked(Context *a, Context *b);
		__device__ void ConnectionUnlocked(Context *a);
		__device__ void ConnectionClosed(Context *a);
#else
		__device__ static void ConnectionBlocked(Context *a, Context *b) { }
		//__device__ static void ConnectionUnlocked(Context *a) { }
		//__device__ static void ConnectionClosed(Context *a) { }
#endif

		__device__ inline bool TempInMemory()
		{
			return true;
			//if (TEMP_STORE == 1) return (temp_store == 2);
			//if (TEMP_STORE == 2) return (temp_store != 1);
			//if (TEMP_STORE == 3) return true;
			//if (TEMP_STORE < 1 || TEMP_STORE > 3) return false;
			//return false;
		}
	};
}