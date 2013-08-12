// wal.h
namespace Core
{
	//typedef struct WalIndexHeader WalIndexHeader;

	struct Wal
	{
#ifdef OMIT_WAL

		inline static RC Open(VSystem *vfs, VFile *dbFile, const char *walName, bool noShm, int64 maxWalSize, Wal **walOut) { return RC::OK; }
		inline void Limit(int64 limit) { }
		inline RC Close(VFile::SYNC sync_flags, int bufLength, uint8 *buf) { return RC::OK; }
		inline RC BeginReadTransaction(bool *changed) { return RC::OK; }
		inline void EndReadTransaction() { }
		inline RC Read(Pid id, bool *inWal, int bufLength, uint8 *buf) { return RC::OK; }
		inline Pid DBSize() { return 0; }
		inline RC BeginWriteTransaction() { return RC::OK; }
		inline RC EndWriteTransaction() { return RC::OK; }
		inline RC Undo(RC (*undo)(void *, Pid), void *undoCtx) { return RC::OK; }
		inline void Savepoint(uint32 *walData) { }
		inline RC SavepointUndo(uint32 *walData) { return RC::OK; }
		inline RC Frames(int sizePage, PgHdr *list, Pid truncate, bool isCommit, VFile::SYNC sync_flags) { return RC::OK; }
		inline RC Checkpoint(int mode, int (*busy)(void*), void *busyArg, VFile::SYNC sync_flags, int bufLength, uint8 *buf, int *logs, int *checkpoints) { *logs = 0, *checkpoints = 0; return RC::OK; }
		inline RC Callback() { return RC::OK; }
		inline bool ExclusiveMode(int op) { return false; }
		inline bool HeapMemory() { return false; }
#ifdef ENABLE_ZIPVFS
		inline int Framesize() { return 0; }
#endif

#else
		enum MODE : uint8
		{
			NORMAL = 0,
			EXCLUSIVE = 1,
			HEAPMEMORY = 2,
		};

		enum RDONLY : uint8
		{
			RDWR = 0,			// Normal read/write connection
			RDONLY = 1,			// The WAL file is readonly
			SHM_RDONLY = 2,		// The SHM file is readonly
		};

		VSystem *Vfs;				// The VFS used to create pDbFd
		VFile *DBFile;					// File handle for the database file
		VFile *WalFile;					// File handle for WAL file
		uint32 Callback;				// Value to pass to log callback (or 0)
		int64 MaxWalSize;				// Truncate WAL to this size upon reset
		int SizeFirstBlock;				// Size of first block written to WAL file
		//int nWiData;					// Size of array apWiData
		volatile uint32 **WiData;		// Pointer to wal-index content in memory
		uint32 SizePage;                // Database page size
		int16 ReadLock;					// Which read lock is being held.  -1 for none
		uint8 SyncFlags;				// Flags to use to sync header writes
		MODE ExclusiveMode;				// Non-zero if connection is in exclusive mode
		bool WriteLock;					// True if in a write transaction
		bool CheckpointLock;			// True if holding a checkpoint lock
		RDONLY ReadOnly;			// WAL_RDWR, WAL_RDONLY, or WAL_SHM_RDONLY
		bool TruncateOnCommit;			// True to truncate WAL file on commit
		uint8 SyncHeader;				// Fsync the WAL header if true
		uint8 PadToSectorBoundary;		// Pad transactions out to the next sector
		struct IndexHeader
		{
			uint32 Version;                 // Wal-index version
			uint32 Unused;					// Unused (padding) field
			uint32 Change;                  // Counter incremented each transaction
			bool IsInit;					// 1 when initialized
			bool BigEndianChecksum;			// True if checksums in WAL are big-endian
			uint16 SizePage;                // Database page size in bytes. 1==64K
			uint32 MaxFrame;                // Index of last valid frame in the WAL
			uint32 Pages;                   // Size of database in pages
			uint32 FrameChecksum[2];		// Checksum of last frame in log
			uint32 Salt[2];					// Two salt values copied from WAL header
			uint32 Checksum[2];				// Checksum over all prior fields
		} Header; // Wal-index header for current transaction
		const char *WalName;			// Name of WAL file
		uint32 Checkpoints;				// Checkpoint sequence counter in the wal-header
#ifdef _DEBUG
		uint8 LockError;				// True if a locking error has occurred
#endif
		//
		static RC Open(VSystem *vfs, VFile *dbFile, const char *walName, bool noShm, int64 maxWalSize, Wal **walOut);
		void Limit(int64 limit);
		RC Close(VFile::SYNC sync_flags, int bufLength, uint8 *buf);
		RC BeginReadTransaction(bool *changed);
		void EndReadTransaction();
		RC Read(Pid id, bool *inWal, int bufLength, uint8 *buf);
		Pid DBSize();
		RC BeginWriteTransaction();
		RC EndWriteTransaction();
		RC Undo(int (*undo)(void *, Pid), void *undoCtx);
		void Savepoint(uint32 *walData);
		RC SavepointUndo(uint32 *walData);
		RC Frames(int sizePage, PgHdr *list, Pid truncate, bool isCommit, VFile::SYNC sync_flags);
		RC Checkpoint(int mode, int (*busy)(void*), void *busyArg, VFile::SYNC sync_flags, int bufLength, uint8 *buf, int *logs, int *checkpoints);
		int Callback();
		bool ExclusiveMode(int op);
		bool HeapMemory();
#ifdef ENABLE_ZIPVFS
		int Framesize();
#endif

#endif
	};
}