// memjournal.c
#include "../Core.cu.h"
using namespace Core;

namespace Core
{
#define JOURNAL_CHUNKSIZE ((int)(1024 - sizeof(FileChunk *)))

	typedef struct FilePoint FilePoint;
	typedef struct FileChunk FileChunk;

	struct FileChunk
	{
		FileChunk *Next;				// Next chunk in the journal
		uint8 Chunk[JOURNAL_CHUNKSIZE];	// Content of this chunk
	};

	struct FilePoint
	{
		int64 Offset;		// Offset from the beginning of the file
		FileChunk *Chunk;	// Specific chunk into which cursor points
	};

	class MemoryVFile : VFile
	{
	private:
		FileChunk *First;       // Head of in-memory chunk-list
		FilePoint _endpoint;    // Pointer to the end of the file
		FilePoint _readpoint;   // Pointer to the end of the last xRead()
		void Open();
	public:
		virtual int Read(void *buffer, int amount, int64 offset);
		virtual int Write(const void *buffer, int amount, int64 offset);
		virtual int Truncate(int64 size);
		virtual int Close();
		virtual int Sync(int flags);
		virtual int get_FileSize(int64 &size);
	};

	int MemoryVFile::Read(void *buffer, int amount, int64 offset)
	{
		// SQLite never tries to read past the end of a rollback journal file
		_assert(offset + amount <= _endpoint.Offset);
		FileChunk *chunk;
		if (_readpoint.Offset != offset || offset == 0)
		{
			int64 offset2 = 0;
			for (chunk = First; SysEx_ALWAYS(chunk) && (offset2 + JOURNAL_CHUNKSIZE) <= offset; chunk = chunk->Next)
				offset2 += JOURNAL_CHUNKSIZE;
		}
		else
			chunk = _readpoint.Chunk;
		int chunkOffset = (int)(offset % JOURNAL_CHUNKSIZE);
		uint8 *out = (uint8 *)buffer;
		int read = amount;
		do
		{
			int space = JOURNAL_CHUNKSIZE - chunkOffset;
			int copy = MIN(read, (JOURNAL_CHUNKSIZE - chunkOffset));
			_memcpy(out, &chunk->Chunk[chunkOffset], copy);
			out += copy;
			read -= space;
			chunkOffset = 0;
		} while (read >= 0 && (chunk = chunk->Next) && read > 0);
		_readpoint.Offset = offset + amount;
		_readpoint.Chunk = chunk;
		return RC::OK;
	}

	int MemoryVFile::Write(const void *buffer, int amount, int64 offset)
	{
		// An in-memory journal file should only ever be appended to. Random access writes are not required by sqlite.
		_assert(offset == _endpoint.Offset);
		uint8 *b = (uint8 *)buffer;
		while (amount > 0)
		{
			FileChunk *chunk = _endpoint.Chunk;
			int chunkOffset = (int)(_endpoint.Offset % JOURNAL_CHUNKSIZE);
			int space = MIN(amount, JOURNAL_CHUNKSIZE - chunkOffset);
			if (chunkOffset == 0)
			{
				// New chunk is required to extend the file
				FileChunk *newChunk = new FileChunk();
				if (!newChunk)
					return RC::IOERR_NOMEM;
				newChunk->Next = nullptr;
				if (chunk) { _assert(First); chunk->Next = newChunk; }
				else { _assert(!First); First = newChunk; }
				_endpoint.Chunk = newChunk;
			}
			_memcpy(&_endpoint.Chunk->Chunk[chunkOffset], b, space);
			b += space;
			amount -= space;
			_endpoint.Offset += space;
		}
		return RC::OK;
	}

	int MemoryVFile::Truncate(int64 size)
	{
		_assert(size == 0);
		FileChunk *chunk = First;
		while (chunk)
		{
			FileChunk *tmp = chunk;
			chunk = chunk->Next;
			SysEx::Free(tmp);
		}
		Open();
		return RC::OK;
	}

	int MemoryVFile::Close()
	{
		Truncate(0);
		return RC::OK;
	}

	int MemoryVFile::Sync(int flags)
	{
		return RC::OK;
	}

	int MemoryVFile::get_FileSize(int64 &size)
	{
		size = (int64)_endpoint.Offset;
		return RC::OK;
	}

	void MemoryVFile::Open()
	{
		_assert(SysEx_HASALIGNMENT8(this));
		_memset(this, 0, sizeof(MemoryVFile));
	}
}
