using System;
using System.Diagnostics;
namespace Core.IO
{
	public class MemoryVFile : VFile
	{
		const int JOURNAL_CHUNKSIZE = 4096;
		
		public class FileChunk
		{
			public FileChunk Next;                             // Next chunk in the journal 
			public byte[] Chunk = new byte[JOURNAL_CHUNKSIZE]; // Content of this chunk 
		}
		
		public class FilePoint
		{
			public long Offset;           // Offset from the beginning of the file 
			public FileChunk Chunk;      // Specific chunk into which cursor points 
		}
		
		public FileChunk First;              // Head of in-memory chunk-list
		public FilePoint endpoint = new FilePoint();            // Pointer to the end of the file
		public FilePoint readpoint = new FilePoint();           // Pointer to the end of the last xRead()
		
		public MemoryVFile()
		{
			Open = true;
		}
		
		public override RC Close() { Truncate(0); return RC.OK; }
		
		public override RC Read(byte[] buffer, int amount, long offset)
		{
			// SQLite never tries to read past the end of a rollback journal file 
			Debug.Assert(offset + amount <= endpoint.Offset);
			FileChunk chunk;
			if (readpoint.Offset != offset || offset == 0)
			{
				var iOff = 0;
				for (chunk = First; Check.ALWAYS(chunk != null) && (iOff + JOURNAL_CHUNKSIZE) <= offset; chunk = chunk.Next)
					iOff += JOURNAL_CHUNKSIZE;
			}
			else
				chunk = readpoint.Chunk;
			var chunkOffset = (int)(offset % JOURNAL_CHUNKSIZE);
			var izOut = 0;
			var read = amount;
			do
			{
				var spaceTE = JOURNAL_CHUNKSIZE - chunkOffset;
				var space = Math.Min(read, spaceTE);
				Buffer.BlockCopy(chunk.Chunk, chunkOffset, buffer, izOut, space);
				izOut += space;
				read -= spaceTE;
				chunkOffset = 0;
			} while (read >= 0 && (chunk = chunk.Next) != null && read > 0);
			readpoint.Offset = (int)(offset + amount);
			readpoint.Chunk = chunk;
			return RC.OK;
		}
		
		public override RC Write(byte[] buffer, int amount, long offset)
		{
			// An in-memory journal file should only ever be appended to. Random access writes are not required by sqlite.
			Debug.Assert(offset == endpoint.Offset);
			var izWrite = 0;
			while (amount > 0)
			{
				var chunk = endpoint.Chunk;
				var chunkOffset = (int)(endpoint.Offset % JOURNAL_CHUNKSIZE);
				var space = Math.Min(amount, JOURNAL_CHUNKSIZE - chunkOffset);
				if (chunkOffset == 0)
				{
					// new chunk is required to extend the file.
					var newChunk = new FileChunk();
					if (newChunk == null)
						return RC.IOERR_NOMEM;
					newChunk.Next = null;
					if (chunk != null) { Debug.Assert(First != null); chunk.Next = newChunk; }
					else { Debug.Assert(First == null); First = newChunk; }
					endpoint.Chunk = newChunk;
				}
				Buffer.BlockCopy(buffer, izWrite, endpoint.Chunk.Chunk, chunkOffset, space);
				izWrite += space;
				amount -= space;
				endpoint.Offset += space;
			}
			return RC.OK;
		}
		
		public override RC Truncate(long size)
		{
			Debug.Assert(size == 0);
			var pChunk = First;
			while (pChunk != null)
			{
				var pTmp = pChunk;
				pChunk = pChunk.Next;
			}
			// clear
			First = null;
			endpoint = new FilePoint();
			readpoint = new FilePoint();
			return RC.OK;
		}
		
		public override RC FileSize(ref long size) { size = endpoint.Offset; return RC.OK; }
	}
}