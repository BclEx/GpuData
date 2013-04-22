#define nullptr NULL
#if __CUDA_ARCH__ == 100
#error Atomics only used with > sm_10 architecture
#endif
#include <string.h>
#include <stdio.h>
#if defined (__APPLE__) || defined(MACOSX)
#include <alloca.h>
#else
#include <malloc.h>
#endif
#include "Runtime.h"
#include "cuda_runtime_api.h"

#define RUNTIME_UNRESTRICTED -1

typedef struct __align__(8)
{
	int threadid; // RUNTIME_UNRESTRICTED for unrestricted
	int blockid;  // RUNTIME_UNRESTRICTED for unrestricted
} runtimeRestriction;

typedef struct __align__(8)
{
	volatile char *blockPtr; // current atomically-incremented non-wrapped offset
	void *reserved;
	runtimeRestriction restriction;
	size_t blockSize;
	size_t blocksLength; // size of circular buffer (set up by host)
	char *blocks; // start of circular buffer (set up by host)
} runtimeHeap;

typedef struct __align__(8)
{
	unsigned short magic;		// magic number says we're valid
	unsigned short type;		// type of block
	unsigned short fmtoffset;	// offset of fmt string into buffer
	unsigned short blockid;		// block ID of author
	unsigned short threadid;	// thread ID of author
} runtimeBlockHeader;

static FILE *_stream;

///////////////////////////////////////////////////////////////////////////////
// HOST SIDE

#define RUNTIME_MAGIC (unsigned short)0xC811
#define RUNTIME_ALIGNSIZE sizeof(long long)
#define RUNTIMETYPE_PRINTF 1
#define RUNTIMETYPE_ASSERT 2
#define RUNTIMETYPE_THROW 3

extern "C" cudaRuntimeHost cudaRuntimeInit(size_t blockSize, size_t length, cudaError_t *error, void *reserved)
{
	cudaError_t localError; if (error == nullptr) error = &localError;
	cudaRuntimeHost host; memset(&host, 0, sizeof(cudaRuntimeHost));
	// fix up blockSize to include fallocBlockHeader
	blockSize = (blockSize + (RUNTIME_ALIGNSIZE - 1)) & ~(RUNTIME_ALIGNSIZE - 1);
	// fix up length to be a multiple of blockSize
	if (!length || length % blockSize)
		length += blockSize - (length % blockSize);
	size_t blocksLength = length;
	length = (length + sizeof(runtimeHeap) + 15) & ~15;
	// allocate a heap on the device and zero it
	char *heap;
	if ((*error = cudaMalloc((void **)&heap, length)) != cudaSuccess || (*error = cudaMemset(heap, 0, length)) != cudaSuccess)
		return host;
	char *blocks = heap + length - blocksLength;
	// no restrictions to begin with
	runtimeRestriction restriction;
	restriction.threadid = restriction.blockid = RUNTIME_UNRESTRICTED;
	// transfer to heap
	runtimeHeap hostHeap;
	hostHeap.blockPtr = (volatile char *)blocks;
	hostHeap.blocks = blocks;
	hostHeap.reserved = reserved;
	hostHeap.restriction = restriction;
	hostHeap.blockSize = blockSize;
	hostHeap.blocksLength = blocksLength;
	if ((*error = cudaMemcpy(heap, &hostHeap, sizeof(runtimeHeap), cudaMemcpyHostToDevice)) != cudaSuccess)
		return host;
	// return the heap
	host.reserved = reserved;
	host.heap = heap;
	host.blocks = host.blockStart = blocks;
	host.blockSize = blockSize;
	host.blocksLength = blocksLength;
	host.length = length;
	return host;
}

extern "C" void cudaRuntimeEnd(cudaRuntimeHost &host)
{
	if (!host.heap)
		return;
	cudaFree(host.heap); host.heap = nullptr;
}

extern "C" void cudaRuntimeSetHandler(cudaRuntimeHost &host, cudaAssertHandler handler)
{
	host.assertHandler = handler;
}

static bool outputPrintfData(FILE *stream, size_t blockSize, char *fmt, char *data);
static int executeRuntime(cudaAssertHandler assertHandler, size_t blockSize, char *heap, int headings, int clear, char *bufstart, char *bufend, char *bufptr, char *endptr)
{
	// grab, piece-by-piece, each output element until we catch up with the circular buffer end pointer
	int count = 0;
	char *b = (char *)alloca(blockSize + 1);
	b[blockSize] = '\0';
	while (bufptr != endptr)
	{
		// wrap ourselves at the end-of-buffer
		if (bufptr == bufend)
			bufptr = bufstart;
		// adjust our start pointer to within the circular buffer and copy a block.
		if (cudaMemcpy(b, bufptr, blockSize, cudaMemcpyDeviceToHost) != cudaSuccess)
			break;
		// if the magic number isn't valid, then this write hasn't gone through yet and we'll wait until it does (or we're past the end for non-async printfs).
		runtimeBlockHeader *hdr = (runtimeBlockHeader *)b;
		//if (hdr->magic != RUNTIME_MAGIC || hdr->fmtoffset >= blockSize)
		//{
		//	fprintf(_stream, "Bad magic number in runtime header\n");
		//	break;
		//}
		// Extract all the info and get this printf done
		bool error = false;
		switch (hdr->type)
		{
		case RUNTIMETYPE_PRINTF:
			if (headings)
				fprintf(_stream, "[%d, %d]: ", hdr->blockid, hdr->threadid);
			if (hdr->fmtoffset == 0)
				fprintf(_stream, "printf buffer overflow\n");
			else
				error = !outputPrintfData(_stream, blockSize, b + hdr->fmtoffset, b + sizeof(runtimeBlockHeader));
			break;
		case RUNTIMETYPE_ASSERT:
			if (headings)
				fprintf(_stream, "[%d, %d]: ", hdr->blockid, hdr->threadid);
			fprintf(_stream, "ASSERT: ");
			if (hdr->fmtoffset == 0)
				fprintf(_stream, "printf buffer overflow\n");
			else
				error = !outputPrintfData(_stream, blockSize, b + hdr->fmtoffset, b + sizeof(runtimeBlockHeader));
			fprintf(_stream, "\n");
			//assertHandler();
			break;
		case RUNTIMETYPE_THROW:
			if (headings)
				fprintf(_stream, "[%d, %d]: ", hdr->blockid, hdr->threadid);
			fprintf(_stream, "THROW: ");
			if (hdr->fmtoffset == 0)
				fprintf(_stream, "printf buffer overflow\n");
			else
				error = !outputPrintfData(_stream, blockSize, b + hdr->fmtoffset, b + sizeof(runtimeBlockHeader));
			fprintf(_stream, "\n");
			break;
		}
		if (error)
			break;
		count++;
		// clear if asked
		if (clear)
			cudaMemset(bufptr, 0, blockSize);
		// now advance our start location, because we're done, and keep copying
		bufptr += blockSize;
	}
	return count;
}

extern "C" cudaError_t cudaRuntimeExecute(cudaRuntimeHost &host, void *stream, bool showThreadID)
{
	// for now, we force "synchronous" mode which means we're not concurrent with kernel execution. This also means we don't need clearOnPrint.
	// if you're patching it for async operation, here's where you want it.
	bool sync = true;
	//
	_stream = (FILE *)(stream == nullptr ? stdout : stream);
	// initialisation check
	if (!host.blockStart || !host.heap || !_stream)
		return cudaErrorMissingConfiguration;
	size_t blocksLength = host.blocksLength;
	char *blocks = host.blocks;
	// grab the current "end of circular buffer" pointer.
	char *blockEnd = nullptr;
	cudaMemcpy(&blockEnd, host.heap, sizeof(char *), cudaMemcpyDeviceToHost);
	// adjust our starting and ending pointers to within the block
	char *bufptr = ((host.blockStart - blocks) % blocksLength) + blocks;
	char *endptr = ((blockEnd - blocks) % blocksLength) + blocks;
	// for synchronous (i.e. after-kernel-exit) printf display, we have to handle circular buffer wrap carefully because we could miss those past "end".
	if (sync)
		executeRuntime(host.assertHandler, host.blockSize, blocks, showThreadID, false, blocks, blocks + blocksLength, endptr, blocks + blocksLength);
	executeRuntime(host.assertHandler, host.blockSize, blocks, showThreadID, false, blocks, blocks + blocksLength, bufptr, endptr);
	host.blockStart = blockEnd;
	// if we were synchronous, then we must ensure that the memory is cleared on exit otherwise another kernel launch with a different grid size could conflict.
	if (sync)
		cudaMemset(blocks, 0, blocksLength);
	return cudaSuccess;
}

#pragma region PRINTF

static bool outputPrintfData(FILE *stream, size_t blockSize, char *fmt, char *data)
{
	// Format string is prefixed by a length that we don't need
	fmt += RUNTIME_ALIGNSIZE;
	// now run through it, printing everything we can. We must run to every % character, extract only that, and use printf to format it.
	char *p = strchr(fmt, '%');
	while (p != nullptr)
	{
		// print up to the % character
		*p = '\0'; fputs(fmt, stream); *p = '%'; // Put back the %
		// now handle the format specifier
		char *format = p++; // Points to the '%'
		p += strcspn(p, "%cdiouxXeEfgGaAnps");
		if (*p == '\0') // If no format specifier, print the whole thing
		{
			fmt = format;
			break;
		}
		// cut out the format bit and use printf to print it. It's prefixed by its length.
		int arglen = *(int *)data;
		if (arglen > blockSize)
		{
			fputs("Corrupt printf buffer data - aborting\n", stream);
			return false;
		}
		data += RUNTIME_ALIGNSIZE;
		//
		char specifier = *p++;
		char c = *p; *p = '\0'; // store for later and clip
		switch (specifier)
		{
			// these all take integer arguments
		case 'c': case 'd': case 'i': case 'o': case 'u': case 'x': case 'X': case 'p': fprintf(stream, format, *((int *)data)); break;
			// these all take float/double arguments, float vs. double thing
		case 'e': case 'E': case 'f': case 'g': case 'G': case 'a': case 'A': if (arglen == 4) fprintf(stream, format, *((float *)data)); else fprintf(stream, format, *((double *)data)); break;
			// Strings are handled in a special way
		case 's': fprintf(stream, format, (char *)data); break;
			// % is special
		case '%': fprintf(stream, "%%"); break;
			// everything else is just printed out as-is
		default: fprintf(stream, format); break;
		}
		data += RUNTIME_ALIGNSIZE; // move on to next argument
		*p = c; fmt = p; // restore what we removed, and adjust fmt string to be past the specifier
		p = strchr(fmt, '%'); // and get the next specifier
	}
	// print out the last of the string
	fputs(fmt, stream);
	return true;
}

static bool outputPrintfData(char *stream, size_t blockSize, char *fmt, char *data)
{
	// Format string is prefixed by a length that we don't need
	fmt += RUNTIME_ALIGNSIZE;
	// now run through it, printing everything we can. We must run to every % character, extract only that, and use printf to format it.
	char *p = strchr(fmt, '%');
	while (p != nullptr)
	{
		// print up to the % character
		*p = '\0'; strcpy(stream, fmt); *p = '%'; // Put back the %
		// now handle the format specifier
		char *format = p++; // Points to the '%'
		p += strcspn(p, "%cdiouxXeEfgGaAnps");
		if (*p == '\0') // If no format specifier, print the whole thing
		{
			fmt = format;
			break;
		}
		// cut out the format bit and use printf to print it. It's prefixed by its length.
		int arglen = *(int *)data;
		if (arglen > blockSize)
		{
			strcpy(stream, "Corrupt printf buffer data - aborting\n");
			return false;
		}
		data += RUNTIME_ALIGNSIZE;
		//
		char specifier = *p++;
		char c = *p; *p = '\0'; // store for later and clip
		switch (specifier)
		{
			// these all take integer arguments
		case 'c': case 'd': case 'i': case 'o': case 'u': case 'x': case 'X': case 'p': sprintf(stream, format, *((int *)data)); break;
			// these all take float/double arguments, float vs. double thing
		case 'e': case 'E': case 'f': case 'g': case 'G': case 'a': case 'A': if (arglen == 4) sprintf(stream, format, *((float *)data)); else sprintf(stream, format, *((double *)data)); break;
			// Strings are handled in a special way
		case 's': sprintf(stream, format, (char *)data); break;
			// % is special
		case '%': sprintf(stream, "%%"); break;
			// everything else is just printed out as-is
		default: sprintf(stream, format); break;
		}
		data += RUNTIME_ALIGNSIZE; // move on to next argument
		*p = c; fmt = p; // restore what we removed, and adjust fmt string to be past the specifier
		p = strchr(fmt, '%'); // and get the next specifier
	}
	// print out the last of the string
	strcpy(stream, fmt);
	return true;
}

#pragma endregion
