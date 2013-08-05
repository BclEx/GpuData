// ow_win.c
#include "../Core.cu.h"
#include <Windows.h>

namespace Core { namespace IO
{

	// winFile
	class CoreVFile : public VFile
	{
	public:
		VSystem *Vfs;		// The VFS used to open this file
		HANDLE h;               // Handle for accessing the file
		uint8 locktype;         // Type of lock currently held on this file
		short sharedLockByte;   // Randomly chosen byte used as a shared lock
		uint8 ctrlFlags;        // Flags.  See WINFILE_* below
		DWORD lastErrno;        // The Windows errno from the last I/O error
#ifndef SQLITE_OMIT_WAL
		winShm *pShm;           // Instance of shared memory on this file */
#endif
		const char *zPath;      // Full pathname of this file
		int szChunk;            // Chunk size configured by FCNTL_CHUNK_SIZE
#if OS_WINCE
		LPWSTR zDeleteOnClose;  // Name of file to delete when closing
		HANDLE hMutex;          // Mutex used to control access to shared lock
		HANDLE hShared;         // Shared memory segment used for locking
		winceLock local;        // Locks obtained by this instance of winFile
		winceLock *shared;      // Global shared lock memory for the file
#endif
	};

	class CoreVFileSystem : public VSystem
	{
	private:
	public:
		__device__ virtual RC Open(const char *name, VFile *id, OPEN flags, OPEN *outFlags);
		__device__ virtual RC Write(const void *buffer, int amount, int64 offset);
	};

	RC CoreVFileSystem::Open(const char *name, VFile *id, OPEN flags, OPEN *outFlags)
	{
//		HANDLE h;
//		DWORD lastErrno;
//		DWORD dwDesiredAccess;
//		DWORD dwShareMode;
//		DWORD dwCreationDisposition;
//		DWORD dwFlagsAndAttributes = 0;
//#if OS_WINCE
//		int isTemp = 0;
//#endif
//			void *converted; // Filename in OS encoding
//		const char *utf8Name = name; // Filename in UTF-8 encoding
//		int cnt = 0;
		_assert(id != nullptr);

		// If argument zPath is a NULL pointer, this function is required to open a temporary file. Use this buffer to store the file name in.
		char tmpname[MAX_PATH + 2]; // Buffer used to create temp filename

		RC rc = RC::OK;
		OPEN type = (OPEN)(flags & 0xFFFFFF00); // Type of file to open
		bool exclusive = (flags & OPEN::EXCLUSIVE);
		bool delete_ = (flags & OPEN::DELETEONCLOSE);
		bool create = (flags & OPEN::CREATE);
		bool readonly = (flags & OPEN::READONLY);
		bool readWrite = (flags & OPEN::OREADWRITE);
		bool openJournal = (create && (type == OPEN::MASTER_JOURNAL || type == OPEN::MAIN_JOURNAL || type == OPEN::WAL));

		// Check the following statements are true: 
		//
		//   (a) Exactly one of the READWRITE and READONLY flags must be set, and 
		//   (b) if CREATE is set, then READWRITE must also be set, and
		//   (c) if EXCLUSIVE is set, then CREATE must also be set.
		//   (d) if DELETEONCLOSE is set, then CREATE must also be set.
		_assert((!readonly|| !readWrite) && (readWrite || readonly));
		_assert(!create || readWrite);
		_assert(!exclusive || create);
		_assert(!delete_ || create);

		// The main DB, main journal, WAL file and master journal are never automatically deleted. Nor are they ever temporary files.
		_assert((!delete_ && name) || type != OPEN::MAIN_DB);
		_assert((!delete_ && name) || type != OPEN::MAIN_JOURNAL);
		_assert((!delete_ && name) || type != OPEN::MASTER_JOURNAL);
		_assert((!delete_ && name) || type != OPEN::WAL);

		// Assert that the upper layer has set one of the "file-type" flags.
		_assert(type == OPEN::MAIN_DB || type == OPEN::TEMP_DB ||
			type == OPEN::MAIN_JOURNAL || type == OPEN::TEMP_JOURNAL ||
			type == OPEN::SUBJOURNAL || type == OPEN::MASTER_JOURNAL ||
			type == OPEN::TRANSIENT_DB || type == OPEN::WAL);

		CoreVFile *file = (CoreVFile *)id;
		_memset(file, 0, sizeof(CoreVFile));
		file->h = INVALID_HANDLE_VALUE;

#if !OS_WINRT
		if (!sqlite3_temp_directory)
			sqlite3_log(SQLITE_ERROR, "sqlite3_temp_directory variable should be set for WinRT");
#endif

		/// If the second argument to this function is NULL, generate a temporary file name to use 
		if (!utf8Name)
		{
			_assert(delete_ && !openJournal);
			_memset(tmpname, 0, MAX_PATH + 2);
			rc = getTempname(MAX_PATH + 2, tmpname);
			if (rc != RC::OK)
				return rc;
			utf8Name = tmpname;
		}

		// Database filenames are double-zero terminated if they are not URIs with parameters.  Hence, they can always be passed into
		// sqlite3_uri_parameter().
		_assert((type != OPEN::MAIN_DB) || (flags & OPEN::URI) || utf8Name[strlen(utf8Name) + 1] == 0);

		/* Convert the filename to the system encoding. */
		zConverted = convertUtf8Filename(zUtf8Name);
		if( zConverted==0 ){
			return SQLITE_IOERR_NOMEM;
		}

		if( winIsDir(zConverted) ){
			sqlite3_free(zConverted);
			return SQLITE_CANTOPEN_ISDIR;
		}

		if( isReadWrite ){
			dwDesiredAccess = GENERIC_READ | GENERIC_WRITE;
		}else{
			dwDesiredAccess = GENERIC_READ;
		}

		/* SQLITE_OPEN_EXCLUSIVE is used to make sure that a new file is 
		** created. SQLite doesn't use it to indicate "exclusive access" 
		** as it is usually understood.
		*/
		if( isExclusive ){
			/* Creates a new file, only if it does not already exist. */
			/* If the file exists, it fails. */
			dwCreationDisposition = CREATE_NEW;
		}else if( isCreate ){
			/* Open existing file, or create if it doesn't exist */
			dwCreationDisposition = OPEN_ALWAYS;
		}else{
			/* Opens a file, only if it exists. */
			dwCreationDisposition = OPEN_EXISTING;
		}

		dwShareMode = FILE_SHARE_READ | FILE_SHARE_WRITE;

		if( isDelete ){
#if SQLITE_OS_WINCE
			dwFlagsAndAttributes = FILE_ATTRIBUTE_HIDDEN;
			isTemp = 1;
#else
			dwFlagsAndAttributes = FILE_ATTRIBUTE_TEMPORARY
				| FILE_ATTRIBUTE_HIDDEN
				| FILE_FLAG_DELETE_ON_CLOSE;
#endif
		}else{
			dwFlagsAndAttributes = FILE_ATTRIBUTE_NORMAL;
		}
		/* Reports from the internet are that performance is always
		** better if FILE_FLAG_RANDOM_ACCESS is used.  Ticket #2699. */
#if SQLITE_OS_WINCE
		dwFlagsAndAttributes |= FILE_FLAG_RANDOM_ACCESS;
#endif

		if( isNT() ){
#if SQLITE_OS_WINRT
			CREATEFILE2_EXTENDED_PARAMETERS extendedParameters;
			extendedParameters.dwSize = sizeof(CREATEFILE2_EXTENDED_PARAMETERS);
			extendedParameters.dwFileAttributes =
				dwFlagsAndAttributes & FILE_ATTRIBUTE_MASK;
			extendedParameters.dwFileFlags = dwFlagsAndAttributes & FILE_FLAG_MASK;
			extendedParameters.dwSecurityQosFlags = SECURITY_ANONYMOUS;
			extendedParameters.lpSecurityAttributes = NULL;
			extendedParameters.hTemplateFile = NULL;
			while( (h = osCreateFile2((LPCWSTR)zConverted,
				dwDesiredAccess,
				dwShareMode,
				dwCreationDisposition,
				&extendedParameters))==INVALID_HANDLE_VALUE &&
				retryIoerr(&cnt, &lastErrno) ){
					/* Noop */
			}
#else
			while( (h = osCreateFileW((LPCWSTR)zConverted,
				dwDesiredAccess,
				dwShareMode, NULL,
				dwCreationDisposition,
				dwFlagsAndAttributes,
				NULL))==INVALID_HANDLE_VALUE &&
				retryIoerr(&cnt, &lastErrno) ){
					/* Noop */
			}
#endif
		}
#ifdef SQLITE_WIN32_HAS_ANSI
		else{
			while( (h = osCreateFileA((LPCSTR)zConverted,
				dwDesiredAccess,
				dwShareMode, NULL,
				dwCreationDisposition,
				dwFlagsAndAttributes,
				NULL))==INVALID_HANDLE_VALUE &&
				retryIoerr(&cnt, &lastErrno) ){
					/* Noop */
			}
		}
#endif
		logIoerr(cnt);

		OSTRACE(("OPEN %d %s 0x%lx %s\n", 
			h, zName, dwDesiredAccess, 
			h==INVALID_HANDLE_VALUE ? "failed" : "ok"));

		if( h==INVALID_HANDLE_VALUE ){
			pFile->lastErrno = lastErrno;
			winLogError(SQLITE_CANTOPEN, pFile->lastErrno, "winOpen", zUtf8Name);
			sqlite3_free(zConverted);
			if( isReadWrite && !isExclusive ){
				return winOpen(pVfs, zName, id, 
					((flags|SQLITE_OPEN_READONLY) &
					~(SQLITE_OPEN_CREATE|SQLITE_OPEN_READWRITE)),
					pOutFlags);
			}else{
				return SQLITE_CANTOPEN_BKPT;
			}
		}

		if( pOutFlags ){
			if( isReadWrite ){
				*pOutFlags = SQLITE_OPEN_READWRITE;
			}else{
				*pOutFlags = SQLITE_OPEN_READONLY;
			}
		}

#if SQLITE_OS_WINCE
		if( isReadWrite && eType==SQLITE_OPEN_MAIN_DB
			&& (rc = winceCreateLock(zName, pFile))!=SQLITE_OK
			){
				osCloseHandle(h);
				sqlite3_free(zConverted);
				return rc;
		}
		if( isTemp ){
			pFile->zDeleteOnClose = zConverted;
		}else
#endif
		{
			sqlite3_free(zConverted);
		}

		pFile->pMethod = &winIoMethod;
		pFile->pVfs = pVfs;
		pFile->h = h;
		if( sqlite3_uri_boolean(zName, "psow", SQLITE_POWERSAFE_OVERWRITE) ){
			pFile->ctrlFlags |= WINFILE_PSOW;
		}
		pFile->lastErrno = NO_ERROR;
		pFile->zPath = zName;

		OpenCounter(+1);
		return rc;
	}


	static int winDelete(sqlite3_vfs *pVfs, const char *zFilename, int syncDir)
	{
		int cnt = 0;
		int rc;
		DWORD attr;
		DWORD lastErrno;
		void *zConverted;
		UNUSED_PARAMETER(pVfs);
		UNUSED_PARAMETER(syncDir);

		SimulateIOError(return SQLITE_IOERR_DELETE);
		zConverted = convertUtf8Filename(zFilename);
		if( zConverted==0 ){
			return SQLITE_IOERR_NOMEM;
		}
		if( isNT() ){
			do {
#if SQLITE_OS_WINRT
				WIN32_FILE_ATTRIBUTE_DATA sAttrData;
				memset(&sAttrData, 0, sizeof(sAttrData));
				if ( osGetFileAttributesExW(zConverted, GetFileExInfoStandard,
					&sAttrData) ){
						attr = sAttrData.dwFileAttributes;
				}else{
					lastErrno = osGetLastError();
					if( lastErrno==ERROR_FILE_NOT_FOUND
						|| lastErrno==ERROR_PATH_NOT_FOUND ){
							rc = SQLITE_IOERR_DELETE_NOENT; /* Already gone? */
					}else{
						rc = SQLITE_ERROR;
					}
					break;
				}
#else
				attr = osGetFileAttributesW(zConverted);
#endif
				if ( attr==INVALID_FILE_ATTRIBUTES ){
					lastErrno = osGetLastError();
					if( lastErrno==ERROR_FILE_NOT_FOUND
						|| lastErrno==ERROR_PATH_NOT_FOUND ){
							rc = SQLITE_IOERR_DELETE_NOENT; /* Already gone? */
					}else{
						rc = SQLITE_ERROR;
					}
					break;
				}
				if ( attr&FILE_ATTRIBUTE_DIRECTORY ){
					rc = SQLITE_ERROR; /* Files only. */
					break;
				}
				if ( osDeleteFileW(zConverted) ){
					rc = SQLITE_OK; /* Deleted OK. */
					break;
				}
				if ( !retryIoerr(&cnt, &lastErrno) ){
					rc = SQLITE_ERROR; /* No more retries. */
					break;
				}
			} while(1);
		}
#ifdef SQLITE_WIN32_HAS_ANSI
		else{
			do {
				attr = osGetFileAttributesA(zConverted);
				if ( attr==INVALID_FILE_ATTRIBUTES ){
					lastErrno = osGetLastError();
					if( lastErrno==ERROR_FILE_NOT_FOUND
						|| lastErrno==ERROR_PATH_NOT_FOUND ){
							rc = SQLITE_IOERR_DELETE_NOENT; /* Already gone? */
					}else{
						rc = SQLITE_ERROR;
					}
					break;
				}
				if ( attr&FILE_ATTRIBUTE_DIRECTORY ){
					rc = SQLITE_ERROR; /* Files only. */
					break;
				}
				if ( osDeleteFileA(zConverted) ){
					rc = SQLITE_OK; /* Deleted OK. */
					break;
				}
				if ( !retryIoerr(&cnt, &lastErrno) ){
					rc = SQLITE_ERROR; /* No more retries. */
					break;
				}
			} while(1);
		}
#endif
		if( rc && rc!=SQLITE_IOERR_DELETE_NOENT ){
			rc = winLogError(SQLITE_IOERR_DELETE, lastErrno,
				"winDelete", zFilename);
		}else{
			logIoerr(cnt);
		}
		sqlite3_free(zConverted);
		OSTRACE(("DELETE \"%s\" %s\n", zFilename, (rc ? "failed" : "ok" )));
		return rc;
	}

	static int winAccess(sqlite3_vfs *pVfs, const char *zFilename, int flags, int *pResOut)
	{
		DWORD attr;
		int rc = 0;
		DWORD lastErrno;
		void *zConverted;
		UNUSED_PARAMETER(pVfs);

		SimulateIOError( return SQLITE_IOERR_ACCESS; );
		zConverted = convertUtf8Filename(zFilename);
		if( zConverted==0 ){
			return SQLITE_IOERR_NOMEM;
		}
		if( isNT() ){
			int cnt = 0;
			WIN32_FILE_ATTRIBUTE_DATA sAttrData;
			memset(&sAttrData, 0, sizeof(sAttrData));
			while( !(rc = osGetFileAttributesExW((LPCWSTR)zConverted,
				GetFileExInfoStandard, 
				&sAttrData)) && retryIoerr(&cnt, &lastErrno) ){}
			if( rc ){
				/* For an SQLITE_ACCESS_EXISTS query, treat a zero-length file
				** as if it does not exist.
				*/
				if(    flags==SQLITE_ACCESS_EXISTS
					&& sAttrData.nFileSizeHigh==0 
					&& sAttrData.nFileSizeLow==0 ){
						attr = INVALID_FILE_ATTRIBUTES;
				}else{
					attr = sAttrData.dwFileAttributes;
				}
			}else{
				logIoerr(cnt);
				if( lastErrno!=ERROR_FILE_NOT_FOUND && lastErrno!=ERROR_PATH_NOT_FOUND ){
					winLogError(SQLITE_IOERR_ACCESS, lastErrno, "winAccess", zFilename);
					sqlite3_free(zConverted);
					return SQLITE_IOERR_ACCESS;
				}else{
					attr = INVALID_FILE_ATTRIBUTES;
				}
			}
		}
#ifdef SQLITE_WIN32_HAS_ANSI
		else{
			attr = osGetFileAttributesA((char*)zConverted);
		}
#endif
		sqlite3_free(zConverted);
		switch( flags ){
		case SQLITE_ACCESS_READ:
		case SQLITE_ACCESS_EXISTS:
			rc = attr!=INVALID_FILE_ATTRIBUTES;
			break;
		case SQLITE_ACCESS_READWRITE:
			rc = attr!=INVALID_FILE_ATTRIBUTES &&
				(attr & FILE_ATTRIBUTE_READONLY)==0;
			break;
		default:
			assert(!"Invalid flags argument");
		}
		*pResOut = rc;
		return SQLITE_OK;
	}


	static BOOL winIsVerbatimPathname(
		const char *zPathname
		){
			/*
			** If the path name starts with a forward slash or a backslash, it is either
			** a legal UNC name, a volume relative path, or an absolute path name in the
			** "Unix" format on Windows.  There is no easy way to differentiate between
			** the final two cases; therefore, we return the safer return value of TRUE
			** so that callers of this function will simply use it verbatim.
			*/
			if ( zPathname[0]=='/' || zPathname[0]=='\\' ){
				return TRUE;
			}

			/*
			** If the path name starts with a letter and a colon it is either a volume
			** relative path or an absolute path.  Callers of this function must not
			** attempt to treat it as a relative path name (i.e. they should simply use
			** it verbatim).
			*/
			if ( sqlite3Isalpha(zPathname[0]) && zPathname[1]==':' ){
				return TRUE;
			}

			/*
			** If we get to this point, the path name should almost certainly be a purely
			** relative one (i.e. not a UNC name, not absolute, and not volume relative).
			*/
			return FALSE;
	}

	static int winFullPathname(sqlite3_vfs *pVfs, const char *zRelative, int nFull, char *zFull)
	{

#if defined(__CYGWIN__)
		SimulateIOError( return SQLITE_ERROR );
		UNUSED_PARAMETER(nFull);
		assert( pVfs->mxPathname>=MAX_PATH );
		assert( nFull>=pVfs->mxPathname );
		if ( sqlite3_data_directory && !winIsVerbatimPathname(zRelative) ){
			/*
			** NOTE: We are dealing with a relative path name and the data
			**       directory has been set.  Therefore, use it as the basis
			**       for converting the relative path name to an absolute
			**       one by prepending the data directory and a slash.
			*/
			char zOut[MAX_PATH+1];
			memset(zOut, 0, MAX_PATH+1);
			cygwin_conv_path(CCP_POSIX_TO_WIN_A|CCP_RELATIVE, zRelative, zOut,
				MAX_PATH+1);
			sqlite3_snprintf(MIN(nFull, pVfs->mxPathname), zFull, "%s\\%s",
				sqlite3_data_directory, zOut);
		}else{
			cygwin_conv_path(CCP_POSIX_TO_WIN_A, zRelative, zFull, nFull);
		}
		return SQLITE_OK;
#endif

#if (SQLITE_OS_WINCE || SQLITE_OS_WINRT) && !defined(__CYGWIN__)
		SimulateIOError( return SQLITE_ERROR );
		/* WinCE has no concept of a relative pathname, or so I am told. */
		/* WinRT has no way to convert a relative path to an absolute one. */
		if ( sqlite3_data_directory && !winIsVerbatimPathname(zRelative) ){
			/*
			** NOTE: We are dealing with a relative path name and the data
			**       directory has been set.  Therefore, use it as the basis
			**       for converting the relative path name to an absolute
			**       one by prepending the data directory and a backslash.
			*/
			sqlite3_snprintf(MIN(nFull, pVfs->mxPathname), zFull, "%s\\%s",
				sqlite3_data_directory, zRelative);
		}else{
			sqlite3_snprintf(MIN(nFull, pVfs->mxPathname), zFull, "%s", zRelative);
		}
		return SQLITE_OK;
#endif

#if !SQLITE_OS_WINCE && !SQLITE_OS_WINRT && !defined(__CYGWIN__)
		DWORD nByte;
		void *zConverted;
		char *zOut;

		/* If this path name begins with "/X:", where "X" is any alphabetic
		** character, discard the initial "/" from the pathname.
		*/
		if( zRelative[0]=='/' && sqlite3Isalpha(zRelative[1]) && zRelative[2]==':' ){
			zRelative++;
		}

		/* It's odd to simulate an io-error here, but really this is just
		** using the io-error infrastructure to test that SQLite handles this
		** function failing. This function could fail if, for example, the
		** current working directory has been unlinked.
		*/
		SimulateIOError( return SQLITE_ERROR );
		if ( sqlite3_data_directory && !winIsVerbatimPathname(zRelative) ){
			/*
			** NOTE: We are dealing with a relative path name and the data
			**       directory has been set.  Therefore, use it as the basis
			**       for converting the relative path name to an absolute
			**       one by prepending the data directory and a backslash.
			*/
			sqlite3_snprintf(MIN(nFull, pVfs->mxPathname), zFull, "%s\\%s",
				sqlite3_data_directory, zRelative);
			return SQLITE_OK;
		}
		zConverted = convertUtf8Filename(zRelative);
		if( zConverted==0 ){
			return SQLITE_IOERR_NOMEM;
		}
		if( isNT() ){
			LPWSTR zTemp;
			nByte = osGetFullPathNameW((LPCWSTR)zConverted, 0, 0, 0);
			if( nByte==0 ){
				winLogError(SQLITE_ERROR, osGetLastError(),
					"GetFullPathNameW1", zConverted);
				sqlite3_free(zConverted);
				return SQLITE_CANTOPEN_FULLPATH;
			}
			nByte += 3;
			zTemp = sqlite3MallocZero( nByte*sizeof(zTemp[0]) );
			if( zTemp==0 ){
				sqlite3_free(zConverted);
				return SQLITE_IOERR_NOMEM;
			}
			nByte = osGetFullPathNameW((LPCWSTR)zConverted, nByte, zTemp, 0);
			if( nByte==0 ){
				winLogError(SQLITE_ERROR, osGetLastError(),
					"GetFullPathNameW2", zConverted);
				sqlite3_free(zConverted);
				sqlite3_free(zTemp);
				return SQLITE_CANTOPEN_FULLPATH;
			}
			sqlite3_free(zConverted);
			zOut = unicodeToUtf8(zTemp);
			sqlite3_free(zTemp);
		}
#ifdef SQLITE_WIN32_HAS_ANSI
		else{
			char *zTemp;
			nByte = osGetFullPathNameA((char*)zConverted, 0, 0, 0);
			if( nByte==0 ){
				winLogError(SQLITE_ERROR, osGetLastError(),
					"GetFullPathNameA1", zConverted);
				sqlite3_free(zConverted);
				return SQLITE_CANTOPEN_FULLPATH;
			}
			nByte += 3;
			zTemp = sqlite3MallocZero( nByte*sizeof(zTemp[0]) );
			if( zTemp==0 ){
				sqlite3_free(zConverted);
				return SQLITE_IOERR_NOMEM;
			}
			nByte = osGetFullPathNameA((char*)zConverted, nByte, zTemp, 0);
			if( nByte==0 ){
				winLogError(SQLITE_ERROR, osGetLastError(),
					"GetFullPathNameA2", zConverted);
				sqlite3_free(zConverted);
				sqlite3_free(zTemp);
				return SQLITE_CANTOPEN_FULLPATH;
			}
			sqlite3_free(zConverted);
			zOut = sqlite3_win32_mbcs_to_utf8(zTemp);
			sqlite3_free(zTemp);
		}
#endif
		if( zOut ){
			sqlite3_snprintf(MIN(nFull, pVfs->mxPathname), zFull, "%s", zOut);
			sqlite3_free(zOut);
			return SQLITE_OK;
		}else{
			return SQLITE_IOERR_NOMEM;
		}
#endif
	}

#ifndef OMIT_LOAD_EXTENSION

	static void *CoreVFileSystem::DllOpen(const char *filename)
	{
		void *converted = convertUtf8Filename(filename);
		if (!converted)
			return nullptr;
		HANDLE h;
		if (isNT())
#if OS_WINRT
			h = osLoadPackagedLibrary((LPCWSTR)converted, 0);
#else
			h = osLoadLibraryW((LPCWSTR)converted);
#endif
#ifdef WIN32_HAS_ANSI
		else
			h = osLoadLibraryA((char *)converted);
#endif
		SysEx::Free(converted);
		return (void*)h;
	}

	static void CoreVFileSystem::DllError(int bufLength, char *bufOut)
	{
		getLastErrorMsg(osGetLastError(), bufLength, bufOut);
	}

	static void (*DllSym(void *handle, const char *sym))(void)
	{
		return (void(*)(void))osGetProcAddressA((HANDLE)h, sym);
	}

	static void DllClose(void *handle)
	{
		osFreeLibrary((HANDLE)pHandle);
	}

#else
#define winDlOpen  0
#define winDlError 0
#define winDlSym   0
#define winDlClose 0
#endif




}}

