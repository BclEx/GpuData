// sqlite.h
namespace Core
{
	class VFileSystem
	{
	public:
		VFileSystem *Next;	// Next registered VFS
		const char *Name;	// Name of this virtual file system
		static VFileSystem *Find(const char *name);
		static int RegisterVfs(VFileSystem *vfs, bool _default);
		static int UnregisterVfs(VFileSystem *vfs);
	};
}
