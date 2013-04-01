using Pid = System.UInt32;
namespace Core
{
    public interface IBackup
    {
        void sqlite3BackupUpdate(Pid id, byte[] data);
        void sqlite3BackupRestart();
    }
}