using System;
namespace Core
{
    public delegate bool RefAction<T>(ref T value);

    #region CollSeq

    public class CollSeq
    {
        public string Name;				// Name of the collating sequence, UTF-8 encoded
        public byte Enc;				// Text encoding handled by xCmp()
        public object User;				// First argument to xCmp()
        public Func<object, int, string, int, string, int> Cmp;
        public RefAction<object> Del;	// Destructor for pUser
        public CollSeq memcopy()
        {
            if (this == null)
                return null;
            else
            {
                var cp = (CollSeq)MemberwiseClone();
                return cp;
            }
        }
    }

    #endregion

    #region ISchema

    [Flags]
    public enum SCHEMA_ : byte
    {
        SchemaLoaded = 0x0001, // The schema has been loaded
        UnresetViews = 0x0002, // Some views have defined column names
        Empty = 0x0004, // The file is empty (length 0 bytes)
    }

    public class ISchema
    {
        public byte FileFormat;
        public SCHEMA_ Flags;
    }

    #endregion

    #region IVdbe

    public interface IVdbe
    {
        UnpackedRecord AllocUnpackedRecord(KeyInfo keyInfo, byte[] space, int spaceLength, out object free);
        void RecordUnpack(KeyInfo keyInfo, int keyLength, byte[] key, UnpackedRecord p);
        void DeleteUnpackedRecord(UnpackedRecord r);
        int RecordCompare(int cells, byte[] cellKey, uint offset_, UnpackedRecord idxKey);
    }

    #endregion
}