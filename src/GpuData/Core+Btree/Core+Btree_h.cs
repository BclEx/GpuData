using System;
namespace Core
{
    public delegate bool RefAction<T>(ref T value);

    #region IVdbe

    public interface IVdbe
    {
        Btree.UnpackedRecord RecordUnpack(Btree.KeyInfo keyInfo, int keyLength, byte[] key, Btree.UnpackedRecord space, int count);
        void DeleteUnpackedRecord(Btree.UnpackedRecord r);
        RC RecordCompare(int cells, byte[] cellKey, Btree.UnpackedRecord idxKey);
        RC RecordCompare(int cells, byte[] cellKey, int offset, Btree.UnpackedRecord idxKey);
    }

    #endregion

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
    public enum SCHEMA : byte
    {
        SchemaLoaded = 0x0001, // The schema has been loaded
        UnresetViews = 0x0002, // Some views have defined column names
        Empty = 0x0004, // The file is empty (length 0 bytes)
    }

    public class ISchema
    {
        public byte FileFormat;
        public SCHEMA Flags;
    }

    #endregion
}