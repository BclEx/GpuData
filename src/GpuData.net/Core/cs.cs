using System;

namespace Core
{
    public static class cs
    {
        public static int memcmp(byte[] a, byte[] b, int limit)
        {
            if (a.Length < limit)
                return (a.Length < b.Length ? -1 : +1);
            if (b.Length < limit)
                return +1;
            for (int i = 0; i < limit; i++)
            {
                if (a[i] != b[i])
                    return (a[i] < b[i] ? -1 : 1);
            }
            return 0;
        }
        public static int memcmp(string a, byte[] b, int limit)
        {
            if (a.Length < limit)
                return (a.Length < b.Length ? -1 : +1);
            if (b.Length < limit)
                return +1;
            char[] cA = a.ToCharArray();
            for (int i = 0; i < limit; i++)
            {
                if (cA[i] != b[i])
                    return (cA[i] < b[i] ? -1 : 1);
            }
            return 0;
        }
        public static int memcmp(byte[] a, int aOffset, byte[] b, int limit)
        {
            if (a.Length < aOffset + limit)
                return (a.Length - aOffset < b.Length ? -1 : +1);
            if (b.Length < limit)
                return +1;
            for (int i = 0; i < limit; i++)
            {
                if (a[i + aOffset] != b[i])
                    return (a[i + aOffset] < b[i] ? -1 : 1);
            }
            return 0;
        }
        public static int memcmp(byte[] a, int aOffset, byte[] b, int bOffset, int limit)
        {
            if (a.Length < aOffset + limit)
                return (a.Length - aOffset < b.Length - bOffset ? -1 : +1);
            if (b.Length < bOffset + limit)
                return +1;
            for (int i = 0; i < limit; i++)
            {
                if (a[i + aOffset] != b[i + bOffset])
                    return (a[i + aOffset] < b[i + bOffset] ? -1 : 1);
            }
            return 0;
        }
        public static int memcmp(byte[] a, int aOffset, string b, int limit)
        {
            if (a.Length < aOffset + limit)
                return (a.Length - aOffset < b.Length ? -1 : +1);
            if (b.Length < limit)
                return +1;
            for (int i = 0; i < limit; i++)
            {
                if (a[i + aOffset] != b[i])
                    return (a[i + aOffset] < b[i] ? -1 : 1);
            }
            return 0;
        }
        public static int memcmp(string a, string b, int limit)
        {
            if (a.Length < limit)
                return (a.Length < b.Length ? -1 : +1);
            if (b.Length < limit)
                return +1;
            int rc;
            if ((rc = String.Compare(a, 0, b, 0, limit, StringComparison.Ordinal)) == 0)
                return 0;
            return (rc < 0 ? -1 : +1);
        }
    }
}