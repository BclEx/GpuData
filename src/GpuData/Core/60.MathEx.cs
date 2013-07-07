using System;
using System.Diagnostics;
namespace Core
{
    public class MathEx
    {
        static void ASSERTCOVERAGE(bool p) { }

        static bool AddInt64(ref long a, long b)
        {
            long a2 = a;
            ASSERTCOVERAGE(a2 == 0);
            ASSERTCOVERAGE(a2 == 1);
            ASSERTCOVERAGE(b == -1);
            ASSERTCOVERAGE(b == 0);
            if (b >= 0)
            {
                ASSERTCOVERAGE(a2 > 0 && long.MaxValue - a2 == b);
                ASSERTCOVERAGE(a2 > 0 && long.MaxValue - a2 == b - 1);
                if (a2 > 0 && long.MaxValue - a2 < b) return true;
                a += b;
            }
            else
            {
                ASSERTCOVERAGE(a2 < 0 && -(a2 + long.MaxValue) == b + 1);
                ASSERTCOVERAGE(a2 < 0 && -(a2 + long.MaxValue) == b + 2);
                if (a2 < 0 && -(a2 + long.MaxValue) > b + 1) return true;
                a += b;
            }
            return false;
        }
        static bool SubInt64(ref long a, long b)
        {
            ASSERTCOVERAGE(b == long.MinValue + 1);
            if (b == long.MinValue)
            {
                ASSERTCOVERAGE((a) == (-1));
                ASSERTCOVERAGE((a) == 0);
                if ((a) >= 0) return true;
                a -= b;
                return false;
            }
            else
                return AddInt64(ref a, -b);
        }

        const long TWOPOWER32 = (((long)1) << 32);
        const long TWOPOWER31 = (((long)1) << 31);
        static bool sqlite3MulInt64(ref long a, long b)
        {
            long a2 = a;

            long iA1 = a2 / TWOPOWER32;
            long iA0 = a2 % TWOPOWER32;
            long iB1 = b / TWOPOWER32;
            long iB0 = b % TWOPOWER32;
            if (iA1 * iB1 != 0) return true;
            Debug.Assert(iA1 * iB0 == 0 || iA0 * iB1 == 0);
            long r = iA1 * iB0 + iA0 * iB1;
            ASSERTCOVERAGE(r == (-TWOPOWER31) - 1);
            ASSERTCOVERAGE(r == (-TWOPOWER31));
            ASSERTCOVERAGE(r == TWOPOWER31);
            ASSERTCOVERAGE(r == TWOPOWER31 - 1);
            if (r < (-TWOPOWER31) || r >= TWOPOWER31) return true;
            r *= TWOPOWER32;
            if (AddInt64(ref r, iA0 * iB0)) return true;
            a = r;
            return false;
        }

        static int Abs(int x)
        {
            if (x >= 0) return x;
            if (x == (int)0x8000000) return 0x7fffffff;
            return -x;
        }
    }
}