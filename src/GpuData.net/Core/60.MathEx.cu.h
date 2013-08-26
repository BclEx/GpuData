namespace Core
{
	class MathEx
	{
	public:
		__device__ inline static bool AddInt64(int64 *a, int64 b)
		{
			int64 a2 = *a;
			ASSERTCOVERAGE(a2 == 0); ASSERTCOVERAGE(a2 == 1);
			ASSERTCOVERAGE(b == -1); ASSERTCOVERAGE(b == 0);
			if (b >= 0)
			{
				ASSERTCOVERAGE(a2 > 0 && MAX_TYPE(int64) - a2 == b);
				ASSERTCOVERAGE(a2 > 0 && MAX_TYPE(int64) - a2 == b - 1 );
				if (a2 > 0 && MAX_TYPE(int64) - a2 < b) return true;
				*a += b;
			}
			else
			{
				ASSERTCOVERAGE(a2 < 0 && -(a2 + MAX_TYPE(int64)) == b + 1);
				ASSERTCOVERAGE(a2 < 0 && -(a2 + MAX_TYPE(int64)) == b + 2);
				if (a2 < 0 && -(a2 + MAX_TYPE(int64)) > b + 1) return true;
				*a += b;
			}
			return false;
		}

		__device__ inline static bool SubInt64(int64 *a, int64 b)
		{
			ASSERTCOVERAGE(b == MIN_TYPE(int64) + 1);
			if (b == MIN_TYPE(int64))
			{
				ASSERTCOVERAGE(*a == -1); ASSERTCOVERAGE(*a == 0);
				if (*a >= 0) return true;
				*a -= b;
				return false;
			}
			else
				return AddInt64(a, -b);
		}
#define TWOPOWER32 (((int64)1) << 32)
#define TWOPOWER31 (((int64)1) << 31)
		__device__ inline static bool MulInt64(int64 *a, int64 b)
		{
			int64 a2 = *a;

			int64 iA1 = a2 / TWOPOWER32;
			int64 iA0 = a2 % TWOPOWER32;
			int64 iB1 = b / TWOPOWER32;
			int64 iB0 = b % TWOPOWER32;
			if (iA1 * iB1 != 0) return true;
			_assert(iA1 * iB0 == 0 || iA0 * iB1 == 0);
			int64 r = iA1 * iB0 + iA0 * iB1;
			ASSERTCOVERAGE(r == (-TWOPOWER31) - 1);
			ASSERTCOVERAGE(r == (-TWOPOWER31));
			ASSERTCOVERAGE(r == TWOPOWER31);
			ASSERTCOVERAGE(r == TWOPOWER31 - 1);
			if (r < (-TWOPOWER31) || r >= TWOPOWER31) return true;
			r *= TWOPOWER32;
			if (AddInt64(&r, iA0 * iB0)) return true;
			*a = r;
			return false;
		}

		__device__ inline static int Abs(int x)
		{
			if (x >= 0) return x;
			if (x == (int)0x8000000) return 0x7fffffff;
			return -x;
		}
	};
}
