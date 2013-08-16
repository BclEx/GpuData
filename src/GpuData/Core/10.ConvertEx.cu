// util.c
#include "Core.cu.h"

namespace Core
{
#define SLOT_2_0     0x001fc07f
#define SLOT_4_2_0   0xf01fc07f

#define getVarint4(A,B) \
	(uint8)((*(A)<(uint8)0x80)?((B)=(uint32)*(A)),1:\
	ConvertEx::GetVarint4((A),(u32 *)&(B)))
#define putVarint4(A,B) \
	(uint8)(((uint32)(B)<(uint32)0x80)?(*(A)=(unsigned char)(B)),1:\
	ConvertEx::PutVarint4((A),(B)))
#define getVarint ConvertEx::GetVarint
#define putVarint ConvertEx::PutVarint

	__device__ int ConvertEx::PutVarint(unsigned char *p, uint64 v)
	{
		int i, j, n;
		if (v & (((uint64)0xff000000) << 32))
		{
			p[8] = (uint8)v;
			v >>= 8;
			for (i = 7; i >= 0; i--)
			{
				p[i] = (uint8)((v & 0x7f) | 0x80);
				v >>= 7;
			}
			return 9;
		}    
		n = 0;
		uint8 b[10];
		do
		{
			b[n++] = (uint8)((v & 0x7f) | 0x80);
			v >>= 7;
		} while (v != 0);
		b[0] &= 0x7f;
		_assert(n <= 9);
		for (i = 0, j = n - 1; j >= 0; j--, i++)
			p[i] = b[j];
		return n;
	}

	__device__ int ConvertEx::PutVarint4(unsigned char *p, uint32 v)
	{
		if ((v & ~0x3fff) == 0)
		{
			p[0] = (uint8)((v>>7) | 0x80);
			p[1] = (uint8)(v & 0x7f);
			return 2;
		}
		return PutVarint(p, v);
	}

	__device__ uint8 ConvertEx::GetVarint(const unsigned char *p, uint64 *v)
	{
		uint32 a, b, s;
		a = *p;
		// a: p0 (unmasked)
		if (!(a & 0x80))
		{
			*v = a;
			return 1;
		}
		p++;
		b = *p;
		// b: p1 (unmasked)
		if (!(b & 0x80))
		{
			a &= 0x7f;
			a = a << 7;
			a |= b;
			*v = a;
			return 2;
		}
		// Verify that constants are precomputed correctly
		_assert(SLOT_2_0 == ((0x7f << 14) | 0x7f));
		_assert(SLOT_4_2_0 == ((0xfU << 28) | (0x7f << 14) | 0x7f));
		p++;
		a = a << 14;
		a |= *p;
		// a: p0<<14 | p2 (unmasked)
		if (!(a & 0x80))
		{
			a &= SLOT_2_0;
			b &= 0x7f;
			b = b << 7;
			a |= b;
			*v = a;
			return 3;
		}
		// CSE1 from below
		a &= SLOT_2_0;
		p++;
		b = b << 14;
		b |= *p;
		// b: p1<<14 | p3 (unmasked)
		if (!(b & 0x80))
		{
			b &= SLOT_2_0;
			// moved CSE1 up
			// a &= (0x7f<<14)|(0x7f);
			a = a << 7;
			a |= b;
			*v = a;
			return 4;
		}
		// a: p0<<14 | p2 (masked)
		// b: p1<<14 | p3 (unmasked)
		// 1:save off p0<<21 | p1<<14 | p2<<7 | p3 (masked)
		// moved CSE1 up
		// a &= (0x7f<<14)|(0x7f);
		b &= SLOT_2_0;
		s = a;
		// s: p0<<14 | p2 (masked)
		p++;
		a = a << 14;
		a |= *p;
		// a: p0<<28 | p2<<14 | p4 (unmasked)
		if (!(a & 0x80))
		{
			// we can skip these cause they were (effectively) done above in calc'ing s
			// a &= (0x7f<<28)|(0x7f<<14)|0x7f;
			// b &= (0x7f<<14)|0x7f;
			b = b << 7;
			a |= b;
			s = s >> 18;
			*v = ((uint64)s) << 32 | a;
			return 5;
		}
		// 2:save off p0<<21 | p1<<14 | p2<<7 | p3 (masked)
		s = s << 7;
		s |= b;
		// s: p0<<21 | p1<<14 | p2<<7 | p3 (masked)
		p++;
		b = b << 14;
		b |= *p;
		/* b: p1<<28 | p3<<14 | p5 (unmasked) */
		if (!(b & 0x80))
		{
			// we can skip this cause it was (effectively) done above in calc'ing s
			// b &= (0x7f<<28)|(0x7f<<14)|0x7f;
			a &= SLOT_2_0;
			a = a << 7;
			a |= b;
			s = s >> 18;
			*v = ((uint64)s) << 32 | a;
			return 6;
		}
		p++;
		a = a << 14;
		a |= *p;
		// a: p2<<28 | p4<<14 | p6 (unmasked)
		if (!(a & 0x80))
		{
			a &= SLOT_4_2_0;
			b &= SLOT_2_0;
			b = b << 7;
			a |= b;
			s = s>>11;
			*v = ((uint64)s) << 32 | a;
			return 7;
		}
		// CSE2 from below
		a &= SLOT_2_0;
		p++;
		b = b << 14;
		b |= *p;
		// b: p3<<28 | p5<<14 | p7 (unmasked)
		if (!(b & 0x80))
		{
			b &= SLOT_4_2_0;
			// moved CSE2 up
			// a &= (0x7f<<14)|0x7f;
			a = a << 7;
			a |= b;
			s = s >> 4;
			*v = ((uint64)s) << 32 | a;
			return 8;
		}
		p++;
		a = a << 15;
		a |= *p;
		// a: p4<<29 | p6<<15 | p8 (unmasked)
		// moved CSE2 up
		// a &= (0x7f<<29)|(0x7f<<15)|(0xff);
		b &= SLOT_2_0;
		b = b << 8;
		a |= b;
		s = s << 4;
		b = p[-4];
		b &= 0x7f;
		b = b >> 3;
		s |= b;
		*v = ((uint64)s) << 32 | a;
		return 9;
	}

	__device__ uint8 ConvertEx::GetVarint4(const unsigned char *p, uint32 *v)
	{
		uint32 a, b;
		// The 1-byte case.  Overwhelmingly the most common.  Handled inline by the getVarin32() macro
		a = *p;
		// a: p0 (unmasked)
		// The 2-byte case
		p++;
		b = *p;
		// b: p1 (unmasked)
		if (!(b & 0x80))
		{
			// Values between 128 and 16383
			a &= 0x7f;
			a = a << 7;
			*v = a | b;
			return 2;
		}
		// The 3-byte case
		p++;
		a = a << 14;
		a |= *p;
		// a: p0<<14 | p2 (unmasked)
		if (!(a & 0x80))
		{
			// Values between 16384 and 2097151
			a &= (0x7f << 14) | 0x7f;
			b &= 0x7f;
			b = b << 7;
			*v = a | b;
			return 3;
		}

		// A 32-bit varint is used to store size information in btrees. Objects are rarely larger than 2MiB limit of a 3-byte varint.
		// A 3-byte varint is sufficient, for example, to record the size of a 1048569-byte BLOB or string.
		// We only unroll the first 1-, 2-, and 3- byte cases.  The very rare larger cases can be handled by the slower 64-bit varint routine.
#if 1
		{
			p -= 2;
			uint64 v64;
			uint8 n = GetVarint(p, &v64);
			_assert(n > 3 && n <= 9);
			*v = ((v64 & MAX_TYPE(uint32)) != v64 ? 0xffffffff : (uint32)v64);
			return n;
		}

#else
		// For following code (kept for historical record only) shows an unrolling for the 3- and 4-byte varint cases.  This code is
		// slightly faster, but it is also larger and much harder to test.
		p++;
		b = b << 14;
		b |= *p;
		// b: p1<<14 | p3 (unmasked)
		if (!(b & 0x80))
		{
			// Values between 2097152 and 268435455
			b &= (0x7f << 14) | 0x7f;
			a &= (0x7f << 14) | 0x7f;
			a = a << 7;
			*v = a | b;
			return 4;
		}
		p++;
		a = a << 14;
		a |= *p;
		// a: p0<<28 | p2<<14 | p4 (unmasked)
		if (!(a & 0x80))
		{
			// Values  between 268435456 and 34359738367
			a &= SLOT_4_2_0;
			b &= SLOT_4_2_0;
			b = b << 7;
			*v = a | b;
			return 5;
		}
		// We can only reach this point when reading a corrupt database file.  In that case we are not in any hurry.  Use the (relatively
		// slow) general-purpose sqlite3GetVarint() routine to extract the value.
		{
			p -= 4;
			uint64 v64;
			uint8 n = GetVarint(p, &v64);
			assert(n > 5 && n <= 9);
			*v = (uint32)v64;
			return n;
		}
#endif
	}

	__device__ int ConvertEx::GetVarintLength(uint64 v)
	{
		int i = 0;
		do { i++; v >>= 7; }
		while (v != 0 && SysEx_ALWAYS(i < 9));
		return i;
	}
}
