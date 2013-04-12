﻿namespace Core
{
#define SLOT_2_0     0x001fc07f
#define SLOT_4_2_0   0xf01fc07f




int sqlite3PutVarint(unsigned char*, u64);
int sqlite3PutVarint32(unsigned char*, u32);
u8 sqlite3GetVarint(const unsigned char *, u64 *);
u8 sqlite3GetVarint32(const unsigned char *, u32 *);
int sqlite3VarintLen(u64 v);

#define getVarint32(A,B) \
  (u8)((*(A)<(u8)0x80)?((B)=(u32)*(A)),1:\
  ConvertEx::GetVarint32((A),(u32 *)&(B)))
#define putVarint32(A,B) \
  (u8)(((u32)(B)<(u32)0x80)?(*(A)=(unsigned char)(B)),1:\
  ConvertEx::PutVarint32((A),(B)))
#define getVarint ConvertEx::GetVarint
#define putVarint ConvertEx::PutVarint


	int ConvertEx::PutVariant(unsigned char *p, u64 v)
	{
		int i, j, n;
		if (v & (((u64)0xff000000) << 32))
		{
			p[8] = (u8)v;
			v >>= 8;
			for (i = 7; i >= 0; i--)
			{
				p[i] = (u8)((v & 0x7f) | 0x80);
				v >>= 7;
			}
			return 9;
		}    
		n = 0;
		u8 b[10];
		do
		{
			b[n++] = (u8)((v & 0x7f) | 0x80);
			v >>= 7;
		} while (v != 0);
		b[0] &= 0x7f;
		assert(n <= 9);
		for (i = 0, j = n - 1; j >= 0; j--, i++)
			p[i] = b[j];
		return n;
	}

	int ConvertEx::PutVariant32(unsigned char *p, u32 v)
	{
		if ((v & ~0x3fff) == 0)
		{
			p[0] = (u8)((v>>7) | 0x80);
			p[1] = (u8)(v & 0x7f);
			return 2;
		}
		return PutVariant(p, v);
	}

	u8 ConvertEx::GetVariant(const unsigned char *p, u64 *v)
	{
		u32 a, b, s;
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
		assert(SLOT_2_0 == ((0x7f << 14) | 0x7f));
		assert(SLOT_4_2_0 == ((0xfU << 28) | (0x7f << 14) | 0x7f));
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
			*v = ((u64)s) << 32 | a;
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
			*v = ((u64)s) << 32 | a;
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
			*v = ((u64)s) << 32 | a;
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
			*v = ((u64)s) << 32 | a;
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
		s |= b
			*v = ((u64)s) << 32 | a;
		return 9;
	}

	u8 ConvertEx::GetVariant32(const unsigned char *p, u32 *v)
	{
		u32 a, b;
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
			u64 v64;
			u8 n = GetVariant(p, &v64);
			assert(n > 3 && n <= 9);
			*v = ((v64 & MAX_U32) != v64 ? 0xffffffff : (u32)v64);
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
			u64 v64;
			u8 n = GetVarint(p, &v64);
			assert(n > 5 && n <= 9);
			*v = (u32)v64;
			return n;
		}
#endif
	}

	int ConvertEx::GetVariantLength(u64 v)
	{
		int i = 0;
		do { i++; v >>= 7; }
		while (v != 0 && SysEx::ALWAYS(i < 9));
		return i;
	}

	u32 ConvertEx::Get32(const u8 *p) { return (p[0]<<24) | (p[1]<<16) | (p[2]<<8) | p[3]; }
	void ConvertEx::Put32(unsigned char *p, u32 v)
	{
		p[0] = (u8)(v>>24);
		p[1] = (u8)(v>>16);
		p[2] = (u8)(v>>8);
		p[3] = (u8)v;
	}
}
