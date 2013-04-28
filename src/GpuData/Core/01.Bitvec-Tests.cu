//bitvec.c
#include "Core.cu.h"
using namespace Core;

namespace Core
{

#define SETBIT(V,I) V[I>>3] |= (1<<(I&7))
#define CLEARBIT(V,I) V[I>>3] &= ~(1<<(I&7))
#define TESTBIT(V,I) (V[I>>3]&(1<<(I&7)))!=0

	__device__ int Bitvec_BuiltinTest(int size, int *aOp)
	{
		int rc = -1;
		// Allocate the Bitvec to be tested and a linear array of bits to act as the reference
		Bitvec *bitvec = new Bitvec(size);
		unsigned char *v = (unsigned char *)SysEx::Alloc((size + 7) / 8 + 1, true);
		void *tmpSpace = SysEx::Alloc(BITVEC_SZ);
		if (!bitvec || !v || !tmpSpace)
			goto bitvec_end;

		// Run the program
		int pc = 0;
		int i, nx, op;
		while ((op = aOp[pc]))
		{
			switch (op)
			{
			case 1:
			case 2:
			case 5:
				{
					nx = 4;
					i = aOp[pc + 2] - 1;
					aOp[pc + 2] += aOp[pc + 3];
					break;
				}
			case 3:
			case 4: 
			default:
				{
					nx = 2;
					SysEx::SetRandom(sizeof(i), &i);
					break;
				}
			}
			if ((--aOp[pc + 1]) > 0) nx = 0;
			pc += nx;
			i = (i & 0x7fffffff) % size;
			if ((op & 1) !=0)
			{
				SETBIT(v, (i + 1));
				if (op != 5)
					if (bitvec->Set(i + 1)) goto bitvec_end;
			}
			else
			{
				CLEARBIT(v, (i + 1));
				bitvec->Clear(i + 1, tmpSpace);
			}
		}

		// Test to make sure the linear array exactly matches the Bitvec object.  Start with the assumption that they do
		// match (rc==0).  Change rc to non-zero if a discrepancy is found.
		rc = bitvec->Get(size + 1)
			+ bitvec->Get(0)
			+ (bitvec->get_Length() - size);
		for (i = 1; i <= size; i++)
		{
			if ((TESTBIT(v,i)) != bitvec->Get(i))
			{
				rc = i;
				break;
			}
		}

		// Free allocated structure
bitvec_end:
		SysEx::Free(tmpSpace);
		SysEx::Free(v);
		Bitvec::Destroy(bitvec);
		return rc;
	}
}
