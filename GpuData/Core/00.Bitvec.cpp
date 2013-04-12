namespace Core
{

#define BITVEC_SZ 512
#define BITVEC_USIZE (((BITVEC_SZ - (3 * sizeof(u32))) / sizeof(Bitvec *)) * sizeof(Bitvec *))
#define BITVEC_SZELEM 8
#define BITVEC_NELEM (BITVEC_USIZE / sizeof(u8))
#define BITVEC_NBIT (BITVEC_NELEM * BITVEC_SZELEM)
#define BITVEC_NINT (BITVEC_USIZE / sizeof(uint))
#define BITVEC_MXHASH (BITVEC_NINT / 2)
#define BITVEC_HASH(X) (((X) * 1) % BITVEC_NINT)
#define BITVEC_NPTR (BITVEC_USIZE / sizeof(Bitvec *))

	class Bitvec {
	private:
		u32 _size;      // Maximum bit index.  Max iSize is 4,294,967,296.
		u32 _set;       // Number of bits that are set - only valid for aHash element.  Max is BITVEC_NINT.  For BITVEC_SZ of 512, this would be 125.
		u32 _divisor;   // Number of bits handled by each apSub[] entry.
		// Should >=0 for apSub element. */
		// Max iDivisor is max(u32) / BITVEC_NPTR + 1.
		// For a BITVEC_SZ of 512, this would be 34,359,739.
		union {
			u8 Bitmap[BITVEC_NELEM]; // Bitmap representation
			u32 Hash[BITVEC_NINT];		// Hash table representation
			Bitvec *Sub[BITVEC_NPTR];	// Recursive representation
		} u;
	};

	Bitvec::Bitvec(u32 size)
	{
		_size = size;
	}

	bool Bitvec::Get(u32 index)
	{
		if (index > _size || index == 0)
			return false;
		index--;
		Bitvec *p = this;
		while (p->_divisor)
		{
			u32 bin = index / p->_divisor;
			index %= p->_ivisor;
			p = p->u.Sub[bin];
			if (!p) return false;
		}
		if( p->_size <= BITVEC_NBIT)
			return ((p->u.Bitmap[index / BITVEC_SZELEM] & (1 << (index & (BITVEC_SZELEM - 1)))) != 0);
		u32 h = BITVEC_HASH(index++);
		while (p->u.Hash[h])
		{
			if (p->u.Hash[h] == index) return true;
			h = (h + 1) % BITVEC_NINT;
		}
		return false;
	}

	int Bitvec::Set(u32 index)
	{
		assert(index > 0);
		assert(index <= _size);
		index--;
		Bitvec *p = this;
		while ((p->_size > BITVEC_NBIT) && p->_divisor)
		{
			u32 bin = index / p->_divisor;
			index %= p->_divisor;
			if (!p->u.Sub[bin])
				if (!(p->u.Sub[bin] = new Bitvec(p->_divisor))) return RC::NOMEM;
			p = p->u.Sub[bin];
		}
		if (p->_size <= BITVEC_NBIT)
		{
			p->u.Bitmap[index / BITVEC_SZELEM] |= (1 << (index & (BITVEC_SZELEM - 1)));
			return RC::OK;
		}
		u32 h = BITVEC_HASH(i++);
		// if there wasn't a hash collision, and this doesn't completely fill the hash, then just add it without worring about sub-dividing and re-hashing.
		if (!p->u.Hash[h])

			if (p->_set < (BITVEC_NINT - 1))
				goto bitvec_set_end;
			else
				goto bitvec_set_rehash;
		// there was a collision, check to see if it's already in hash, if not, try to find a spot for it
		do
		{
			if (p->u.Hash[h] == index) return RC::OK;
			h++;
			if (h >= BITVEC_NINT) h = 0;
		} while (p->u.Hash[h]);
		// we didn't find it in the hash.  h points to the first available free spot. check to see if this is going to make our hash too "full".
bitvec_set_rehash:
		if (p->_set >= BITVEC_MXHASH)
		{
			;
			u32 *values;
			if (!(values = sqlite3StackAllocRaw(0, sizeof(p->u.Hash)))) return RC::NOMEM;
			memcpy(values, p->u.Hash, sizeof(p->u.Hash));
			memset(p->u.Sub, 0, sizeof(p->u.Sub));
			p->_divisor = ((p->_size + BITVEC_NPTR - 1) / BITVEC_NPTR);
			int rc = p.Set(index);
			for (unsigned int j = 0; j < BITVEC_NINT; j++)
				if (values[j]) rc |= p.Set(values[j]);
			sqlite3StackFree(0, values);
			return rc;
		}
bitvec_set_end:
		p->_set++;
		p->u.Hash[h] = i;
		return RC::OK;
	}

	void Bitvec::Clear(u32 index, void *buffer){
		assert(index > 0);
		index--;
		Bitvec *p = this;
		while (p->_divisor)
		{
			u32 bin = index / p->_divisor;
			index %= p->_divisor;
			p = p->u.Sub[bin];
			if (!p) return;
		}
		if (p->_size <= BITVEC_NBIT)
			p->u.aBitmap[index / BITVEC_SZELEM] &= ~(1 << (index & (BITVEC_SZELEM - 1)));
		else
		{
			u32 *values = buffer;
			memcpy(values, p->u.Hash, sizeof(p->u.Hash));
			memset(p->u.Hash, 0, sizeof(p->u.Hash));
			p->_set = 0;
			for (unsigned int j = 0; j < BITVEC_NINT; j++)
				if (values[j] && values[j] != (index + 1))
				{
					u32 h = BITVEC_HASH(values[j] - 1);
					p->_set++;
					while (p->u.Hash[h])
					{
						h++;
						if (h >= BITVEC_NINT) h = 0;
					}
					p->u.Hash[h] = values[j];
				}
		}
	}

	void Bitvec::Destroy(Bitvec *p)
	{
		if (!p)
			return;
		if (p->_divisor)
			for (unsigned int index = 0; index < BITVEC_NPTR; index++)
				Destroy(p->u.Sub[index]);
		SysEx::Free(p);
	}

	u32 Bitvec::get_Length() { return _size; }

#ifndef SQLITE_OMIT_BUILTIN_TEST
	/*
	** Let V[] be an array of unsigned characters sufficient to hold
	** up to N bits.  Let I be an integer between 0 and N.  0<=I<N.
	** Then the following macros can be used to set, clear, or test
	** individual bits within V.
	*/
#define SETBIT(V,I)      V[I>>3] |= (1<<(I&7))
#define CLEARBIT(V,I)    V[I>>3] &= ~(1<<(I&7))
#define TESTBIT(V,I)     (V[I>>3]&(1<<(I&7)))!=0

	/*
	** This routine runs an extensive test of the Bitvec code.
	**
	** The input is an array of integers that acts as a program
	** to test the Bitvec.  The integers are opcodes followed
	** by 0, 1, or 3 operands, depending on the opcode.  Another
	** opcode follows immediately after the last operand.
	**
	** There are 6 opcodes numbered from 0 through 5.  0 is the
	** "halt" opcode and causes the test to end.
	**
	**    0          Halt and return the number of errors
	**    1 N S X    Set N bits beginning with S and incrementing by X
	**    2 N S X    Clear N bits beginning with S and incrementing by X
	**    3 N        Set N randomly chosen bits
	**    4 N        Clear N randomly chosen bits
	**    5 N S X    Set N bits from S increment X in array only, not in bitvec
	**
	** The opcodes 1 through 4 perform set and clear operations are performed
	** on both a Bitvec object and on a linear array of bits obtained from malloc.
	** Opcode 5 works on the linear array only, not on the Bitvec.
	** Opcode 5 is used to deliberately induce a fault in order to
	** confirm that error detection works.
	**
	** At the conclusion of the test the linear array is compared
	** against the Bitvec object.  If there are any differences,
	** an error is returned.  If they are the same, zero is returned.
	**
	** If a memory allocation error occurs, return -1.
	*/
	int sqlite3BitvecBuiltinTest(int sz, int *aOp){
		Bitvec *pBitvec = 0;
		unsigned char *pV = 0;
		int rc = -1;
		int i, nx, pc, op;
		void *pTmpSpace;

		/* Allocate the Bitvec to be tested and a linear array of
		** bits to act as the reference */
		pBitvec = sqlite3BitvecCreate( sz );
		pV = sqlite3MallocZero( (sz+7)/8 + 1 );
		pTmpSpace = sqlite3_malloc(BITVEC_SZ);
		if( pBitvec==0 || pV==0 || pTmpSpace==0  ) goto bitvec_end;

		/* NULL pBitvec tests */
		sqlite3BitvecSet(0, 1);
		sqlite3BitvecClear(0, 1, pTmpSpace);

		/* Run the program */
		pc = 0;
		while( (op = aOp[pc])!=0 ){
			switch( op ){
			case 1:
			case 2:
			case 5: {
				nx = 4;
				i = aOp[pc+2] - 1;
				aOp[pc+2] += aOp[pc+3];
				break;
					}
			case 3:
			case 4: 
			default: {
				nx = 2;
				sqlite3_randomness(sizeof(i), &i);
				break;
					 }
			}
			if( (--aOp[pc+1]) > 0 ) nx = 0;
			pc += nx;
			i = (i & 0x7fffffff)%sz;
			if( (op & 1)!=0 ){
				SETBIT(pV, (i+1));
				if( op!=5 ){
					if( sqlite3BitvecSet(pBitvec, i+1) ) goto bitvec_end;
				}
			}else{
				CLEARBIT(pV, (i+1));
				sqlite3BitvecClear(pBitvec, i+1, pTmpSpace);
			}
		}

		/* Test to make sure the linear array exactly matches the
		** Bitvec object.  Start with the assumption that they do
		** match (rc==0).  Change rc to non-zero if a discrepancy
		** is found.
		*/
		rc = sqlite3BitvecTest(0,0) + sqlite3BitvecTest(pBitvec, sz+1)
			+ sqlite3BitvecTest(pBitvec, 0)
			+ (sqlite3BitvecSize(pBitvec) - sz);
		for(i=1; i<=sz; i++){
			if(  (TESTBIT(pV,i))!=sqlite3BitvecTest(pBitvec,i) ){
				rc = i;
				break;
			}
		}

		/* Free allocated structure */
bitvec_end:
		sqlite3_free(pTmpSpace);
		sqlite3_free(pV);
		sqlite3BitvecDestroy(pBitvec);
		return rc;
	}
#endif /* SQLITE_OMIT_BUILTIN_TEST */

}
