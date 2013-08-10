namespace Core
{
	class ConvertEx
	{
	public:
		__device__ static int PutVarint(unsigned char *p, uint64 v);
		__device__ static int PutVarint4(unsigned char *p, uint32 v);
		__device__ static uint8 GetVarint(const unsigned char *p, uint64 *v);
		__device__ static uint8 GetVarint4(const unsigned char *p, uint32 *v);
		__device__ static int GetVarintLength(uint64 v);
		__device__ inline static uint16 Get2nz(const uint8 *p) { return ((( (int)((p[0]<<8) | p[1]) -1)&0xffff)+1); }
		__device__ inline static uint16 Get2(const uint8 *p) { return (p[0]<<8) | p[1]; }
		__device__ inline static void Put2(unsigned char *p, uint32 v)
		{
			p[0] = (uint8)(v>>8);
			p[1] = (uint8)v;
		}
		__device__ inline static uint32 Get4(const uint8 *p) { return (p[0]<<24) | (p[1]<<16) | (p[2]<<8) | p[3]; }
		__device__ inline static void Put4(unsigned char *p, uint32 v)
		{
			p[0] = (uint8)(v>>24);
			p[1] = (uint8)(v>>16);
			p[2] = (uint8)(v>>8);
			p[3] = (uint8)v;
		}

		__device__ inline static bool IsAlpha(unsigned char c) { return (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z'); }
	};
}
