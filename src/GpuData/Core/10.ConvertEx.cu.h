﻿namespace Core
{
	class ConvertEx
	{
	public:
		__device__ static int PutVariant(unsigned char *p, uint64 v);
		__device__ static int PutVariant4(unsigned char *p, uint32 v);
		__device__ static uint8 GetVariant(const unsigned char *p, uint64 *v);
		__device__ static uint8 GetVariant4(const unsigned char *p, uint32 *v);
		__device__ static int GetVariantLength(uint64 v);
		__device__ inline static uint32 Get4(const uint8 *p) { return (p[0]<<24) | (p[1]<<16) | (p[2]<<8) | p[3]; }
		__device__ inline static void Put4(unsigned char *p, uint32 v)
		{
			p[0] = (uint8)(v>>24);
			p[1] = (uint8)(v>>16);
			p[2] = (uint8)(v>>8);
			p[3] = (uint8)v;
		}
	};
}