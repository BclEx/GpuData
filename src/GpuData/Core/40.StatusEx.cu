﻿// status.c
#include "Core.cu.h"
using namespace Core;

namespace Core
{
	typedef struct StatType StatType;
	static struct StatType
	{
		int nowValue[10];         // Current value
		int mxValue[10];          // Maximum value
	} Stat = { {0,}, {0,} };

	int StatusEx::StatusValue(StatusEx::STATUS op)
	{
		_assert(op >= 0 && op < _static_arraylength(Stat.nowValue));
		return Stat.nowValue[op];
	}

	void StatusEx::StatusAdd(StatusEx::STATUS op, int N)
	{
		_assert(op >= 0 && op < _static_arraylength(Stat.nowValue));
		Stat.nowValue[op] += N;
		if (Stat.nowValue[op] > Stat.mxValue[op])
			Stat.mxValue[op] = Stat.nowValue[op];
	}

	void StatusEx::StatusSet(StatusEx::STATUS op, int X)
	{
		_assert(op >= 0 && op < _static_arraylength(Stat.nowValue));
		Stat.nowValue[op] = X;
		if (Stat.nowValue[op] > Stat.mxValue[op])
			Stat.mxValue[op] = Stat.nowValue[op];
	}

	int StatusEx::Status(StatusEx::STATUS op, int *current, int *highwater, int resetFlag)
	{
		if (op < 0 || op >= _static_arraylength(Stat.nowValue))
			return SysEx_MISUSE_BKPT;
		*current = Stat.nowValue[op];
		*highwater = Stat.mxValue[op];
		if (resetFlag)
			Stat.mxValue[op] = Stat.nowValue[op];
		return RC::OK;
	}
}
