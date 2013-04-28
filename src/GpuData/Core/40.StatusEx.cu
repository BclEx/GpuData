// status.c
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
		wsdStatInit;
		_assert(op >= 0 && op < ArraySize(wsdStat.nowValue));
		return wsdStat.nowValue[op];
	}

	void StatusEx::StatusAdd(int op, int N)
	{
		wsdStatInit;
		_assert(op >= 0 && op < ArraySize(wsdStat.nowValue));
		wsdStat.nowValue[op] += N;
		if (wsdStat.nowValue[op]>wsdStat.mxValue[op])
			wsdStat.mxValue[op] = wsdStat.nowValue[op];
	}

	void StatusEx::StatusSet(int op, int X){
		wsdStatInit;
		_assert(op >= 0 && op < ArraySize(wsdStat.nowValue));
		wsdStat.nowValue[op] = X;
		if (wsdStat.nowValue[op]>wsdStat.mxValue[op])
			wsdStat.mxValue[op] = wsdStat.nowValue[op];
	}

	int StatusEx::Status(int op, int *pCurrent, int *pHighwater, int resetFlag)
	{
		wsdStatInit;
		if (op < 0 || op >= ArraySize(wsdStat.nowValue))
			return RC::MISUSE_BKPT;
		*pCurrent = wsdStat.nowValue[op];
		*pHighwater = wsdStat.mxValue[op];
		if (resetFlag)
			wsdStat.mxValue[op] = wsdStat.nowValue[op];
		return RC::OK;
	}


}
