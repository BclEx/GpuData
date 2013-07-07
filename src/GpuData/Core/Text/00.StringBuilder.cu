﻿#include "../Core.cu.h"

namespace Core { namespace Text
{
	void StringBuilder::Append(const char *z, int length)
	{
		_assert(z != nullptr || length == 0);
		if (Overflowed | MallocFailed)
		{
			ASSERTCOVERAGE(Overflowed);
			ASSERTCOVERAGE(MallocFailed);
			return;
		}
		_assert(Text != nullptr || Index == 0);
		if (length < 0)
			length = _strlen(z);
		if (length == 0 || SysEx_NEVER(z == nullptr))
			return;
		if (Index + length >= Size)
		{
			char *newText;
			if (!UseMalloc)
			{
				Overflowed = true;
				length = Size - Index - 1;
				if (length <= 0)
					return;
			}
			else
			{
				char *oldText = (Text == Base ? nullptr : Text);
				int64 newSize = Index;
				newSize += length + 1;
				if (newSize > MaxSize)
				{
					Reset();
					Overflowed = true;
					return;
				}
				else
					Size = (int)newSize;
				if (UseMalloc == 1)
					newText = SysEx::TagRealloc(Ctx, oldText, Size);
				else
					newText = SysEx::Realloc(oldText, Size);
				if (newText)
				{
					if (oldText == nullptr && Index > 0) _memcpy(newText, Text, Index);
					Text = newText;
				}
				else
				{
					Reset();
					MallocFailed = true;
					return;
				}
			}
		}
		_assert(Text != nullptr);
		_memcpy(&Text[Index], z, length);
		Index += length;
	}

	char *StringBuilder::ToString()
	{
		if (Text)
		{
			Text[Index] = 0;
			if (UseMalloc && Text == Base)
			{
				if (UseMalloc == 1)
					Text = SysEx::TagAlloc(Ctx, Index + 1);
				else
					Text = SysEx::Alloc(Index + 1);
				if (Text)
					_memcpy(Text, Base, Index + 1);
				else
					MallocFailed = true;
			}
		}
		return Text;
	}

	void StringBuilder::Reset()
	{
		if (Text != Base)
		{
			if (UseMalloc == 1)
				SysEx::TagFree(p->Ctx, p->Text);
			else
				SysEx::Free(p->Text);
		}
		Text = nullptr;
	}

	void StringBuilder::Init(StringBuilder *b, char *text, int capacity, int maxSize)
	{
		b->Text = b->Base = text;
		b->Ctx = nullptr;
		b->Index = 0;
		b->Size = capacity;
		b->MaxSize = maxSize;
		b->UseMalloc = 1;
		b->Overflowed = false;
		b->MallocFailed = false;
	}

}}