/**
 * Copyright 2019 Adobe. All rights reserved.
 * This file is licensed to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License. You may obtain a copy
 * of the License at http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under
 * the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR REPRESENTATIONS
 * OF ANY KIND, either express or implied. See the License for the specific language
 * governing permissions and limitations under the License.
 */

#include "dictionary.h"
#include "memory.h"
#include <cassert>

void Dictionary::clear()
{
	stringOffsets.clear();
	stringHeap.clear();
	size = 0;
}

//Returns: true if additional memory is available, or false if memory limit has been exceeded
bool Dictionary::insert(const char* str)
{
	std::pair<DictionaryT::iterator, bool> ret =
		stringOffsets.insert(std::pair<const char*, int unsigned>(str, 0));

	if (ret.second) {
		const size_t len = strlen(str) + 1; //include null terminator
		char *heapStr = this->stringHeap.copyToHeap(str, len);
		this->size += len;

		//performance optimization: hack map with equivalent key, pointing to the heap string.
		// This avoids slower operations, e.g., a membership search before inserting into the heap,
		// or copying the string into the heap before attempting a map insert, then backing it out on map collision
		const char **strPtr = const_cast<const char**>(&(ret.first->first));
		*strPtr = heapStr;

		return !this->stringHeap.is_low_on_memory();
	}

	return true;
}

ULONG Dictionary::getOffset(const char* str) const
{
	DictionaryT::const_iterator it = stringOffsets.find(str);
	assert(it != stringOffsets.end());

	return it->second;
}

//Returns: byte size required to represent largest offset
ULONG Dictionary::getBytesInOffset() const
{
	ULONG indexSize = 1;
	ULONG maxIndex = getSize();
	while (maxIndex >= 256)
	{
		++indexSize;
		maxIndex /= 256;
	}

	return indexSize;
}

//Post-condition: stringOffsets has values populated
void Dictionary::write(FILE* f)
{
	static const char unsigned zero = 0;

	if (empty()) {
		//Write 0, indicating empty set.
		fwrite(&zero, 1, 1, f);
		return;
	}

	//Write bytes used to store an offset.
	const ULONG indexSize = getBytesInOffset();
	fwrite(&indexSize, 1, 1, f);

	//Write buffer size.
	const ULONG bufferSize = getSize();
	indexBytes val;
	val.n = bufferSize;
	fwrite(val.c, indexSize, 1, f);

	//origin byte offset: only non-zero indices are recognized in the unconverter, so start at 1
	fwrite(&zero, 1, 1, f);
	ULONG index = 1;

	//Populate offsets and dump keys.
	for (DictionaryT::iterator it = stringOffsets.begin(); it != stringOffsets.end(); ++it)
	{
		it->second = index;
		const char* str = it->first;
		const ULONG len = strlen(str) + 1; //include null terminator
		fwrite(str, len, 1, f);
		index += len;
	}

	assert(index == bufferSize);
}

