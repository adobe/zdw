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

#ifndef STRINGHEAP_H
#define STRINGHEAP_H

#include <list>
#include <cstring>


class StringHeap
{
public:
	StringHeap()
		: freePtr(NULL)
		, freeBytesInCurrentBlock(0)
		, low_on_memory(false)
		{ }
	~StringHeap() { clear(); }

	void clear() { FreeMemory(); }

	char* copyToHeap(const char* str, const size_t len);

	bool is_low_on_memory() const { return low_on_memory; }

private:
	void allocBlock(const size_t size);
	void FreeMemory();

	char* insert(const char* str, const size_t len);

	void flag_low_memory() { low_on_memory = true; }

	std::list<char*> blocks;
	char *freePtr;
	size_t freeBytesInCurrentBlock;

	bool low_on_memory;
};

#endif
