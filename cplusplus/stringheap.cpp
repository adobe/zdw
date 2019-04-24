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

#include "stringheap.h"
#include "memory.h"

#include <cassert>
#include <cstring>
#include <stdio.h>

const size_t HEAP_BLOCK_SIZE = 64*1024*1024;

char* StringHeap::copyToHeap(const char* str, const size_t len)
{
	//If we have space to allocate in the current block, use it.
	if (this->freeBytesInCurrentBlock >= len)
		return insert(str, len);

	//Allocate more memory for our heap.
	//Residual on previous block is wasted.
	try
	{
		const size_t bytes = len > HEAP_BLOCK_SIZE ? len : HEAP_BLOCK_SIZE;
		allocBlock(bytes);
		return insert(str, len);
	}
	catch(const std::bad_alloc&)
	{
		flag_low_memory();
		if (len < HEAP_BLOCK_SIZE) {
			try {
				allocBlock(len);
				return insert(str, len);
			}
			catch(const std::bad_alloc&) {
				throw;
			}
		}
		throw;
	}
}

char* StringHeap::insert(const char* str, const size_t len) //includes null terminator
{
	assert(this->freePtr);
	assert(this->freeBytesInCurrentBlock >= len);

	char *heapStr = this->freePtr;
	strcpy(heapStr, str);

	this->freePtr += len;
	this->freeBytesInCurrentBlock -= len;

	return heapStr;
}

void StringHeap::allocBlock(const size_t size)
{
//printf("\rAllocating a heap block with %lu bytes for a total of %lu blocks\n", size, this->blocks.size() + 1);
	char *block = new char[size];

	this->blocks.push_front(block);
	this->freeBytesInCurrentBlock = size;
	this->freePtr = block;

	if (!Memory::CanAllocateMemory(0))
		flag_low_memory();
}

void StringHeap::FreeMemory()
{
	while (!this->blocks.empty())
	{
		delete[] this->blocks.front();
		this->blocks.pop_front();
	}

	this->freePtr = NULL;
	this->freeBytesInCurrentBlock = 0;

	this->low_on_memory = false;
}

