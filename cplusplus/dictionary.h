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

#ifndef DICTIONARY_H
#define DICTIONARY_H

#include "includes.h"
#include "stringheap.h"

#include <cstring>
#include <map>
#include <string>


//********************************************************
struct cstringComp {
	bool operator() (const char* const& lhs, const char* const& rhs) const { return std::strcmp(lhs, rhs) < 0; }
};

typedef std::map<const char*, ULONG, cstringComp> DictionaryT;
//typedef std::map<std::string, ULONG> DictionaryT; //replaced with custom memory management

//********************************************************
class Dictionary
{
public:
	Dictionary() : size(0) { }

	void clear();

	bool insert(const char* str);

	bool empty() const { return size == 0; }
	ULONG getBytesInOffset() const;
	ULONG getNumEntries() const { return stringOffsets.size(); }
	ULONG getSize() const { return size + 1; } //include origin null byte
	ULONG getOffset(const char* str) const;

	void write(FILE* f); //populates values in stringOffsets

private:
	DictionaryT stringOffsets;
	StringHeap stringHeap;
	ULONG size;
};

#endif
