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

#include "getnextrow.h"

#include <assert.h>
#include <string.h>

// Reads a row from the given file into row, of allocated length rowSize.
// If row is too small to fit the entire line of text, it is reallocated to a larger rowSize.
//
//Returns: length of row
int Common::GetNextRow(
	FILE* f,
	char*& row,   //(in/out) row text
	ULONG& rowSize) //(in/out) size of longest row encountered so far
{
	assert(row);
	assert(rowSize);

	size_t e, len = 0;
	char* str;
	while ((str = fgets(row, rowSize, f)))
	{
		len = strlen(row);
		if (len < 2)
		{
			len = 0; //if this is the last text in the file, we want to return an empty size
			continue;
		}

		bool bEndlineFound = row[len-1] == '\n';
		//Count number of char escapes.
		e = 2;
		while (e <= len && row[len - e] == '\\')
			++e;

		//If it's odd, an escape char interrupted getting the line of text.
		//So, append some more text until the full line has been compiled.
		bool bEndOfLine = bEndlineFound && ((e % 2)==0);
		while (str && !bEndOfLine)
		{
			//Ensure there is enough space to hold an entire row in the buffer.
			const bool bBufferFull = (len == rowSize-1);
			if (bBufferFull)
			{
				char *temp = new char[rowSize*2]; //strategy to avoid many reallocations
				strcpy(temp, row);
				delete[] row;
				row = temp;
				rowSize *= 2; //perform at end for exception safety
			}

			str = fgets(row + len, rowSize - len, f);
			if (!str)
				return 0; //eof -- probably corrupted data
			len += strlen(str);

			bEndlineFound = row[len-1] == '\n';
			e = 2;
			while (e <= len && row[len - e] == '\\')
				++e;

			bEndOfLine = bEndlineFound && ((e % 2)==0);
		}
		row[len - 1] = 0; //truncate trailing newline
		break; //done reading this row
	}
	assert(len < rowSize); //otherwise we had a buffer overflow above
	return len;
}

