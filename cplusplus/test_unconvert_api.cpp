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

//
// Example usage of the UnconvertFromZDWToMemory class.
//

#include "UnconvertFromZDW.h"

#include <stdio.h>
#include <stdlib.h>

using namespace std;
using namespace adobe::zdw;


//*****************************
void ShowHelp(char* executable)
{
	char* exe = strrchr(executable, '/');
	if (exe)
		++exe; //skip '/'
	else
		exe = executable;
	printf("UnconvertFromZDWToMemory, Version %d%s\n",
			UnconvertFromZDW_Base::UNCONVERT_ZDW_VERSION, UnconvertFromZDW_Base::UNCONVERT_ZDW_VERSION_TAIL);
	printf("Usage: %s [-ci csvColumnNames] file1 [files...]\n", exe);
	printf("\t-ci specify a comma-separated list of column names to output (default = all columns)\n");
	printf("\n");
}


void processLine(const char** outColumns, size_t numColumns)
{
	assert(outColumns);
	for (size_t col = 0; col < numColumns; ++col) {
		printf("%s", outColumns[col]);
		if (col<numColumns - 1)
			printf("\t");
	}
	printf("\n");
}

int main(int argc, char* argv[])
{
	int fileBeginIndex = 1;
	if (argc < 2)
	{
		ShowHelp(argv[0]);
		exit(1);
	}

	string namesOfColumnsToOutput;
	if (!strcmp(argv[1], "-ci"))
	{
		if (argc < 4)
		{
			ShowHelp(argv[0]);
			exit(1);
		}
		namesOfColumnsToOutput = argv[2];
		fileBeginIndex = 3;
	}

	//Each iteration unconverts one ZDW input file.
	for (int i = fileBeginIndex; i < argc; ++i)
	{
		UnconvertFromZDWToMemory unconvert(argv[i], false); //test in-memory API
		if (!namesOfColumnsToOutput.empty())
		{
			unconvert.setNamesOfColumnsToOutput(namesOfColumnsToOutput, SKIP_INVALID_COLUMN);
		}

		ERR_CODE eRet;

		eRet = unconvert.readHeader();
		if (eRet != OK)
		{
			fprintf(stderr, "Error %i\n", eRet);
			return eRet;
		}

		//const vector<string> columns = unconvert.getColumnNames();
		//const char unsigned columnTypes = unconvert.getColumnTypes();

		size_t numColumns;
		eRet = unconvert.getNumOutputColumns(numColumns);
		if (eRet != OK)
		{
			fprintf(stderr, "Error %i\n", eRet);
			return eRet;
		}

		const char **outColumns = new const char*[numColumns]; //array of char* -- will point to column values in each row

		size_t lineLength = unconvert.getLineLength();
		char * buffer = new char[lineLength];

		while (!unconvert.isFinished()) {
			eRet = unconvert.getRow(&buffer, &lineLength, outColumns, numColumns);
			switch (eRet) {
				case OK:
					processLine(outColumns, numColumns);
					break;
				case AT_END_OF_FILE:
					//successful completion
					assert(unconvert.isFinished());
					break;
				default:
					fprintf(stderr, "Error %i\n", eRet);
					return eRet;
			}
		}
		delete[] buffer;
		delete[] outColumns;
	}

	return 0;
}

