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

#ifndef CONVERTTOZDW_H
#define CONVERTTOZDW_H

#include "dictionary.h"
#include "status_output.h"

#include <string>
#include <vector>


//************************************************
class ConvertToZDW
{
public:
	//Version info
	static const int CONVERT_ZDW_CURRENT_VERSION;
	static const char CONVERT_ZDW_VERSION_TAIL[3];

	enum Compressor {
		GZIP = 0,
		BZIP2= 1,
		XZ   = 2
	};

	//Error codes
	enum ERR_CODE
	{
		OK=0, //don't change values API
		NO_ARGS=1,
		CONVERSION_FAILED=2,
		UNTAR_FAILED=3,
		MISSING_DESC_FILE=4,
		MISSING_SQL_FILE=5,
		FILE_CREATION_ERR=6,
		OUT_OF_MEMORY=7,
		UNCONVERT_FAILED=8,
		FILE_SIZES_DIFFER=9,
		FILES_DIFFER=10,
		MISSING_ARGUMENT=11,
		GZIP_FAILED=12,
		BZIP2_FAILED=13,
		DESC_FILE_MISSING_TYPE_INFO=14,
		WRONG_NUM_OF_COLUMNS_ON_A_ROW=15,
		BAD_PARAMETER=16,
		TOO_MANY_INPUT_FILES=17,
		NO_INPUT_FILES=18,
		CANT_OPEN_TEMP_FILE=19,
		UNKNOWN_ERROR=20,
		ERR_CODE_COUNT
	};
	static const char ERR_CODE_TEXTS[ERR_CODE_COUNT][30];

	ConvertToZDW(const bool bQuiet = false, const bool bStreamingInput = false)
		: compressor(GZIP)
		, numRows(0)
		, m_row(NULL)
		, m_Version(CONVERT_ZDW_CURRENT_VERSION)
		, minmaxset(NULL), columnSize(NULL)
		, statusOutput(ZDW::defaultStatusOutputCallback)
		, bQuiet(bQuiet)
		, bTrimTrailingSpaces(false)
		, bStreamingInput(bStreamingInput)
		, tmp_fp(NULL)
	{ }
	~ConvertToZDW()
	{
		delete[] m_row;
		delete[] minmaxset;
		delete[] columnSize;
	}

	void setStatusOutputCallback(ZDW::StatusOutputCallback cb) { statusOutput = cb; }

	void trimTrailingSpaces(bool val = true) { bTrimTrailingSpaces = val; }
	const char* getInputFileExtension() const { return "sql"; }

	ERR_CODE convertFile(const char* infile, const char* exeName,
		const bool bValidate, char* filestub, const char* outputDir = NULL, const char* zArgs = NULL);

	Compressor compressor;

private:
	const char* getExtensionForCompressor() {
		switch (compressor) {
			case GZIP:
				return ".gz";
			case BZIP2:
				return ".bz2";
			case XZ:
				return ".xz";
			default:
				return "";
		}
	}

	const char* getCompressionCommand() {
		switch (compressor) {
			case GZIP:
				return "gzip";
			case BZIP2:
				return "bzip2";
			case XZ:
				return "xz";
			default:
				return "";
		}
	}


	size_t GetDataRow(FILE* f, char *&row, std::vector<char*>& rowColumns);
	ULONG writeBlockRows(FILE* in, FILE* out,
		const size_t numColumns, const size_t numColumnsUsed);
	size_t writeLookupColumnStats(FILE* out, const size_t numColumns);

	enum INPUT_STATUS
	{
		IS_DONE=0,
		IS_NOT_ENOUGH_MEMORY=1,
		IS_WRONG_NUM_OF_COLUMNS_ON_A_ROW=2
	};

	INPUT_STATUS parseInput(FILE* in);
	ERR_CODE processFile(FILE* in, const char* filestub, const size_t numColumns,
			const bool bValidate, const char* exeName, const char* outputDir = NULL, const char* zArgs = NULL);
	int unsigned ReadDescFile(FILE* f);

	ERR_CODE validate(const char* zdwFile, const std::vector<std::string>& src_filenames,
		const char* exeName, const char* outputDir = NULL);

	ULONG numRows;

	std::vector<std::string> m_DWColumns;
	std::vector<char unsigned> m_ColumnType;
	std::vector<int> columnCharSize; //# of characters in the data type
	char *m_row;

	USHORT m_Version;
	ULONG m_LongestLine;

	Dictionary uniques;

	std::vector<char*> rowColumns;
	char unsigned *minmaxset;
	std::vector<ULONGLONG> columnMin;
	std::vector<ULONGLONG> columnMax;
	char unsigned *columnSize;
	std::vector<ULONGLONG> columnVal;
	std::vector<storageBytes> columnStoredVal[2];
	std::vector<short> usedColumn;

	ZDW::StatusOutputCallback statusOutput;

	const bool bQuiet; //quiet running (no progress output messages)
	bool bTrimTrailingSpaces;

	const bool bStreamingInput; //if set, reading data from stdin
	FILE *tmp_fp; //used when streaming data in -- stores data for second pass
};

#endif
