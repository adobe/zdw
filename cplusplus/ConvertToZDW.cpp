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

#include "ConvertToZDW.h"

#include "getnextrow.h"

#include "zdw_column_type_constants.h"

#include <cstring>
#include <cassert>
#include <fstream>
#include <sstream>
#include <math.h>
#include <stdlib.h>
#include <strings.h>
#include <unistd.h>

using namespace adobe::zdw::internal;
using std::map;
using std::strchr;
using std::string;
using std::vector;

//version 4  -- multi-block support in a single file
//version 4a -- streaming validation + quiet mode
//version 5  -- fixed single escaped characters being corrupted, added output dir configuration option, bzip2 compression support
//version 5a -- fixed a bug causing crash for lines over 16K
//version 5b -- workaround to allow lines bigger than 32K
//version 5c -- now preserves carriage returns from the source file instead of stripping them when they precede the line feed
//version 5d -- more accurate RAM available check
//version 5e -- added support for source data with no visIDs and no unique values (e.g. empty file); now supports input files with other extensions
//version 6  -- added explicit support for signed (negative) numbers (ZDW files created before ver6 are all considered unsigned), support for lines longer than 64K bytes (up to 4G bytes)
//version 7  -- added support for tinytext and char(3+) fields; also now store the exact size of fields
//version 7a -- rebalance the visitor ID node tree much less frequently to avoid performance slowdowns on files with monotonically increasing visids
//version 7b -- added support for reading data from stdin
//version 7c -- robust to Data Sources import visids (increasing visid_low's on each row)
//version 7d -- strip trailing whitespace option
//version 7e -- support for xz files
//version 8  -- removed separate visitor id dictionary table
//version 8a -- fixed buffer overrun when column header is too long
//version 9  -- replaced 8-char block dictionary tree with simplified sorted map strategy, which XZ compression can capitalize on
//version 9a -- added support for passing in arguments to the file compression process
//version 9b -- Allowing > 2040 columns to be set in a block (via fixing an integer overflow)
//version 10 -- add support for mediumtext and longtext columns
//version 10a -- version 11 support built in (use of v11 is disabled by default)
//version 11 -- add metadata block to file header
//version 11a -- add fxz support
//version 11b -- add zstd support


namespace {

static const int unsigned BAD_FIELD = static_cast<int unsigned>(-1);


inline void get_next_column(char*& col);
inline bool dump_trimmed_row_to_temp_file(FILE* fp, const vector<char*>& rowColumns);

}


namespace adobe {
namespace zdw {

const int ConvertToZDW::CONVERT_ZDW_CURRENT_VERSION = 11;
const char ConvertToZDW::CONVERT_ZDW_VERSION_TAIL[3] = "b";

const char ConvertToZDW::ERR_CODE_TEXTS[ERR_CODE_COUNT][30] = {
	"OK","NO_ARGS","CONVERSION_FAILED","UNTAR_FAILED","MISSING_DESC_FILE","MISSING_SQL_FILE",
	"FILE_CREATION_ERR","OUT_OF_MEMORY","UNCONVERT_FAILED","FILE_SIZES_DIFFER",
	"FILES_DIFFER","MISSING_ARGUMENT","GZIP_FAILED","BZIP2_FAILED",
	"DESC_FILE_MISSING_TYPE_INFO","WRONG_NUM_OF_COLUMNS_ON_A_ROW",
	"BAD_PARAMETER","TOO_MANY_INPUT_FILES","NO_INPUT_FILES","CANT_OPEN_TEMP_FILE",
	"Unknown error",
	"BAD_METADATA_PARAMETER", "BAD_METADATA_FILE"
};

//Parses the .desc.sql file to determine column names and sql types
//
//Returns: number of columns in the description file, or BAD_FIELD if a field type is missing
int unsigned ConvertToZDW::ReadDescFile(FILE* f)
{
	static const ULONG MAX_DESC_LINE_LENGTH = 1024;
	static const size_t MAX_EXPECTED_COLUMNS = 600;

	char *tab;
	int unsigned numColumns = 0;
	char row[MAX_DESC_LINE_LENGTH];
	m_ColumnType.clear();
	this->columnCharSize.clear();
	m_ColumnType.reserve(MAX_EXPECTED_COLUMNS);
	this->columnCharSize.reserve(MAX_EXPECTED_COLUMNS);
	while (fgets(row, MAX_DESC_LINE_LENGTH, f))
	{
		if (!strncasecmp(row, "Field", 5))
			continue;

		tab = strchr(row, '\t');
		if (!tab)
			return BAD_FIELD;
		*tab = 0;

		m_DWColumns.push_back(row);
		this->columnCharSize.push_back(0); //default: don't care

		++tab;
		if (!strncmp(tab, "varchar", 7)) {
			const int char_size = atoi(tab + 8);
			m_ColumnType.push_back(VARCHAR);
			this->columnCharSize.back() = char_size;
		}
		else if (!strncmp(tab, "char", 4))
		{
			const int char_size = atoi(tab + 5);
			if (char_size == 1)
				m_ColumnType.push_back(CHAR);
			else if (char_size == 2)
				m_ColumnType.push_back(CHAR_2);
			else
				m_ColumnType.push_back(VARCHAR); //don't care about representing the size of this char(x) field precisely
			this->columnCharSize.back() = char_size;
		}
		else if (!strncmp(tab, "text", 4))
			m_ColumnType.push_back(TEXT);
		else if (!strncmp(tab, "tinytext", 8))
			m_ColumnType.push_back(TINYTEXT);
		else if (!strncmp(tab, "mediumtext", 10))
			m_ColumnType.push_back(MEDIUMTEXT);
		else if (!strncmp(tab, "longtext", 8))
			m_ColumnType.push_back(LONGTEXT);
		else if (!strncmp(tab, "datetime", 8))
			m_ColumnType.push_back(DATETIME);
		else if (!strncmp(tab, "decimal", 7) || !strncmp(tab + 1, "decimal", 7))
			m_ColumnType.push_back(DECIMAL);
		else {
			//Numeric values.
			//Look for "unsigned" (ver6+).
			const bool bSigned = (strstr(tab, "unsigned") == NULL);

			if (!strncmp(tab, "tinyint", 7))
				m_ColumnType.push_back(bSigned ? TINY_SIGNED : TINY);
			else if (!strncmp(tab, "smallint", 8))
				m_ColumnType.push_back(bSigned ? SHORT_SIGNED : SHORT);
			else if (!strncmp(tab, "bigint", 6))
				m_ColumnType.push_back(bSigned ? LONGLONG_SIGNED : LONGLONG);
			else
				m_ColumnType.push_back(bSigned ? LONG_SIGNED : LONG);
		}
		++numColumns;
	}
	return numColumns;
}

//Returns: whether the data written out are textually identical to the source data
//NOTE: does not compare the .desc.sql files, because these will typically not be identical
ConvertToZDW::ERR_CODE ConvertToZDW::validate(
	const char* zdwFile, const vector<string>& src_filenames,
	const char* exeName,
	const char* outputDir) //(in) directory to output files to [default=NULL]
{
	assert(zdwFile);
	assert(exeName);
	assert(!src_filenames.empty());

	if (!this->bQuiet)
		statusOutput(INFO, "Unconverting %s back for validation...\n", zdwFile);

	//Call unconvertDWfile at the same path location where this app was invoked.
	string zdw_path = exeName;
	//Strip the binary name off (everything after the final /).
	const char* pos = exeName + strlen(exeName);  //start from end...
	while (pos > exeName && pos[-1] != '/') //...and look backwards
		--pos;
	zdw_path.resize(pos - exeName);
	//Stream ZDW output to validation process.
	string zdw_cmd = zdw_path + "unconvertDWfile -q - ";

	//Use a different output dir?
	if (outputDir) {
		zdw_cmd += "-d ";
		zdw_cmd += outputDir;
		zdw_cmd += " ";
	}

	zdw_cmd += zdwFile;

	string cmd;
	if (this->bStreamingInput) {
		//Stream from the series of .gz temp files.
		string zcat_str = "zcat ";
		for (size_t i = 0; i < src_filenames.size(); ++i) {
			zcat_str += src_filenames[i];
			zcat_str += " ";
		}
		//Compare the two data pipes.
		cmd = "/bin/bash -c \"cmp <(" + zdw_cmd + ") <(" + zcat_str + ")\"";
	} else if (this->bTrimTrailingSpaces) {
		//Stream ZDW file to 'cmp' (binary comparison) against a trimmed version of the original .sql file
		assert(src_filenames.size() == 1);
		string cat_str = zdw_path + "trim_spaces ";
		cat_str += src_filenames[0];
		cmd = "/bin/bash -c \"cmp <(" + zdw_cmd + ") <(" + cat_str + ")\"";
	} else {
		//Stream ZDW file to 'cmp' (binary comparison) against the original .sql file
		assert(src_filenames.size() == 1);
		cmd = zdw_cmd + " | cmp " + src_filenames[0];
	}

	if (!this->bQuiet)
		statusOutput(INFO, "VALIDATION COMMAND: %s\n", cmd.c_str());
	const int err = system(cmd.c_str());
	return err == 0 ? OK : FILES_DIFFER;
}

//Returns: whether inputted metadata is valid
bool ConvertToZDW::validateMetadata(const map<string, string>& metadata) const
{
	for (map<string, string>::const_iterator it = metadata.begin(); it != metadata.end(); ++it) {
		const string &key = it->first, &value = it->second;
		if (key.find_first_of("=\n") != string::npos)
			return false;
		if (value.find_first_of("\n") != string::npos)
			return false;
	}

	return true;
}

//Returns: 0=success, -1=file load error, otherwise # of bad line
int ConvertToZDW::loadMetadataFile(const char* filepath,
	map<string, string>& metadata) //(in/out)
{
	std::ifstream stream(filepath);

	if (!stream)
		return -1;

	string line;
	for (int linenum = 1; getline(stream, line); ++linenum)
	{
		if (line.empty())
			continue;

		const size_t position = line.find('=');
		if (position == string::npos)
			return linenum;

		metadata[line.substr(0, position)] = line.substr(position + 1);
	}

	return 0;
}

//Returns: number of columns in returned row
size_t ConvertToZDW::GetDataRow(
	FILE* f, char *&row,
	vector<char*>& rowColumns) // read the next line from f into row and set rowColumns to point to columns
{
	rowColumns.clear();

	if (GetNextRow(f, row, m_LongestLine))
	{
		//If we're streaming data in, store this data to a temp file
		if (this->tmp_fp &&
			!this->bTrimTrailingSpaces) //trimming whitespace here is expensive -- output row below after trimming
		{
			const size_t len = strlen(row);

			if (fwrite(row, 1, len, this->tmp_fp) != len)
				return 0; //had an error writing to the temp file
			if (fwrite("\n", 1, 1, this->tmp_fp) != 1) //reinsert trailing newline that was truncated
				return 0;
		}

		char *col = row, *temp;
		while (col)
		{
			rowColumns.push_back(col);
			get_next_column(col);
			if (col) // replace the tab with an end of string null
			{
				*col = 0;
				++col;

				if (this->bTrimTrailingSpaces) {
					temp = col - 2; //last character of field just parsed
					while (*temp == ' ' && temp >= row) {
						*temp = 0;
						--temp;
					}
				}
			} //and move on to the next field
		}

		if (this->bTrimTrailingSpaces) {
			//trim final field
			char *final_field = rowColumns.back();
			temp = final_field;
			temp += strlen(temp) - 1;
			while (*temp == ' ' && temp >= final_field) {
				*temp = 0;
				--temp;
			}

			//now dump trimmed fields to temp file
			if (this->tmp_fp) {
				if (!dump_trimmed_row_to_temp_file(this->tmp_fp, rowColumns))
					return 0;
			}
		}
	}
	return rowColumns.size();
}

//******************************************
//Parse through the rows in the input file.
//
//Returns: error enum indicating whether we completed parsing the input file, and if not, what happened
ConvertToZDW::INPUT_STATUS ConvertToZDW::parseInput(FILE* in)
{
	bool hadEnoughMemory = true;
	size_t n;
	const size_t numColumns = m_ColumnType.size();
	while (hadEnoughMemory && (n = GetDataRow(in, m_row, this->rowColumns)))
	{
		if (n != numColumns)
			return IS_WRONG_NUM_OF_COLUMNS_ON_A_ROW;
		for (size_t c = 0; hadEnoughMemory && c < n; ++c)
		{
			if (!this->rowColumns[c][0])
				continue; //skip empty values

			switch (m_ColumnType[c])
			{
				case DECIMAL:
				case VARCHAR:
				case TEXT:
				case TINYTEXT:
				case MEDIUMTEXT:
				case LONGTEXT:
				case DATETIME:
				case CHAR_2:
					minmaxset[c] = 1;
					hadEnoughMemory = this->uniques.insert(this->rowColumns[c]);
				break;
				case CHAR:
				{
					ULONGLONG& val = columnVal[c];
					val = this->rowColumns[c][0];
					if (this->rowColumns[c][0] == '\\') //include escaped chars verbatim
						val += (this->rowColumns[c][1] * 256);
					if (val > 0)
					{
						if (minmaxset[c])
						{
							if (val > columnMax[c])
								columnMax[c] = val;
							else if (val < columnMin[c])
								columnMin[c] = val;
						} else {
							columnMax[c] = columnMin[c] = val;
							minmaxset[c] = 1;
						}
					}
				}
				break;
				case TINY: case TINY_SIGNED:
				case SHORT: case SHORT_SIGNED:
				case LONG: case LONG_SIGNED:
				case LONGLONG: case LONGLONG_SIGNED:
				{
					//Signed values can be considered as unsigned values
					//when gathering the range (though it is technically inaccurate).
					ULONGLONG& val = columnVal[c];
					val = strtoull(this->rowColumns[c], NULL, 10);
					if (val > 0)
					{
						if (minmaxset[c])
						{
							if (val > columnMax[c])
								columnMax[c] = val;
							else if (val < columnMin[c])
								columnMin[c] = val;
						} else {
							columnMax[c] = columnMin[c] = val;
							minmaxset[c] = 1;
						}
					}
				}
				break;
				default: assert(!"Unrecognized column type"); break;
			}
		}
		if (hadEnoughMemory)
		{
			++this->numRows;
			if (!(this->numRows % 10000) && !this->bQuiet)
			{
				statusOutput(INFO, "\r%u rows", this->numRows);
			}
		}
	}
	return hadEnoughMemory ? IS_DONE : IS_NOT_ENOUGH_MEMORY;
}

//Returns: number of columns with non-default values.
size_t ConvertToZDW::writeLookupColumnStats(FILE* out, const size_t numColumns)
{
	const char offsetSize = static_cast<char>(this->uniques.getBytesInOffset());

	ULONGLONG *usedColumnMin = new ULONGLONG[numColumns];

	//Determine size of columns in this block.  If column is empty, mark it unused.
	size_t numColumnsUsed = 0;
	ULONGLONG val;
	for (size_t c = 0; c < numColumns; ++c)
	{
		if (!minmaxset[c])
		{
			//column empty everywhere
			columnSize[c] = 0;
//			statusOutput(INFO, "%s not used\n", m_DWColumns[c].c_str());
			continue;
		}

		switch (m_ColumnType[c])
		{
			default: assert(!"Unrecognized type"); break;
			case VARCHAR:
			case TEXT:
			case TINYTEXT:
			case MEDIUMTEXT:
			case LONGTEXT:
			case DATETIME:
			case CHAR_2:
			case DECIMAL:
				columnSize[c] = offsetSize;
				columnMin[c] = 0;
				break;
			case CHAR:
			case TINY: case TINY_SIGNED:
			case SHORT: case SHORT_SIGNED:
			case LONG: case LONG_SIGNED:
			case LONGLONG: case LONGLONG_SIGNED:
				--columnMin[c];

				//Determine # of bytes required to store this value.
				val = columnMax[c] - columnMin[c];
				columnSize[c] = 1;
				while (val >= 256)
				{
					++columnSize[c];
					val /= 256;
				}
				break;
		}

//		statusOutput(INFO, "%s type %d stored in %d bytes\n", m_DWColumns[c].c_str(), (int)m_ColumnType[c], columnSize[c]);
		usedColumn[numColumnsUsed] = c;
		usedColumnMin[numColumnsUsed] = columnMin[c];
		++numColumnsUsed;
	}

	//Write byte size required for each column index.
	fwrite(columnSize, 1, numColumns, out);

	//Write minimum value of each column index.
	assert(sizeof(ULONGLONG) == 8);
	fwrite(usedColumnMin, 8, numColumnsUsed, out);
	delete[] usedColumnMin;

	return numColumnsUsed;
}

//Returns: the number of rows outputted.
ULONG ConvertToZDW::writeBlockRows(
	FILE* in, FILE* out,
	const size_t numColumns, const size_t numColumnsUsed)
{
	size_t k, u, c, r = 1;
	int j;
	UCHAR b; //for bit packing

	assert(in);
	assert(out);

	char *rowIndexOut = new char[numColumnsUsed * 8];
//	BufferedOutput buffer(out); //commented out -- not needed when writing to a pipe

	const size_t numSetColumnBytes = static_cast<size_t>(ceil(numColumnsUsed / 8.0));
	unsigned char *setColumns = new unsigned char[numSetColumnBytes];

	//Use default values of 0 for the previous row check.
	for (k = 0; k < numColumns; ++k)
		this->columnStoredVal[0][k].n = 0;

	//Each iteration writes out one row for this block.
	ULONG cnt = 0;
	while (cnt < this->numRows && GetDataRow(in, m_row, this->rowColumns) > 0)
	{
		ULONG p = 0;
		memset(setColumns, 0, numSetColumnBytes);

		for (u = 0; u < numColumnsUsed; u++)
		{
			c = usedColumn[u];
			storageBytes& storedVal = this->columnStoredVal[r][c];
			switch (m_ColumnType[c])
			{
				case VARCHAR:
				case TEXT:
				case TINYTEXT:
				case MEDIUMTEXT:
				case LONGTEXT:
				case DATETIME:
				case CHAR_2:
				case DECIMAL:
					if (this->rowColumns[c][0])
						storedVal.n = this->uniques.getOffset(this->rowColumns[c]); // - columnMin[c]; -- now hardcoded to 0
					else
						storedVal.n = 0;
					if (columnStoredVal[0][c].n != columnStoredVal[1][c].n)
					{
						b = 1u << (u % 8);
						setColumns[u / 8] |= b;
						for (j = 0; j < columnSize[c]; j++)
						{
							rowIndexOut[p++] = storedVal.c[j];
						}
					}
					break;
				case CHAR:
					storedVal.n = this->rowColumns[c][0];
					if (storedVal.n) {
						storedVal.n += this->rowColumns[c][1] * 256; //will either be NULL or an escaped char
						storedVal.n -= columnMin[c];
					}
					if (columnStoredVal[0][c].n != columnStoredVal[1][c].n)
					{
						b = 1u << (u % 8);
						setColumns[u / 8] |= b;
						for (j = 0; j < columnSize[c]; j++)
						{
							rowIndexOut[p++] = storedVal.c[j];
						}
					}
					break;
				case TINY: case TINY_SIGNED:
				case SHORT: case SHORT_SIGNED:
				case LONG: case LONG_SIGNED:
				case LONGLONG: case LONGLONG_SIGNED:
					//Signed values can be stored as unsigned values
					//as long as we flag to unpack them correctly.
					storedVal.n = strtoull(this->rowColumns[c], NULL, 10);
					if (storedVal.n > 0)
						storedVal.n -= columnMin[c];
					if (columnStoredVal[0][c].n != columnStoredVal[1][c].n)
					{
						b = 1u << (u % 8);
						setColumns[u / 8] |= b;
						for (j = 0; j < columnSize[c]; j++)
						{
							rowIndexOut[p++] = storedVal.c[j];
						}
					}
					break;
			}
		}

		//Output encoded row.
		//A row is encoded as:
		//1. A set of bits, one per column, representing which column values were the same
		//   as those of the previous row.
		//2. A series of indices into the lookup tables for columns whose values were
		//   not the same as those in the previous row.
//		buffer.write(setColumns, numSetColumnBytes);
//		buffer.write(rowIndexOut, p);
		fwrite(setColumns, numSetColumnBytes, 1, out);
		fwrite(rowIndexOut, p, 1, out);

		//Toggle to track field values that match those of the previous row.
		r = (r ? 0 : 1);

		cnt++;
		if (!(cnt % 10000) && !this->bQuiet)
		{
			statusOutput(INFO, "\r%u", cnt);
		}
	}

	//Clean up.
	delete[] rowIndexOut;
	delete[] setColumns;

	return cnt;
}

//Returns: whether file was completely processed successfully
ConvertToZDW::ERR_CODE ConvertToZDW::processFile(
	FILE* f_in,              //(in) input file handle -- may be stdin or a file on disk
	const char* filestub,  //(in) input path + basename
	const size_t numColumns, //(in) # columns in .desc.sql file
	const bool bValidate,  //(in) whether to validate outputted file
	const char* exeName,   //(in) name of this binary
	const char* outputDir, //(in) directory to output to [default=NULL, i.e. current]
	const char* zArgs,     //(in) if not NULL (default), pass this into compression process
	const map<string, string>& metadata) //(in) supply these key-value pairs as file metadata
{
	assert(filestub);
	assert(exeName);

	//Validate metadata block.
	if (!validateMetadata(metadata))
	{
		statusOutput(ERROR, "Invalid metadata parameter\n");
		return BAD_METADATA_PARAM;
	}

	string zdwFile;

	if (!outputDir) {
		zdwFile += filestub; //same name + path
	} else {
		//Output to a different dir
		zdwFile += outputDir;
		zdwFile += "/";

		//Skip any directory separators in the input filepath
		const char *base = filestub + strlen(filestub);
		while (base > filestub && base[-1] != '/')
			--base;
		zdwFile += base;
	}

	const string outfile_basepath = zdwFile;

	//The ZDW file will be named "<outputDir><basefilename>.zdw.[xz|gz|bz2|etc]"
	zdwFile += ".zdw";
	zdwFile += getExtensionForCompressor();

	//Stream out the data being created to a temp file name.
	//We'll rename it to the final filename (zdwFile) on completion.
	static const char temp_suffix[] = ".creating";
	string temp_outfile_name = outfile_basepath;
	temp_outfile_name += temp_suffix;
	temp_outfile_name += ".zdw";
	temp_outfile_name += getExtensionForCompressor();

	string cmd = getCompressionCommand();
	if (zArgs) {
		cmd += " ";
		cmd += zArgs;
	}
	cmd += " > ";
	cmd += temp_outfile_name;
	FILE *out = popen(cmd.c_str(), "w");
	if (!out)
	{
		statusOutput(ERROR, "Could not open the process '%s' for writing!\n", cmd.c_str());
		return FILE_CREATION_ERR;
	}

	//Write version #.
	fwrite(&m_Version, 1, 2, out);

	//Write metadata block.
	{
		ULONG metadata_length = 0;
		map<string, string>::const_iterator it;
		for (it = metadata.begin(); it != metadata.end(); ++it) {
			metadata_length += it->first.length();
			metadata_length += it->second.length();
			metadata_length += 2; //two null terminators
		}
		fwrite(&metadata_length, 1, 4, out);

		for (it = metadata.begin(); it != metadata.end(); ++it) {
			const string &key = it->first, &value = it->second;
			fwrite(key.c_str(), 1, key.length(), out);
			fputc('\0', out);
			fwrite(value.c_str(), 1, value.length(), out);
			fputc('\0', out);
		}
	}

	//Write column info.
	{
		//Write the column names in the following format:
		//{<name>/0}*/0
		//i.e. null-terminated strings, followed by a null character, signifying the end of the column names
		{
			int unsigned c, n;
			for (n = 0, c = 0; c < numColumns; c++) {
				n += m_DWColumns[c].length() + 1; // we want the null character left at the end of each column name.
			}
			char *line = new char[n + 1];

			for (n = 0, c = 0; c < numColumns; c++)
			{
				strcpy(line + n, m_DWColumns[c].c_str());
				n += m_DWColumns[c].length() + 1; //skip over null char
			}
			line[n++] = 0;
			fwrite(line, 1, n, out);
			delete[] line;
		}

		//Write the column types as a byte vector of enumerated values.
		{
			char unsigned *temp = new char unsigned[numColumns];
			for (int unsigned k = 0; k < numColumns; ++k)
				temp[k] = m_ColumnType[k];
			assert(sizeof(char unsigned) == 1);
			fwrite(temp, 1, numColumns, out);
			delete[] temp;
		}

		//Version 7+: Write the column char sizes as a byte vector of enumerated values.
		//Used for outputting an accurate table schema description.
		{
			short unsigned *temp = new USHORT[numColumns];
			for (int unsigned k = 0; k < numColumns; ++k)
				temp[k] = static_cast<USHORT>(this->columnCharSize[k]);
			fwrite(temp, 2, numColumns, out);
			delete[] temp;
		}
	}

	//Each iteration generates a block of information.
	//A block contains:
	//1.  Header info.
	//2.  Lookup tables that are made as large as memory permits.
	//3.  A series of encoded rows for which the lookup tables are relevant.
	//If available memory is exhausted before parsing the entire input file,
	//the the current block will be written to the output file,
	//and the process will repeat with a fresh block
	//until the input file has been completely parsed.
	ULONG cnt = 0, totalCnt = 0;
	ERR_CODE res = OK;
	bool hadEnoughMemory = true;

	//Maintain a list of the input files/blocks for later validation.
	vector<string> tmp_filenames; //for validation of data that was streamed in
	int file_pieces = 0, blocks = 0;
	if (!this->bStreamingInput) {
		//There is only one input file -- indicate it here.
		string src_filename = filestub;
		src_filename += ".";
		src_filename += getInputFileExtension();
		tmp_filenames.push_back(src_filename);
		++file_pieces;
	}

	ULONGLONG total_rows = 0;
	do
	{
		INPUT_STATUS inputStatus;
		fpos_t fbegin;
		string tmp_filename;

		++blocks;

		if (!this->bQuiet) {
			if (hadEnoughMemory)
				statusOutput(INFO, "\nProcessing %s\n", filestub);
			else
				statusOutput(INFO, "\nProcessing block %d of %s (%" PF_LLU " rows so far)\n", blocks, filestub, total_rows);
		}

		//Scan input rows.  Build lookup tables.
		if (!this->bQuiet)
			statusOutput(INFO, "Compiling unique values\n");
		this->numRows = 0;
		memset(minmaxset, 0, numColumns);

		if (this->bStreamingInput) {
			//Open a temp file in the output dir in order to store
			//the data being streamed in for the second read pass.
			assert(!this->tmp_fp);
			std::ostringstream str;
			str << outfile_basepath << ".tmp." << file_pieces << ".gz";
			tmp_filename = str.str();
			string cmd = "gzip > "; //compress the data to reduce disk writes
			cmd += tmp_filename;
			this->tmp_fp = popen(cmd.c_str(), "w");
			if (!this->tmp_fp) {
				res = CANT_OPEN_TEMP_FILE;
				goto Done;
			}
		} else {
			//mark current spot in input file -- we will rewind to here for the second pass
			fgetpos(f_in, &fbegin);
		}

		inputStatus = parseInput(f_in);
		switch (inputStatus)
		{
			case IS_DONE: hadEnoughMemory = true; break;
			case IS_NOT_ENOUGH_MEMORY: hadEnoughMemory = false; break;
			case IS_WRONG_NUM_OF_COLUMNS_ON_A_ROW:
				statusOutput(ERROR, "\nRow %u had the problem\n", this->numRows + 1); //one past the last good row
				return WRONG_NUM_OF_COLUMNS_ON_A_ROW;
		}
		if (this->bStreamingInput) {
			//We are now done writing to the temp file.
			assert(this->tmp_fp);
			pclose(this->tmp_fp);
			this->tmp_fp = NULL;
		}

		if (!this->bQuiet)
			statusOutput(INFO, "\r%u rows\n", this->numRows);

		if (!this->numRows)
		{
			if (hadEnoughMemory)
			{
				assert(!totalCnt); //this should be during the first block
				statusOutput(ERROR, "Empty data file -- nothing to process\n");
			} else {
				statusOutput(ERROR, "Not enough memory to run %s\n", exeName);
				res = OUT_OF_MEMORY;
				goto Done;
			}
			break;
		}

		//Write header info for this block.
		fwrite(&this->numRows, 1, 4, out);
		fwrite(&m_LongestLine, 1, 4, out);
		char unsigned done = hadEnoughMemory ? 1 : 0;
		fwrite(&done, 1, 1, out); //if 0, indicates another block will follow this one

		//Write dictionary.
		if (!this->bQuiet)
			statusOutput(INFO, "\nWriting dictionary:\n%u bytes being stored for %u unique entries.  Generating %d-byte offsets...\n",
				this->uniques.getSize(), this->uniques.getNumEntries(), this->uniques.getBytesInOffset());
		this->uniques.write(out); //side-effect: populates offsets for second pass below

		//Write column field info for these lookup tables.
		const size_t numColumnsUsed = writeLookupColumnStats(out, numColumns);

		//Second pass: parse rows for encoding to the output file.
		FILE *p_second_in = NULL;
		if (this->bStreamingInput) {
			//Begin reading from the temp file.
			string cmd = "zcat ";
			cmd += tmp_filename;
			cmd.append(" 2>/dev/null"); //we don't want to see any chatter
			p_second_in = popen(cmd.c_str(), "r");
			if (!p_second_in) {
				res = CANT_OPEN_TEMP_FILE;
				goto Done;
			}
		} else {
			//rewind to the marked spot in the input file
			fsetpos(f_in, &fbegin);
		}
		if (!this->bQuiet)
			statusOutput(INFO, "\nWriting rows\n");

		cnt = writeBlockRows(this->bStreamingInput ? p_second_in : f_in,
				out, numColumns, numColumnsUsed);
		totalCnt += cnt;

		if (!this->bQuiet)
			statusOutput(INFO, "\r%u\nDone with block %d -- cleaning up...\n", cnt, blocks);

		//Clean up.
		this->uniques.clear();
		if (this->bStreamingInput) {
			//Done reading the temp file -- either save it for validatation or delete it.
			assert(p_second_in);
			pclose(p_second_in);
			if (bValidate) {
				tmp_filenames.push_back(tmp_filename);
			} else {
				unlink(tmp_filename.c_str()); //we're not validating -- free up disk space now
			}

			++file_pieces;
		}
		total_rows += this->numRows;
	} while (!hadEnoughMemory);

	//Done writing out the ZDW file.
	pclose(out);
	out = NULL;

	if (bValidate)
	{
		//Ensure the ZDW file's output is identical to the input data.
		assert(tmp_filenames.size() == static_cast<size_t>(file_pieces)); //if we're storing temp files, we need to validate against them all
		const ERR_CODE eValid = validate(temp_outfile_name.c_str(), tmp_filenames, exeName, outputDir);

		if (eValid == OK)
		{
			if (!this->bQuiet)
				statusOutput(INFO, "%s GOOD\n", zdwFile.c_str());
		} else {
			statusOutput(INFO, "%s BAD\n", zdwFile.c_str());
			res = eValid;
		}
	}

Done:
	//Ensure we've closed this process, in case we are erroring out.
	if (out)
		pclose(out);
	//Delete the temp files created during streaming input.
	if (this->bStreamingInput) {
		for (size_t i = 0; i < tmp_filenames.size(); ++i)
			unlink(tmp_filenames[i].c_str());
	}

	if (res == OK)
	{
		//OUTPUT used by caller apps to grep specific information about what was produced
		if (!this->bQuiet)
			statusOutput(INFO, "Rows=%u\n", totalCnt);

		//Now rename the temp file to the final name.
		const int ret = rename(temp_outfile_name.c_str(), zdwFile.c_str());
		if (ret == 0) {
			if (!this->bQuiet)
				statusOutput(INFO, "Done\n");
		} else {
			res = FILE_CREATION_ERR;
			statusOutput(INFO, "Final create file failed -- you can use %s instead.\n", temp_outfile_name.c_str());
		}
	} else {
		//Remove the temp file we were in the process of creating.
		unlink(temp_outfile_name.c_str());
	}

	return res;
}

//****************************************************
ConvertToZDW::ERR_CODE ConvertToZDW::convertFile(
	const char* infile,   //(in) file to convert
	const char* exeName,  //(in) name of this executable
	const bool bValidate, //(in) if true, then validate that the converted file is good
	char* filestub,       //(out)
	const char* outputDir,//(in) if not NULL (default), use this as the output directory
	const char* zArgs,    //(in) if not NULL (default), pass these arguments into file compressor
	const map<string, string>& metadata) //(in) supply these key-value pairs as file metadata
{
	FILE* in;

	assert(infile);
	assert(exeName);
	assert(filestub);

	m_LongestLine = 16 * 1024; //16K default
	delete[] m_row;
	m_row = new char[m_LongestLine];

	//Input is an .sql dump file.
	strcpy(filestub, infile);
	char* pos = strstr(filestub, ".sql");
	if (pos) {
		*pos = 0;
	} else {
		return MISSING_SQL_FILE;
	}

	//Read .desc file.
	std::string command = filestub;
	command += ".desc.";
	command += getInputFileExtension();
	in = fopen(command.c_str(), "r");
	if (!in)
		return MISSING_DESC_FILE;

	const int unsigned numColumns = ReadDescFile(in);
	if (numColumns == BAD_FIELD)
		return DESC_FILE_MISSING_TYPE_INFO;
	fclose(in);

	map<string, string> inMetadata = metadata;
	if (inMetadata.empty()) {
		//Default to .metadata file contents, if present.
		command = filestub;
		command += ".metadata";
		const int res = loadMetadataFile(command.c_str(), inMetadata);
		if (res > 0) //ignore -1: it's okay for file to be not present
			return BAD_METADATA_FILE;
	}

	//Set needed size for vars.
	this->rowColumns.reserve(numColumns);
	delete[] minmaxset;
	minmaxset = new char unsigned[numColumns];
	columnMin.reserve(numColumns);
	columnMax.reserve(numColumns);
	delete[] columnSize;
	columnSize = new char unsigned[numColumns];
	columnVal.reserve(numColumns);
	columnStoredVal[0].reserve(numColumns);
	columnStoredVal[1].reserve(numColumns);
	usedColumn.reserve(numColumns);

	//Open input file handle.
	if (this->bStreamingInput) {
		in = stdin;
	} else {
		command = filestub;
		command += ".";
		command += getInputFileExtension();
		in = fopen(command.c_str(), "r");
		if (!in)
			return MISSING_SQL_FILE; //couldn't read file
	}

	ERR_CODE conversionResult = UNKNOWN_ERROR;
	try {
		conversionResult = processFile(in, filestub, numColumns, bValidate, exeName, outputDir, zArgs, inMetadata);
	}
	catch(const std::bad_alloc&) {
		return OUT_OF_MEMORY;
	}

	//Clean-up.
	if (!this->bStreamingInput)
		fclose(in);

	return conversionResult;
}

} // namespace zdw
} // namespace adobe


namespace {

//Skip over embedded tabs.
inline void get_next_column(char*& col)
{
	col = strchr(col, '\t'); // embedded tabs are escaped by an odd number of backslashes.
	if (col && col[-1] == '\\')
	{
		char* slash = col - 2;
		while (*slash == '\\')
			--slash;
		while (col && ((col - slash) % 2) == 0)
		{
			col = strchr(col + 1, '\t');
			if (col)
			{
				slash = col - 1;
				while (*slash == '\\')
					--slash;
			}
		}
	}
}

inline bool dump_trimmed_row_to_temp_file(FILE* fp, const vector<char*>& rowColumns)
{
	const int size_minus_one = rowColumns.size() - 1;
	char *field;
	size_t len;
	for (int i = 0; i < size_minus_one; ++i) {
		field = rowColumns[i];
		len = strlen(field);
		if (len > 0 && fwrite(field, 1, len, fp) != len)
			return false; //has an error writing to the temp file
		if (fwrite("\t", 1, 1, fp) != 1) //reinsert tab separators
			return false;
	}
	field = rowColumns[size_minus_one];
	len = strlen(field);
	if (len > 0 && fwrite(field, 1, len, fp) != len)
		return false;
	if (fwrite("\n", 1, 1, fp) != 1) //reinsert trailing newline that was truncated
		return false;
	return true;
}

}

