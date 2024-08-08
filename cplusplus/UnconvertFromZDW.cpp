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

#if not defined( __STDC_LIMIT_MACROS )
#error __STDC_LIMIT_MACROS must be defined
#endif

#include "zdw/UnconvertFromZDW.h"

#include <algorithm>
#include <math.h>
#include <ostream>
#include <set>
#include <cassert>
#include <sstream>
#include <stdint.h>
#include <string.h>
#include <sys/stat.h>
#include "zdw_column_type_constants.h"

using namespace adobe::zdw::internal;
using std::ostream;
using std::map;
using std::pair;
using std::set;
using std::string;
using std::vector;

//version 4  -- added support for multiple blocks in a single file
//version 4a -- streaming uncompression
//version 5  -- escaped single char fix, file naming fix
//version 5a -- added support for reading files with no visIDs (e.g. wbenchecom) or no unique data (i.e. empty file); added option to name output files with alternate extension instead of .sql
//version 6  -- now explicitly flag signed (negative) column values, added support for lines longer than 16K bytes (up to 4GB); added API interface
//version 6a -- now the .desc file can be dumped to stdout
//version 6b -- now the ZDW data may be read from stdin; now columns specified on the command line are output in the indicated order
//version 6c -- added -ci option
//version 6e -- added -cx and -s options
//version 7  -- added "tinytext" data type
//version 7a -- fixed regression in version 7 w.r.t. stderr chatter
//version 7b -- fixed incorrect output when char field is empty (bug #57203)
//version 7c -- now using -ci and -o and getting no columns back isn't considered an error by the class wrapper
//version 7d -- added -ce option
//version 7e -- add support for xz compression/decompression
//version 7f -- now use zlib instead of zcat for .gz files
//version 7g -- fixed crash when attempting to process a non-existent file
//version 8  -- removed visitors lookup table from internal data format
//version 9  -- replaced 8-char block dictionary tree with simplified sorted map strategy, which XZ compression can capitalize on
//version 9a -- added bit vector counting stats option
//version 9b -- made robust to very large dictionary segments
//version 9c -- pclose fix; case insensitive column selection fixes
//version 9d -- expose "virtual_export_basename" column.
//version 9e -- expose "virtual_export_row" column.
//version 10 -- support mediumtext and longtext column types
//version 10a -- support header line output with non-empty column names for each file block
//version 11 -- add metadata block to header
//version 11a -- fix virtual_export_row output
//version 11b -- add zstandard support
//version 11c -- fix invalid buffer reuse bug from ZSTD pr series.


namespace {

static const char newline[] = "\n";
static const char tab[] = "\t";

const int IGNORE = -1;
const int USE_VIRTUAL_COLUMN = -2;

char const* const VIRTUAL_EXPORT_BASENAME_COLUMN_NAME = "virtual_export_basename";
char const* const VIRTUAL_EXPORT_ROW_COLUMN_NAME = "virtual_export_row";

struct InsensitiveCompare
{
	bool operator() (const string& lhs, const string& rhs) const
	{
		return strcasecmp(lhs.c_str(), rhs.c_str()) < 0;
	}
};


string getInputFilename(string filename)
{
	return !filename.empty() ? filename : string("stdin");
}

/*
 * In: inFileName
 * Out: sourceDir & basename (a pointer into the allocated sourceDir buffer)
 * The caller needs to free the memory pointed to by sourceDir, after all references to basename.
 */
void InitDirAndBasenameFromFileName(string const& inFileNameStr, char * &sourceDir, const char* &basename)
{
	char *filestub_local = NULL;

	//Get basename w/o extension.
	const char* inFileName = inFileNameStr.c_str();
	if (strchr(inFileName, '/')) {
		sourceDir = strdup(inFileName);
		char* tmp = strrchr(sourceDir, '/');
		*tmp = 0;
		tmp++;
		filestub_local = tmp;
	} else {
		const string buf = "./" + inFileNameStr;
		sourceDir = strdup(buf.c_str());
		sourceDir[1] = 0;
		filestub_local = sourceDir + 2;
	}

	//Modify input filestub: cut off the final ".zdw*" for naming the output file(s).
	char *pos = strstr(filestub_local, ".zdw");
	while (pos) {
		//Look for another ".zdw"
		char *nextPos = strstr(pos + 4, ".zdw");
		if (nextPos)
			pos = nextPos;
		else {
			//This is the last one.  Truncate it.
			*pos = 0;
			break;
		}
	}

	basename = filestub_local;
}

}


namespace adobe {
namespace zdw {

const int UnconvertFromZDW_Base::UNCONVERT_ZDW_VERSION = 11;
const char UnconvertFromZDW_Base::UNCONVERT_ZDW_VERSION_TAIL[3] = "c";

const size_t UnconvertFromZDW_Base::DEFAULT_LINE_LENGTH = 16 * 1024; //16K default

const char UnconvertFromZDW_Base::ERR_CODE_TEXTS[ERR_CODE_COUNT + 1][30] = {
	"OK",
	"BAD_PARAMETER",
	"GZREAD_FAILED",
	"FILE_CREATION_ERR",
	"FILE_OPEN_ERR",
	"UNSUPPORTED_ZDW_VERSION_ERR",
	"ZDW_LONGER_THAN_EXPECTED_ERR",
	"UNEXPECTED_DESC_TYPE",
	"ROW_COUNT_ERR",
	"CORRUPTED_DATA_ERROR",
	"HEADER_NOT_READ_YET",
	"HEADER_ALREADY_READ_ERR",
	"AT_END_OF_FILE",
	"BAD_REQUESTED_COLUMN",
	"NO_COLUMNS_TO_OUTPUT",
	"PROCESSING_ERROR",
	"UNSUPPORTED_OPERATION",
	"METADATA_KEY_NOT_PRESENT",
	"Unknown error"
};
//*****************************


ZDWException::ZDWException(const ERR_CODE errcode)
	: runtime_error(UnconvertFromZDW_Base::ERR_CODE_TEXTS[errcode])
	, code(errcode)
{ }


//*****************************
//API maintenance -- old breakdown currently uses this text to detect whether zdw unconvert failed or not
void UnconvertFromZDW_Base::printError(const string &exeName, const string &fileName)
{
	statusOutput(ERROR, "%s: %s failed\n\n", !exeName.empty() ? exeName.c_str() : "UnconvertFromZDW", fileName.c_str());
}

//***********************************************
UnconvertFromZDW_Base::UnconvertFromZDW_Base(const string &fileName,
		const bool bShowStatus, const bool bQuiet, //[default=true, true]
		const bool bTestOnly, const bool bOutputDescFileOnly) //[default=false, false]
	: exportFileLineLength(0)
	, virtualLineLength(0)
	, uniques(NULL)
	, visitors(NULL)
	, version(UNCONVERT_ZDW_VERSION)
	, decimalFactor(DECIMAL_FACTOR)
	, numLines(0)
	, numColumnsInExportFile(0)
	, numColumns(0)
	, lastBlock(1)
	, row(new char[DEFAULT_LINE_LENGTH])
	, exeName()
	, inFileName(fileName)
	, inFileBaseName(GetBaseNameForInFile(inFileName))
	, input(NULL)
	, bOutputDescFileOnly(bOutputDescFileOnly)
	, bShowStatus(bShowStatus && !bQuiet), bQuiet(bQuiet)
	, bTestOnly(bTestOnly)
	, bOutputNonEmptyColumnHeader(false)
	, bShowBasicStatisticsOnly(false)
	, bFailOnInvalidColumns(true)
	, bExcludeSpecifiedColumns(false)
	, bOutputEmptyMissingColumns(false)
	, indexForVirtualBaseNameColumn(IGNORE)
	, indexForVirtualRowColumn(IGNORE)
	, columnType(NULL)
	, columnCharSize(NULL)
	, outputColumns(NULL)
	, columnSize(NULL), setColumns(NULL)
	, columnBase(NULL), columnVal(NULL)
	, dictionarySize(0), numVisitors(0)
	, rowsRead(0)
	, numSetColumns(0)
	, statusOutput(NULL)
	, eState(ZDW_BEGIN)
	, currentRowNumber(0)
{
	temp_buf[TEMP_BUF_LAST_POS] = '\0';

	if (!inFileName.empty())
	{
		//Check for file existence.
		struct stat buf;
		const int exists = stat(inFileName.c_str(), &buf);
		if (exists >= 0)
		{
			string cmd;
			const size_t len = inFileName.size();
			if (len >= 4 && !strcmp(inFileName.c_str() + len - 3, ".gz")) {
				cmd = "zcat ";
				cmd += inFileName;
				cmd.append(" 2>/dev/null"); //we don't need to see any chatter -- we output all relevant error codes ourselves
			} else if (len >= 5 && !strcmp(inFileName.c_str() + len - 4, ".bz2")) {
				//Streaming uncompression of .bz2 files.
				cmd = "bzip2 -d --stdout ";
				cmd += inFileName;
				cmd.append(" 2>/dev/null"); //we don't need to see any chatter -- we output all relevant error codes ourselves
			} else if (len >= 4 && !strcmp(inFileName.c_str() + len - 3, ".xz")) {
				cmd = "xzcat ";
				cmd += inFileName;
			} else if (len >= 5 && !strcmp(inFileName.c_str() + len - 4, ".zst")) {
				//Streaming uncompression of .zst files.
				cmd = "zstd -d --stdout ";
				cmd += inFileName;
				cmd.append(" 2>/dev/null"); //we don't need to see any chatter -- we output all relevant error codes ourselves
			} else {
				//Streaming text.
				cmd = "cat ";
				cmd += inFileName;
			}

			input = new BufferedInput(cmd);
		}
	} else {
		//No filename specified -- read ZDW data from stdin.
		input = new BufferedInput();
	}
}

//**************************************************
UnconvertFromZDW_Base::~UnconvertFromZDW_Base()
{
	cleanupBlock();

	delete[] this->columnType;
	delete[] this->columnCharSize;
	delete[] this->outputColumns;
	delete[] this->row;
	delete this->input;
}

string UnconvertFromZDW_Base::getVersion()
{
	std::ostringstream str;
	str << UNCONVERT_ZDW_VERSION << UNCONVERT_ZDW_VERSION_TAIL;
	return str.str();
}

//**********************************************
size_t UnconvertFromZDW_Base::readBytes(
	void* buf, //(out) buffer to write out
	const size_t len, //(in) # of bytes to read in
	const bool bHaltOnReadError) //[default=true]
{
	//Read from input source.
	const size_t result = this->input->read(buf, len);

	if ((result != len) && bHaltOnReadError)
	{
		printError(this->exeName, getInputFilename(this->inFileName));
		delete this->input;
		this->input = NULL;
		throw ZDWException(GZREAD_FAILED);
	}
	return result;
}

//**********************************************
size_t UnconvertFromZDW_Base::skipBytes(
	const size_t len) //(in) # of bytes to skip
{
	//Skip this amount of data from input source
	return this->input->skip(len);
}

//**********************************************
//A fast itoa variant that doesn't require reversing the string text
//
//Returns: the size of the string outputted
//
//POST-COND: this->temp_buf has the converted numeric string at the end
size_t UnconvertFromZDW_Base::llutoa(ULONGLONG value)
{
	size_t rem, pos = TEMP_BUF_LAST_POS;

	do
	{
		rem    = value % 10;
		value /= 10;
		this->temp_buf[--pos] = static_cast<char>(rem + 0x30); //+ '0'
	} while (value != 0);

	return TEMP_BUF_LAST_POS - pos;
}

//Signed variant of the above method.
size_t UnconvertFromZDW_Base::lltoa(SLONGLONG value)
{
	size_t rem, pos = TEMP_BUF_LAST_POS;

	//Minus sign.
	bool bMinus = false;
	if (value < 0)
	{
		bMinus = true;
		value = -value;
	}

	do
	{
		rem    = value % 10;
		value /= 10;
		this->temp_buf[--pos] = static_cast<char>(rem + 0x30); //+ '0'
	} while (value != 0);

	if (bMinus) //sign goes at beginning
		this->temp_buf[--pos] = '-';

	return TEMP_BUF_LAST_POS - pos;
}

//**********************************************
char* UnconvertFromZDW_Base::GetWord(ULONG index, char* row)
{
	if (this->version >= 9) {
		//Determine in-memory dictionary block and relative offset.
		size_t dict_memblock = 0;
		while (index >= this->dictionary_memblock_size[dict_memblock]) {
			index -= this->dictionary_memblock_size[dict_memblock];
			++dict_memblock;
			assert(dict_memblock < this->dictionary_memblock_size.size());
		}

		return this->dictionary[dict_memblock] + index;
	}

	//Build string backwards -- start with the null terminator.
	char *out = row + this->exportFileLineLength - 1;
	*out = 0;

	do
	{
		const char *textBlock = this->uniques[index].m_Char.c;

		//Unrolled loop.
		assert(BLOCKSIZE == 8); //how many elements in the block (i.e. steps in our unrolled loop)
		*(--out) = *(textBlock++);
		*(--out) = *(textBlock++);
		*(--out) = *(textBlock++);
		*(--out) = *(textBlock++);
		*(--out) = *(textBlock++);
		*(--out) = *(textBlock++);
		*(--out) = *(textBlock++);
		*(--out) = *(textBlock++);

		//Continue to a previous block, if any.
		index = this->uniques[index].m_PrevChar.n;
	} while (index > 0);

	return out;
}

//***************************************************************
// Tokenizes the inputted string into a vector of strings.
// These texts are the only columns that will be outputted.
//
// Returns: true if operation succeeded, or false on error (duplicate string definition)
bool UnconvertFromZDW_Base::setNamesOfColumnsToOutput(
	const string& csv_str,
	COLUMN_INCLUSION_RULE inclusionRule)
{
	this->namesOfColumnsToOutput.clear(); //start empty
	this->bFailOnInvalidColumns = this->bExcludeSpecifiedColumns = this->bOutputEmptyMissingColumns = false;

	switch (inclusionRule) {
		default:
		case FAIL_ON_INVALID_COLUMN: this->bFailOnInvalidColumns = true; break;
		case SKIP_INVALID_COLUMN: break;
		case EXCLUDE_SPECIFIED_COLUMNS: this->bExcludeSpecifiedColumns = true; break;
		case PROVIDE_EMPTY_MISSING_COLUMNS: this->bOutputEmptyMissingColumns = true; break;
	}

	const string delimiters = ", ";

	// Skip delimiters at beginning.
	string::size_type lastPos = csv_str.find_first_not_of(delimiters, 0);

	// Find first "non-delimiter".
	string::size_type pos = csv_str.find_first_of(delimiters, lastPos);

	//Design constraint: Forbid requesting multiple (case-insensitive) instances of a column name
	set<string, InsensitiveCompare> lowercasedNames;

	//Each iteration tokenizes a column name.
	int unsigned index = 0;
	while (string::npos != pos || string::npos != lastPos)
	{
		const string column_name = csv_str.substr(lastPos, pos - lastPos);

		//skip duplicate column names
		const bool bNewColumnName = (lowercasedNames.insert(column_name)).second;
		if (bNewColumnName) {
			// Add column name to the map.
			const bool bAdded = this->namesOfColumnsToOutput.insert(
				std::make_pair(column_name, index)).second; //output this column at this position in the row
			assert(bAdded);

			++index;
			if (column_name == VIRTUAL_EXPORT_BASENAME_COLUMN_NAME && !this->bExcludeSpecifiedColumns) {
				EnableVirtualExportBaseNameColumn();
			} else if (column_name == VIRTUAL_EXPORT_ROW_COLUMN_NAME && !this->bExcludeSpecifiedColumns) {
				EnableVirtualExportRowColumn();
			}
		} else {
			if (this->bFailOnInvalidColumns)
				return false;

			if (this->bOutputEmptyMissingColumns) {
				this->blankColumnNames[index++] = column_name;
			}
		}

		// Skip delimiters.  Note the "not_of"
		lastPos = csv_str.find_first_not_of(delimiters, pos);
		// Find next "non-delimiter"
		pos = csv_str.find_first_of(delimiters, lastPos);
	}
	return true;
}

bool UnconvertFromZDW_Base::setNamesOfColumnsToOutput(
	const vector<string> &csv_vector,
	COLUMN_INCLUSION_RULE inclusionRule)
{
	this->namesOfColumnsToOutput.clear(); //start empty
	this->bFailOnInvalidColumns = this->bExcludeSpecifiedColumns = this->bOutputEmptyMissingColumns = false;

	switch (inclusionRule) {
	default:
	case FAIL_ON_INVALID_COLUMN: this->bFailOnInvalidColumns = true; break;
	case SKIP_INVALID_COLUMN: break;
	case EXCLUDE_SPECIFIED_COLUMNS: this->bExcludeSpecifiedColumns = true; break;
	case PROVIDE_EMPTY_MISSING_COLUMNS: this->bOutputEmptyMissingColumns = true; break;
	}

	int unsigned index = 0;
	for (vector<string>::const_iterator itr = csv_vector.begin(); itr != csv_vector.end(); itr++)
	{
		// Add column name to the map.
		const string& column_name = *itr;
		const bool bAdded = this->namesOfColumnsToOutput.insert(
			std::make_pair(column_name, index)).second; //output this column at this position in the row
		if (column_name == VIRTUAL_EXPORT_BASENAME_COLUMN_NAME && !this->bExcludeSpecifiedColumns) {
			EnableVirtualExportBaseNameColumn();
		}
		if (column_name == VIRTUAL_EXPORT_ROW_COLUMN_NAME && !this->bExcludeSpecifiedColumns) {
			EnableVirtualExportRowColumn();
		}
		if (bAdded) {
			++index;
		} else {
			if (this->bFailOnInvalidColumns)
				return false; //skip duplicate column names

			if (this->bOutputEmptyMissingColumns) {
				this->blankColumnNames[index++] = column_name;
			}
		}
	}
	return true;
}

//****************************************************
ERR_CODE UnconvertFromZDW_Base::outputDescToFile(
	const vector<string>& columnNames,
	const string& outputDir, const char* filestub, //where to output the .desc.sql
	const char* ext) //extension to give the outputted .desc file (NULL=none)
{
	//Open .desc.sql file handle for output.
	string outFileName = outputDir + "/" + filestub + ".desc";
	if (ext)
		outFileName += ext;

	FILE *out = fopen(outFileName.c_str(), "w");
	if (!out) {
		statusOutput(ERROR, "%s: Could not open %s for writing\n", exeName.c_str(), outFileName.c_str());
		return FILE_CREATION_ERR;
	}

	const ERR_CODE val = outputDesc(columnNames, out);
	fclose(out);
	return val;
}

ERR_CODE UnconvertFromZDW_Base::outputDescToStdOut(
	const vector<string>& columnNames)
{
	return outputDesc(columnNames, stdout);
}

//****************************************************
ERR_CODE UnconvertFromZDW_Base::outputDesc(
	const vector<string>& columnNames,
	FILE *out)
{
	vector<string> outColumnTexts = getDesc(columnNames, "\t", "\n");
	if (outColumnTexts.empty() && !columnNames.empty())
		return UNEXPECTED_DESC_TYPE;

	//Output column texts as ordered.
	vector<string>::const_iterator strIt;
	for (strIt = outColumnTexts.begin(); strIt != outColumnTexts.end(); ++strIt) {
		fprintf(out, "%s", strIt->c_str());
	}

	return OK;
}

ERR_CODE UnconvertFromZDW_Base::GetSchema(
	ostream& stream)
{
	vector<string> outColumnTexts = getDesc(this->columnNames, " ", "");
	if (outColumnTexts.empty() && !this->columnNames.empty())
		return UNEXPECTED_DESC_TYPE;

	//Output column texts as ordered.
	vector<string>::const_iterator strIt;
	vector<string>::const_iterator begin = outColumnTexts.begin();
	for (strIt = begin; strIt != outColumnTexts.end(); ++strIt) {
		if (strIt != begin) {
			stream << ",\n";
		}
		stream << *strIt;
	}

	return OK;
}

string UnconvertFromZDW_Base::GetBaseNameForInFile(const string &inFileName)
{
	if (inFileName.empty()) {
		//return "No Input Filename Found";
		return string();
	}

	char *sourceDir = NULL;
	const char* outputBasename = NULL;
	InitDirAndBasenameFromFileName(inFileName, sourceDir, outputBasename);
	// Must get the value of outputBaseName *before* we deallocate sourceDir since the
	// character buffer used for outputBasename will be freed when we free sourceDir.
	const string inFileBaseName = outputBasename;
	if (sourceDir)
		free(sourceDir);
	return inFileBaseName;
}

//****************************************************
vector<string> UnconvertFromZDW_Base::getDesc(const vector<string>& columnNames,
	const string& name_type_separator, const string& delimiter) const
{
	const ULONG numColumns = columnNames.size();
	const ULONG numOutputColumns = numColumns + this->blankColumnNames.size();
	vector<string> outColumnTexts(numOutputColumns);
	for (size_t c = 0; c < numColumns; c++)
	{
		const int outColumnIndex = this->namesOfColumnsToOutput.empty() ? c : this->outputColumns[c];
		if (outColumnIndex != IGNORE) //only list columns that are being outputted
		{
			//This column goes at this output position.
			assert(outColumnIndex >= 0 && outColumnIndex < int(numOutputColumns)); //valid range
			outColumnTexts[outColumnIndex] =
				getColumnDesc(columnNames[c], this->columnType[c], c, name_type_separator, delimiter);

			//Error condition
			if (outColumnTexts[outColumnIndex].empty())
				return vector<string>();
		}
	}

	//When requesting absent columns for padding, give them a generic text type.
	for (map<int, string>::const_iterator iter = this->blankColumnNames.begin(); iter != this->blankColumnNames.end(); ++iter)
	{
		outColumnTexts[iter->first] =
			getColumnDesc(iter->second, TEXT, size_t(-1), name_type_separator, delimiter);
	}

	return outColumnTexts;
}

//****************************************************
string UnconvertFromZDW_Base::getColumnDesc(const string& name, UCHAR type, size_t index,
	const string& name_type_separator, const string& delimiter) const
{
	string text = name;
	text += name_type_separator;

	switch (type)
	{
		case VIRTUAL_EXPORT_FILE_BASENAME:
		case VARCHAR:
		{
			const size_t TEMP_BUFFER_SIZE=32;
			char temp[TEMP_BUFFER_SIZE];
			const int char_size = (this->columnCharSize && this->columnCharSize[index]) ? this->columnCharSize[index] : 255; //before version 7, we don't have a size value
			snprintf(temp, TEMP_BUFFER_SIZE, "varchar(%d)", char_size);
			text += temp;
		}
		break;
		case TEXT: text += "text"; break;
		case TINYTEXT: text += "tinytext"; break;
		case MEDIUMTEXT: text += "mediumtext"; break;
		case LONGTEXT: text += "longtext"; break;
		case DATETIME: text += "datetime"; break;
		case CHAR_2: text += "char(2)"; break;
		case VISID_LOW: text += "bigint(20) unsigned"; break;
		case VISID_HIGH: text += "bigint(20) unsigned"; break;
		case CHAR: text += "char(1)"; break;
		case TINY: text += "tinyint(3) unsigned"; break;
		case SHORT: text += "smallint(5) unsigned"; break;
		case VIRTUAL_EXPORT_ROW:
		case LONG: text += "int(11) unsigned"; break;
		case LONGLONG: text += "bigint(20) unsigned"; break;
		case TINY_SIGNED: text += "tinyint(3)"; break;
		case SHORT_SIGNED: text += "smallint(5)"; break;
		case LONG_SIGNED: text += "int(11)"; break;
		case LONGLONG_SIGNED: text += "bigint(20)"; break;
		case DECIMAL: text += "decimal(24,12)"; break;
		default: return string(); //unexpected type
	}

	text += delimiter;
	return text;
}

ERR_CODE UnconvertFromZDW_Base::outputMetadataToFile(
	const string& outputDir, const char* filestub) const
{
	const string outFileName = outputDir + "/" + filestub + ".metadata";

	FILE *out = fopen(outFileName.c_str(), "w");
	if (!out) {
		statusOutput(ERROR, "%s: Could not open %s for writing\n", exeName.c_str(), outFileName.c_str());
		return FILE_CREATION_ERR;
	}

	const ERR_CODE val = outputMetadata(out);
	fclose(out);
	return val;
}

ERR_CODE UnconvertFromZDW_Base::outputMetadataToStdOut() const
{
	return outputMetadata(stdout);
}

//****************************************************
ERR_CODE UnconvertFromZDW_Base::outputMetadata(FILE *out) const
{
	const set<string>& keys = this->metadataOptions.keys;

	//Filter output set
	set<string> outKeys;
	set<string>::const_iterator it;
	for (it = keys.begin(); it != keys.end(); ++it)
	{
		if (!this->metadataOptions.bAllowMissingKeys &&
			this->metadata.find(*it) == this->metadata.end())
		{
			return METADATA_KEY_NOT_PRESENT;
		}

		outKeys.insert(*it);
	}

	//Output metadata keys
	map<string, string>::const_iterator m_it;
	if (outKeys.empty()) {
		for (m_it = this->metadata.begin(); m_it != this->metadata.end(); ++m_it) {
			if (this->metadataOptions.bOnlyMetadataKeys) {
				fprintf(out, "%s\n", m_it->first.c_str());
			} else {
				fprintf(out, "%s=%s\n", m_it->first.c_str(), m_it->second.c_str());
			}
		}
	} else {
		for (it = outKeys.begin(); it != outKeys.end(); ++it) {
			if (this->metadataOptions.bOnlyMetadataKeys) {
				fprintf(out, "%s\n", it->c_str());
			} else {
				m_it = this->metadata.find(*it);
				fprintf(out, "%s=%s\n", it->c_str(), (m_it != this->metadata.end()) ? m_it->second.c_str() : "");
			}
		}
	}

	return OK;
}

//****************************************
void UnconvertFromZDW_Base::cleanupBlock()
{
	//Deinit for this block.
	for (size_t i = 0; i < this->dictionary.size(); ++i)
		delete[] this->dictionary[i];
	delete[] this->uniques;
	delete[] this->visitors;
	delete[] this->columnSize;
	delete[] this->setColumns;
	delete[] this->columnBase;
	delete[] this->columnVal;

	this->dictionary.clear();
	this->dictionary_memblock_size.clear();
	this->uniques = NULL;
	this->visitors = NULL;
	this->columnSize = NULL;
	this->setColumns = NULL;
	this->columnBase = NULL;
	this->columnVal = NULL;
}

//****************************************************
ERR_CODE UnconvertFromZDW_Base::parseBlockHeader()
{
	assert(this->dictionary.empty());
	assert(this->dictionary_memblock_size.empty());
	assert(!this->uniques);
	assert(!this->visitors);
	assert(!this->columnSize);
	assert(!this->setColumns);
	assert(!this->columnBase);
	assert(!this->columnVal);

	assert(sizeof(UCHAR) == 1);
	assert(sizeof(USHORT) == 2);

	readLineLength();

	readDictionary();

	readColumnFieldStats();

	this->rowsRead = 0;
	return OK;
}

void UnconvertFromZDW_Base::readLineLength()
{
	if (this->version >= 3)
	{
		//Each block stores this header info.
		readBytes(&this->numLines, 4);
		if (this->version >= 6) {
			assert(sizeof(this->exportFileLineLength) == 4);
			readBytes(&this->exportFileLineLength, 4);
		} else {
			// Before version 6, the format only allowed a 2-byte length field.
			// So, we're reading the 2-byte field
			// and storing it in the properly sized variable.
			USHORT t_version;
			readBytes(&t_version, 2);
			this->exportFileLineLength = t_version;
		}
		readBytes(&this->lastBlock, 1);

		if (this->exportFileLineLength > DEFAULT_LINE_LENGTH)
		{
			delete[] this->row;
			this->row = new char[this->exportFileLineLength];
		}
	}
	if (this->bShowBasicStatisticsOnly) {
		this->statusOutput(INFO, "Max line length = %lu\n", static_cast<long unsigned>(this->exportFileLineLength));
	}
}

void UnconvertFromZDW_Base::readDictionary()
{
	UCHAR indexSize;

	//Read byte size of the index values.
	this->dictionarySize = 0;
	readBytes(&indexSize, 1);
	if (indexSize != 0) //0 size indicates no values to read in
	{
		indexBytes sb;
		sb.n = 0;
		readBytes(sb.c, indexSize);
		this->dictionarySize = sb.n;
	}

	//Read dictionary.
	if (this->version >= 9) {
		if (!this->bQuiet)
			this->statusOutput(INFO, "Reading %" PF_LLU " byte dictionary\n", this->dictionarySize);
		if (this->bShowBasicStatisticsOnly) {
			skipBytes(this->dictionarySize);
		} else {
			//Create large dictionary as smaller memory chunks to work around memory fragmentation.
			const size_t MAX_DICTIONARY_CHUNK = 500000000; //500M -- should be much larger than any single possible entry
			const size_t numChunks = size_t(ceil(this->dictionarySize / float(MAX_DICTIONARY_CHUNK)));
			this->dictionary.reserve(numChunks);
			this->dictionary_memblock_size.reserve(numChunks);

			ULONGLONG sizeLeft = this->dictionarySize;
			while (sizeLeft > MAX_DICTIONARY_CHUNK) {
				readDictionaryChunk(MAX_DICTIONARY_CHUNK);
				sizeLeft -= MAX_DICTIONARY_CHUNK;
			}
			readDictionaryChunk(sizeLeft);
		}
	} else {
		if (!this->bShowBasicStatisticsOnly) {
			this->uniques = new UniquesPart[this->dictionarySize + 1];
			memset(this->uniques, 0, (this->dictionarySize + 1) * sizeof(UniquesPart));
		}
		if (!this->bQuiet)
			this->statusOutput(INFO, "Reading %" PF_LLU " uniques\n", this->dictionarySize);

		if (this->bShowBasicStatisticsOnly) {
			//Just skip through this data.
			skipBytes(this->dictionarySize * (BLOCKSIZE + indexSize));
		} else {
			const ULONG OUTPUT_MOD = 1000000;
			this->uniques[0].m_Char.n = 0;
			this->uniques[0].m_PrevChar.n = 0;
			unsigned int c;
			for (c = 1; c <= this->dictionarySize; c++)
			{
				readBytes(this->uniques[c].m_Char.c, BLOCKSIZE);
				readBytes(this->uniques[c].m_PrevChar.c, indexSize);
				if (this->bShowStatus && !(c % OUTPUT_MOD))
				{
					this->statusOutput(INFO, "\r%u", c - 1);
				}
			}
			if (this->bShowStatus && indexSize != 0)
				this->statusOutput(INFO, "\r%u\n", c - 1);
		}
	}

	readVisitorDictionary();
}

void UnconvertFromZDW_Base::readDictionaryChunk(const size_t size)
{
	if (!size)
		return;

	//With multiple memory chunks,
	//stitch any final partial entry in the previous chunk onto the front of this chunk
	//to create an unbroken string.
	const char *textToStitch = NULL;
	size_t bytesToStitch = 0;
	const size_t numChunks = this->dictionary.size();
	if (numChunks) {
		const size_t prevChunk = numChunks - 1;
		const char *endOfBlock = this->dictionary[prevChunk] + this->dictionary_memblock_size[prevChunk] - 1;
		textToStitch = endOfBlock;
		while (*textToStitch)
			--textToStitch;
		bytesToStitch = endOfBlock - textToStitch;

		++textToStitch; //stitch after null terminator, i.e., residual after last complete entry
	}

	char *chunk = new char[size + bytesToStitch];

	if (bytesToStitch) {
		//Move partial entry from end of previous block to new block.
		strcpy(chunk, textToStitch);
		this->dictionary_memblock_size[numChunks - 1] -= bytesToStitch;
	}

	try {
		readBytes(chunk + bytesToStitch, size);
	} catch (ZDWException &ex) {
		delete[] chunk;
		throw;
	}

	this->dictionary.push_back(chunk);
	this->dictionary_memblock_size.push_back(size + bytesToStitch);
}

void UnconvertFromZDW_Base::readVisitorDictionary()
{
	if (this->version < 8)
	{
		UCHAR vIndexSize;

		//Read byte size of visitor IDs.
		this->numVisitors = 0;
		readBytes(&vIndexSize, 1);
		if (vIndexSize != 0) //0 size indicates no values to read in
		{
			indexBytes sb;
			sb.n = 0;
			readBytes(sb.c, vIndexSize);
			this->numVisitors = sb.n;
		}
		this->visitors = new VisitorPart[this->numVisitors + 1];
		memset(this->visitors, 0, (this->numVisitors + 1) * sizeof(VisitorPart));
		if (!this->bQuiet)
			this->statusOutput(INFO, "Reading %" PF_LLU " visitor indices\n", this->numVisitors);

		//Read visitor IDs
		if (this->bShowBasicStatisticsOnly) {
			//Just skip through this data.
			skipBytes(this->numVisitors * (8 + vIndexSize));
		} else {
			const ULONG OUTPUT_MOD = 1000000;
			this->visitors[0].m_VID = 0;
			this->visitors[0].m_PrevID.n = 0;
			unsigned int c;
			for (c = 1; c <= this->numVisitors; c++)
			{
				readBytes(&(this->visitors[c].m_VID), 8);
				readBytes(this->visitors[c].m_PrevID.c, vIndexSize);
				if (this->bShowStatus && !(c % OUTPUT_MOD))
				{
					this->statusOutput(INFO, "\r%u", c - 1);
				}
			}
			if (this->bShowStatus && vIndexSize != 0)
				this->statusOutput(INFO, "\r%u\n", c - 1);
		}
	}
}

void UnconvertFromZDW_Base::readColumnFieldStats()
{
	this->columnSize = new UCHAR[this->numColumns];
	readBytes(this->columnSize, this->numColumnsInExportFile);
	this->columnBase = new ULONGLONG[this->numColumns];
	ULONG numColumnsUsedFromExportFile = 0;
	for (unsigned int c = 0; c < this->numColumnsInExportFile; ++c)
	{
		if (this->columnSize[c])
		{
			readBytes(this->columnBase + c, 8);
			++numColumnsUsedFromExportFile;
		} else {
			this->columnBase[c] = 0;
		}
	}
	assert(numColumnsUsedFromExportFile <= this->numColumnsInExportFile);
	this->numSetColumns = static_cast<long>(ceil(numColumnsUsedFromExportFile / 8.0));
	this->setColumns = new unsigned char[this->numSetColumns];
	this->columnVal = new storageBytes[this->numColumns];
	memset(this->columnVal, 0, this->numColumns * sizeof(storageBytes));

	if (UseVirtualExportBaseNameColumn()) {
		// This causes the VIRTUAL_EXPORT_FILE_BASENAME column to fall through to the default value
		// code path. That is where we actually update the column value. See outputDefault(...)
		this->columnSize[this->indexForVirtualBaseNameColumn] = 0;
		this->columnBase[this->indexForVirtualBaseNameColumn] = 0;
	}
	if (UseVirtualExportRowColumn()) {
		// This causes the VIRTUAL_EXPORT_ROW column to fall through to the default value
		// code path. That is where we actually update the column value. See outputDefault(...)
		this->columnSize[this->indexForVirtualRowColumn] = 0;
		this->columnBase[this->indexForVirtualRowColumn] = 0;
	}
}

string UnconvertFromZDW_Base::getBlockHeaderString() const
{
	string header = "***ZDW BLOCK HEADER*** NON-EMPTY COLUMNS: ";

	bool bFirst = true;
	for (size_t c = 0; c < this->numColumns; ++c)
	{
		if (this->outputColumns[c] != IGNORE) {
			if (this->columnSize[c]) {
				if (bFirst) {
					bFirst = false;
				} else {
					header += ",";
				}

				header += this->columnNames[c];
			}
		}
	}
	header += newline;

	return header;
}

//***************************************************************
//
// Read header data for ZDW file.
//
ERR_CODE UnconvertFromZDW_Base::readHeader()
{
	delete[] this->columnType;
	this->columnType = NULL;
	delete[] this->columnCharSize;
	this->columnCharSize = NULL;

	if (!isReadOpen())
		return FILE_OPEN_ERR;
	if (this->eState != ZDW_BEGIN)
		return HEADER_ALREADY_READ_ERR;

	//1. Parse version #.
	readBytes(&this->version, 2);
	if (this->version > UNCONVERT_ZDW_VERSION)
		return UNSUPPORTED_ZDW_VERSION_ERR;

	if (this->bShowBasicStatisticsOnly)
		this->statusOutput(INFO, "File version %d\n", static_cast<int>(this->version));

	this->metadata.clear();
	if (this->version >= 11)
	{
		//2. Metadata block
		ULONG metadata_size;
		readBytes(&metadata_size, 4);

		if (this->bShowBasicStatisticsOnly)
			this->statusOutput(INFO, "Metadata block size = %u\n", metadata_size);

		char *metadata_block = static_cast<char*>(malloc(metadata_size));
		readBytes(metadata_block, metadata_size);
		const char *key = metadata_block, *value;
		while (key - metadata_block < static_cast<long>(metadata_size)) {
			value = key + strlen(key) + 1; //skip past null terminator
			this->metadata[string(key)] = string(value);

			//advance to next key-value pair
			key = value + strlen(value) + 1; //skip past null terminator
		}
		free(metadata_block);
	} else if (this->version <= 2) {
		//2a. Parse file attributes (before version 3).
		if (this->version == 1)
			this->decimalFactor = DECIMAL_FACTOR_VERSION_1;

		readBytes(&this->numLines, 4);
		readBytes(&this->exportFileLineLength, 2);
		if (this->exportFileLineLength > DEFAULT_LINE_LENGTH)
		{
			delete[] this->row;
			this->row = new char[this->exportFileLineLength];
		}
	}

	//3. Parse column names.
	this->columnNames.clear();
	readBytes(row, 1);
	while (row[0])
	{
		size_t p = 0;
		do {
			readBytes(&(row[++p]), 1);
		} while (row[p]);
		this->columnNames.push_back(row);
		readBytes(row, 1); //pass null terminator
	}

	this->numColumnsInExportFile = this->columnNames.size();

	// 3b Detour To Start Setup For Virtual Columns (i.e. VIRTUAL_EXPORT_FILE_BASENAME, VIRTUAL_EXPORT_ROW_COLUMN_NAME)
	if (UseVirtualExportBaseNameColumn()) {
		this->indexForVirtualBaseNameColumn = this->columnNames.size();
		this->columnNames.push_back(VIRTUAL_EXPORT_BASENAME_COLUMN_NAME);
		this->virtualLineLength += inFileBaseName.size() + 1;
	}
	if (UseVirtualExportRowColumn()) {
		this->indexForVirtualRowColumn = this->columnNames.size();
		this->columnNames.push_back(VIRTUAL_EXPORT_ROW_COLUMN_NAME);
		this->virtualLineLength += llutoa(SIZE_MAX) + 1;
	}
	this->numColumns = this->columnNames.size();

	// 3c Back to Parsing Columns
	//Flag which columns to output, by index.
	delete[] this->outputColumns;
	this->outputColumns = new int[this->numColumns];
	memset(this->outputColumns,
		(this->namesOfColumnsToOutput.empty() ? 0 : IGNORE), //if no column names are set, output all columns
		this->numColumns * sizeof(int));
	map<int unsigned, int unsigned> encounteredValues; //output position --> column index

	//Case-insensitive matching
	map<string, int unsigned, InsensitiveCompare> columnsCopy;
	for (map<string, int unsigned>::const_iterator it = this->namesOfColumnsToOutput.begin();
			it != this->namesOfColumnsToOutput.end(); ++it)
		columnsCopy[it->first] = it->second;

	//Scan the columns in the file.
	int unsigned outIndex = 0;
	for (int unsigned index = 0; index < this->numColumns; ++index)
	{
		//Do we have an explicit inclusion/exclusion for this column?
		map<string, int unsigned>::iterator findIt = columnsCopy.find(this->columnNames[index]);

		if (this->bExcludeSpecifiedColumns) {
			//Include only non-excluded columns in order of appearance.
			if (findIt == columnsCopy.end()) {
				this->outputColumns[index] = outIndex; //output the index'th column to this position in the output row
				++outIndex; //next non-exluded column will be output at the next position
			}
		} else {
			//If this column is one of the specified output columns, then mark this column index for output.
			if (findIt != columnsCopy.end()) {
				outIndex = int(findIt->second);
				this->outputColumns[index] = outIndex; //output the index'th column to this position in the output row
				encounteredValues[outIndex] = index;   //track positions we will output to, in case we need to compact the ordering
				columnsCopy.erase(findIt);
			}
		}
	}

	//Some column names were specified for inclusion, but they do not exist in this file.
	if (!columnsCopy.empty() && !this->bExcludeSpecifiedColumns) {
		//Flag as an error?
		if (this->bFailOnInvalidColumns)
			return BAD_REQUESTED_COLUMN; //column doesn't exist in the file -- don't attempt to process further.

		//If outputting empty values for requested columns that don't exist,
		//  then we will fill in the missing indexes with blank values.
		if (this->bOutputEmptyMissingColumns) {
			//Identify which column names aren't in the file.
			for (map<string, int unsigned>::const_iterator iter = columnsCopy.begin();
				iter != columnsCopy.end(); ++iter)
			{
				this->blankColumnNames[iter->second] = iter->first;
			}
		} else {
			//Were any requested column names encountered in this file?
			if (encounteredValues.empty())
				return NO_COLUMNS_TO_OUTPUT; //no requested columns exist in the file -- don't attempt to process further.

			//Otherwise, we expect to have a gap in our output column indexing.
			//Compact the column output ordering.
			//EX:
			//    Given a sequence of existing output column positioning of [2,1,3,5],
			//    we compact the set of values, resulting in the sequence [1,0,2,3], which contains no gaps.
			int unsigned nextIndex = 0;
			for (map<int unsigned, int unsigned>::const_iterator valIter = encounteredValues.begin();
					valIter != encounteredValues.end(); ++valIter, ++nextIndex)
			{
				const int unsigned out_index = valIter->first;
				if (out_index != nextIndex) {
					const int file_index = valIter->second;
					//We've identified a gap in output index sequencing -- compact.
					assert(this->outputColumns[file_index] > int(nextIndex)); //index should need to be decreased when compacting
					this->outputColumns[file_index] = nextIndex;
				}
			}
		}
	}

	//4. Parse column attributes.
	this->columnType = new UCHAR[this->numColumns];
	readBytes(this->columnType, this->numColumnsInExportFile);

	//4b. Parse column char sizes (version 7+).
	if (this->version >= 7) {
		this->columnCharSize = new USHORT[this->numColumns];
		readBytes(this->columnCharSize, this->numColumnsInExportFile * 2);
	}

	// 5. Finish Setup For Virtual Columns (i.e. VIRTUAL_EXPORT_FILE_BASENAME)

	if (UseVirtualExportBaseNameColumn()) {
		this->columnType[this->indexForVirtualBaseNameColumn] = VIRTUAL_EXPORT_FILE_BASENAME;
		if (this->columnCharSize) {
			this->columnCharSize[this->indexForVirtualBaseNameColumn] = inFileBaseName.size() + 1;
		}
	}
	if (UseVirtualExportRowColumn()) {
		this->columnType[this->indexForVirtualRowColumn] = VIRTUAL_EXPORT_ROW;
		if (this->columnCharSize) {
			this->columnCharSize[this->indexForVirtualRowColumn] = 0;
		}
	}

	setState(ZDW_PARSE_BLOCK_HEADER);
	return OK;
}

//***************************************************************
//Output the default value for this var type to the indicated file.
template <typename T>
void UnconvertFromZDW<T>::outputDefault(T& buffer, const UCHAR type)
{
	switch (type)
	{
		case CHAR:
		case VARCHAR:
		case TEXT:
		case TINYTEXT:
		case MEDIUMTEXT:
		case LONGTEXT:
		case DATETIME:
		case CHAR_2:
			buffer.writeEmpty();
			break;
		case VISID_HIGH:
			buffer.write("0", 1);
			break;
		case VISID_LOW: //this type is handled by the calling function (currently, readNextRow)
			assert(!"VISID_LOW shouldn't be considered here");
			break;
		case TINY: case TINY_SIGNED:
		case SHORT: case SHORT_SIGNED:
		case LONG: case LONG_SIGNED:
		case LONGLONG: case LONGLONG_SIGNED:
			buffer.write("0", 1);
			break;
		case DECIMAL:
			buffer.write("0.000000000000", 14);
			break;
		case VIRTUAL_EXPORT_FILE_BASENAME:
			buffer.write(inFileBaseName.c_str(), inFileBaseName.size());
			break;
		case VIRTUAL_EXPORT_ROW:
		{
			const size_t currentRowStrSize = this->llutoa(GetCurrentRowNumber());
			buffer.write(this->temp_buf + TEMP_BUF_LAST_POS - currentRowStrSize, currentRowStrSize);
			break;
		}
		default:
			assert(!"Unsupported type");
			break;
	}
}

//****************************************************
template <typename T>
ERR_CODE UnconvertFromZDW<T>::readNextRow(T& buffer)
{
	const size_t TEMP_BUFFER_SIZE=64;
	long u = 0;
	char *pos;
	ULONG index;
	ULONGLONG visid_low = 0;
	int tempLength;
	char temp[TEMP_BUFFER_SIZE];
	bool bColumnWritten = false;

	IncrementCurrentRowNumber();

	//1. Read 'sameness' bit flags.
	readBytes(this->setColumns, this->numSetColumns); //bit flags -- are field values the same as in the previous row?

	//2. Output column values.
	for (size_t c = 0; c < this->numColumns; ++c)
	{
		const UCHAR ct = this->columnType[c];
		if (ct == VISID_LOW) {
			//Output the visid_low value, if selected for output.
			if (this->outputColumns[c] != IGNORE) {
				if (bColumnWritten)
					buffer.writeSeparator(tab, 1);
				tempLength = llutoa(visid_low);
				buffer.write(this->temp_buf + TEMP_BUF_LAST_POS - tempLength, tempLength);
			}
			continue; //handled along with the adjacent VISID_HIGH column
		}

		if (this->outputColumns[c] == IGNORE) //if we don't need to output this column index, skip it
		{
			//Parse only to maintain position in source file -- skip the rest of processing.
			if (this->columnSize[c])
			{
				storageBytes& val = this->columnVal[c];
				if (this->setColumns[u / 8] & (1u << (u % 8))) //is the bit for this column set?
				{
					//This column's value is different from that of the previous row,
					//so we need to read the new value to stay current in the data stream.
					val.n = 0;
					readBytes(val.c, this->columnSize[c]);
				}
				++u;

				if (ct == VISID_HIGH) {
					//Store the value for visid_low, to output when we get to its column index (if it's not excluded).
					index = val.n + this->columnBase[c];
					if (index > this->numVisitors)
						return CORRUPTED_DATA_ERROR;

					visid_low = this->visitors[this->visitors[index].m_PrevID.n].m_VID;
				}
			}
			continue;
		}

		if (bColumnWritten) //tab-delineated columns
			buffer.writeSeparator(tab, 1);

		if (!this->columnSize[c])
		{
			outputDefault(buffer, ct);
			//if (ct == VISID_HIGH) visid_low = 0;
			//in order to write out a default value for visid_low, this logic should technically happen here, and in the "if (this->outputColumns[c] == IGNORE)" block above.
			//However, since visid_low already defaults to 0 above, we don't actually need to execute this check and assignment and slow things down.
		} else {
			//3. Read new value of this column when it is not the same as that of the previous row.
			storageBytes& val = this->columnVal[c];
			if (this->setColumns[u / 8] & (1u << (u % 8))) //is the bit for this column set?
			{
				//This column's value is different from that of the previous row,
				//so read the new value.
				val.n = 0;
				readBytes(val.c, this->columnSize[c]);
			}
			++u;

			//4. Output this column's value, based on type.
			switch (ct)
			{
				case VIRTUAL_EXPORT_FILE_BASENAME: assert(!"VIRTUAL_EXPORT_FILE_BASENAME should only get default value"); break;
				case VIRTUAL_EXPORT_ROW: assert(!"VIRTUAL_EXPORT_ROW should only get default value"); break;
				case VISID_LOW: assert(!"VISID_LOW should be skipped"); break;
				case VARCHAR:
				case TEXT:
				case TINYTEXT:
				case MEDIUMTEXT:
				case LONGTEXT:
				case DATETIME:
				case CHAR_2:
					if (val.n)
					{
						index = val.n + this->columnBase[c];
						if (index > this->dictionarySize)
							return CORRUPTED_DATA_ERROR;
						pos = GetWord(index, row);

						// In ZDW v9 or higher, all of the column strings are stored
						// in a large persistent map that isn't modified throughout the
						// ZDW process.  Thus, we can simply adjust our buffer pointers
						// in v9.  However, in earlier formats the data is stored in
						// a dictionary tree, which requires copying/reversing in
						// order to extract data from correctly.  This reversed copy
						// of the data is stored in `row`, which is reused from column
						// to column.  Thus, we must copy the data out.
						if (this->version >= 9) {
							buffer.writePtr(pos, strlen(pos));
						} else {
							buffer.write(pos, strlen(pos));
						}
					} else {
						//There's an empty field value.
						outputDefault(buffer, ct);
					}
					break;
				case VISID_HIGH:
					index = val.n + this->columnBase[c];
					if (index > this->numVisitors)
						return CORRUPTED_DATA_ERROR;

					tempLength = llutoa(this->visitors[index].m_VID);
					buffer.write(this->temp_buf + TEMP_BUF_LAST_POS - tempLength, tempLength);
					//Store the value for visid_low, to output when we get to its column index (if it's not excluded).
					visid_low = this->visitors[this->visitors[index].m_PrevID.n].m_VID;
					break;
				case CHAR:
					if (this->columnVal[c].n) {
						if (this->version >= 5) {
							//Deserialize character text (possibly an escaped character sequence).
							const ULONGLONG chartuple = val.n + this->columnBase[c];
							temp[0] = static_cast<char>(chartuple);
							if (temp[0] != '\\') {
								if (!temp[0]) //there's an empty field for this column-row
									outputDefault(buffer, ct);
								else
									buffer.write(temp, 1);
							} else {
								//Output escaped character (e.g. "\\\t")
								temp[1] = static_cast<char>(chartuple / 256);
								buffer.write(temp, 2);
							}
						} else {
							//Before version 5, the converter included no more than one byte per char field.
							temp[0] = static_cast<char>(val.n);
							buffer.write(temp, 1);
						}
					} else {
						outputDefault(buffer, CHAR);
					}
					break;
				case TINY:
				case SHORT:
				case LONG:
				case LONGLONG:
					tempLength = llutoa(val.n ? val.n + this->columnBase[c] : 0);
					buffer.write(this->temp_buf + TEMP_BUF_LAST_POS - tempLength, tempLength);
					break;
				case TINY_SIGNED:
				case SHORT_SIGNED:
				case LONG_SIGNED:
				case LONGLONG_SIGNED:
					tempLength = lltoa(val.n ? val.n + this->columnBase[c] : 0);
					buffer.write(this->temp_buf + TEMP_BUF_LAST_POS - tempLength, tempLength);
					break;
				case DECIMAL:
					if (val.n)
					{
						if (this->version >= 4)
						{
							index = val.n + this->columnBase[c];
							if (index > this->dictionarySize)
								return CORRUPTED_DATA_ERROR;
							pos = GetWord(index, row);
							buffer.write(pos, strlen(pos));
						} else { //version 1-3
							tempLength = snprintf(temp, TEMP_BUFFER_SIZE, "%0.12lf", (val.n + this->columnBase[c]) / this->decimalFactor);
							buffer.write(temp, tempLength);
						}
					} else {
						outputDefault(buffer, DECIMAL);
					}
					break;
			}
		}
		bColumnWritten = true;
	}

	//Done with line.
	buffer.writeEndline(newline, 1);

	++this->rowsRead;

	return OK;
}

//******************************************************
//
// Parse a "block" of a ZDW file.
//    It also outputs progress and can test a ZDW file for apparent correctness.
//
template <typename T>
ERR_CODE UnconvertFromZDW<T>::parseNextBlock(T& buffer)
{
	ERR_CODE eRet = parseBlockHeader();
	if (eRet != OK)
		return eRet;

	printBlockHeader(buffer);

	if (!this->bQuiet)
		this->statusOutput(INFO, "Reading %u rows\n", this->numLines);

	//Show compression statistics when both test and statistics modes are set.
	ULONGLONG equalityBitsSet = 0;
	vector<ULONG> equalityBitsInColumn(this->numSetColumns * 8, 0);

	//If testing, or showing stats, don't actually uncompress any data.
	if (this->bTestOnly ||
		//when showing stats, note we only need to scan through this block if there is another one following
		(this->bShowBasicStatisticsOnly && !isLastBlock()))
	{
		ULONG index;

		//Read in the data and ensure lookup indices are valid, but output nothing.
		//This code should mirror the read format of the non-test code below.
		while (this->rowsRead < this->numLines && !isFinished())
		{
			//Process a row.
			readBytes(this->setColumns, this->numSetColumns); //bit flags -- are fields same as last row?

			long u = 0;
			for (size_t c = 0; c < this->numColumnsInExportFile; ++c)
			{
				if (this->columnType[c] == VISID_LOW)
					continue; //handled along with the adjacent VISID_HIGH column

				if (!this->columnSize[c])
					continue;

				storageBytes& val = this->columnVal[c];
				if (this->setColumns[u / 8] & (1u << (u % 8))) //is the bit for this column set?
				{
					//This column's value is different from that of the previous row,
					//so read the new value.
					val.n = 0;
					readBytes(val.c, this->columnSize[c]);
					if (this->bShowBasicStatisticsOnly) {
						++equalityBitsSet;
						++equalityBitsInColumn[u];
					}
				}
				++u;

				//Validate the field code.
				if (this->bTestOnly) {
					switch (this->columnType[c])
					{
						case VIRTUAL_EXPORT_FILE_BASENAME: assert(!"VIRTUAL_EXPORT_FILE_BASENAME should only get default value"); break;
						case VISID_LOW: assert(!"VISID_LOW should be skipped"); break;
						case VARCHAR:
						case TEXT:
						case TINYTEXT:
						case MEDIUMTEXT:
						case LONGTEXT:
						case DATETIME:
						case CHAR_2:
							if (val.n)
							{
								index = val.n + this->columnBase[c];
								if (index > this->dictionarySize)
									return CORRUPTED_DATA_ERROR;
							}
							break;
						case VISID_HIGH:
							index = val.n + this->columnBase[c];
							if (index > this->numVisitors)
								return CORRUPTED_DATA_ERROR;
							break;
						case CHAR:
						case TINY: case TINY_SIGNED:
						case SHORT: case SHORT_SIGNED:
						case LONG: case LONG_SIGNED:
						case LONGLONG: case LONGLONG_SIGNED:
							break; //nothing to test
						case DECIMAL:
							if (val.n)
							{
								if (this->version >= 4)
								{
									index = val.n + this->columnBase[c];
									if (index > this->dictionarySize)
										return CORRUPTED_DATA_ERROR;
								}
							}
							break;
					}
				}
			}

			//Done with line.
			++this->rowsRead;
		}
	} else if (!this->bShowBasicStatisticsOnly) {
		//Normal parsing and output of the data.

		//Each iteration processes one row.
		while (this->rowsRead < this->numLines && !isFinished())
		{
			//Process a row.
			eRet = readNextRow(buffer);
			if (eRet != OK)
				return eRet;

			//Progress.
			if (this->bShowStatus && !(this->rowsRead % 10000))
			{
				this->statusOutput(INFO, "\r%u", this->rowsRead);
			}
		}

		//Final progress for this block.
		if (this->bShowStatus)
			this->statusOutput(INFO, "\r%u\n", this->rowsRead);
	}

	//Finished unconverting this block.
	if (this->rowsRead != this->numLines &&
		//when looking at statistics, we are only parsing rows when we are not in the final block
		(!this->bShowBasicStatisticsOnly || !isLastBlock()))
	{
		printError(this->exeName, getInputFilename(this->inFileName));
		this->statusOutput(INFO, "Rows unpacked (%u) does not match expected (%u)\n\n",
			this->rowsRead, this->numLines);
		return ROW_COUNT_ERR;
	}

	//Optional compression characteristic display
	if (equalityBitsSet) {
		size_t nonEmptyColumns = 0;
		for (size_t c = 0; c < this->numColumnsInExportFile; ++c) {
			if (this->columnSize[c])
				++nonEmptyColumns;
		}

		this->statusOutput(INFO, "Equality delta bits set: %" PF_LLU " (%0.1f%%) (rows=%u, columns=%u, bit vector width=%ld bytes, non-empty columns=%zu (%0.1f%%)\n",
			equalityBitsSet, equalityBitsSet*100 / float(this->numLines * this->numSetColumns * 8),
			this->numLines, this->numColumnsInExportFile, this->numSetColumns,
			nonEmptyColumns, nonEmptyColumns*100/float(this->numColumnsInExportFile));

		for (size_t c = 0; c < nonEmptyColumns; ++c) {
			this->statusOutput(INFO, "%u ", equalityBitsInColumn[c]);
		}
		this->statusOutput(INFO, "\n");
	}

	if (isLastBlock() && !this->bQuiet && !this->bShowBasicStatisticsOnly)
	{
		this->statusOutput(INFO, "%s %s\n\n",
				getInputFilename(this->inFileName).c_str(), this->bTestOnly ? "tested good" : "uncompressed");
	}

	return OK;
}

//*********************************************************************************
template <typename T>
bool UnconvertFromZDW<T>::printBlockHeader(T& buffer)
{
	if (!this->bOutputNonEmptyColumnHeader)
		return false;

	const string header = this->getBlockHeaderString();

	buffer.writeRawLine(header.c_str(), header.size());

	return true;
}

//*********************************************************************************
//
// Performs the entire decompression of a ZDW file
//    (and .desc and .metadata files, if requested) to an output file/stream.
//    Also provides a progress indicator and test-only functionality.
//
template<typename BufferedOutput_T>
ERR_CODE UnconvertFromZDWToFile<BufferedOutput_T>::unconvert(
	const char* binaryName, //name of executable
	const char* outputBasename, //name of output file (NULL = same as input file's basename)
	const char* ext, //extension to give to output file names (NULL = none)
	const char* specifiedDir, //use if non NULL and non-empty
	bool bStdout)             //if true, direct unconverted text to stdout and don't output
	                          //.desc or .metadata files
{
	ERR_CODE eRet = OK;
	ERR_CODE eDescErr;
	ERR_CODE eMetadataErr;
	char *sourceDir = NULL;
	const char *filestub = NULL;

	if (binaryName != NULL && strlen(binaryName) > 0) {
		this->exeName = binaryName;
	}

	if (!this->statusOutput) {
		//When outputting status messages, where do they get outputted?
		this->statusOutput = bStdout ? stdErrStatusOutputCallback : defaultStatusOutputCallback;
	}

	if (!this->isReadOpen()) {
		this->statusOutput(ERROR, "%s: Could not open %s for reading\n", this->exeName.c_str(), getInputFilename(this->inFileName).c_str());
		return FILE_OPEN_ERR;
	}

	if (!this->inFileName.empty()) {
		//Reading from a file.
		InitDirAndBasenameFromFileName(this->inFileName, sourceDir, filestub);
	} else {
		//When reading from stdin...

		//...and no output filename has been provided, force stream output to stdout.
		if (!outputBasename)
			bStdout = true;
		//...and a filename has been provided, but no output directory, then output to current dir
		if (!specifiedDir || (strlen(specifiedDir) == 0))
			specifiedDir = "."; //set whether sending to stdout or not
		//We're reading from stdin.
		filestub = "stdin";
	}

	const string outputDir = (specifiedDir && (strlen(specifiedDir) > 0)) ? specifiedDir : sourceDir;

	//If no output filename is specified, use the input filename as the default.
	if (!outputBasename)
		outputBasename = filestub;

	if (this->bShowStatus) {
		const char *status;
		if (this->bShowBasicStatisticsOnly) {
			status = "Showing statistics";
		} else if (this->bTestOnly) {
			status = "Testing";
		} else if (this->bOutputDescFileOnly) {
			status = "Outputting .desc file only";
		} else if (this->metadataOptions.bOutputOnlyMetadata) {
			status = "Outputting .metadata file only";
		} else {
			status = "Processing";
		}
		this->statusOutput(INFO, "\n%s %s\n", filestub, status);
	}

	//1. Read header info.
	eRet = this->readHeader();
	if (eRet != OK)
	{
		if (eRet == UNSUPPORTED_ZDW_VERSION_ERR)
			this->statusOutput(ERROR, "%s: %s is newer (version %d) than supported version (%d)\n%s\n", this->exeName.c_str(), filestub, this->version, UnconvertFromZDW_Base::UNCONVERT_ZDW_VERSION, this->version > 10000 ? "Maybe you are trying to read a tar or gzip file?\n" : "");
		goto Done;
	}

	//2. Begin extracting to SQL export file/stdout.
	if (!this->bTestOnly && !this->bShowBasicStatisticsOnly && !this->bOutputDescFileOnly && !this->metadataOptions.bOutputOnlyMetadata) //...except in these cases
	{
		string outFileName = outputDir + "/" + outputBasename;
		if (ext)
			outFileName += ext;

		if (this->bShowStatus)
			this->statusOutput(INFO, "Writing %s\n", outFileName.c_str());
		//Open output stream.
		if (bStdout) {
			this->out = stdout;
		} else {
			this->out = fopen(outFileName.c_str(), "w");
		}
		if (!this->out)
		{
			this->statusOutput(ERROR, "%s: Could not open %s for writing\n", this->exeName.c_str(), outFileName.c_str());
			eRet = FILE_CREATION_ERR;
			goto Done;
		}
	}

	if (!this->bTestOnly && !this->bShowBasicStatisticsOnly) {
		if ((!bStdout || this->bOutputDescFileOnly) && !this->metadataOptions.bOutputOnlyMetadata)
		{
			//Output a .desc.sql file to disk/stdout.
			//This is not done when testing the integrity of a .zdw file,
			//streaming the text of the main file to stdout,
			//or outputting the metadata segment.
			eDescErr = bStdout ? this->outputDescToStdOut(this->columnNames) : this->outputDescToFile(this->columnNames, outputDir, outputBasename, ext);
			if (eDescErr != OK)
			{
				this->statusOutput(ERROR, "%s: Could not extract the %s.desc%s file\n", this->exeName.c_str(), outputBasename, ext ? ext : "");
				eRet = eDescErr;
				goto Done;
			}
			if (this->bOutputDescFileOnly)
				goto Done; //don't parse the rest of the file
		}

		if (!bStdout || this->metadataOptions.bOutputOnlyMetadata)
		{
			//Output a .metadata file to disk/stdout,
			//excepting in the above cases mentioned for .desc files.
			if (!this->metadata.empty() || !this->metadataOptions.keys.empty() || this->metadataOptions.bOutputOnlyMetadata) {
				eMetadataErr = bStdout ? this->outputMetadataToStdOut() : this->outputMetadataToFile(outputDir, outputBasename);
				if (eMetadataErr != OK)
				{
					this->statusOutput(ERROR, "%s: Could not extract the %s.metadata file\n", this->exeName.c_str(), outputBasename);
					eRet = eMetadataErr;
					goto Done;
				}
			}
			if (this->metadataOptions.bOutputOnlyMetadata)
				goto Done;
		}
	}

	{
		BufferedOutput_T buffer(this->out);
		//if a output ordering is specified, prepare it in the output buffer
		if (!this->namesOfColumnsToOutput.empty()) {
			const size_t num_output_columns = this->numColumns + this->blankColumnNames.size();
			vector<int> all_output_columns(num_output_columns);
			memcpy(&all_output_columns[0], this->outputColumns, num_output_columns * sizeof(int));
			//If there are blank columns, define them at the end of the input buffer, each outputting to its specified position in the column list.
			size_t index = this->numColumns;
			for (map<int, string>::const_iterator iter = this->blankColumnNames.begin(); iter != this->blankColumnNames.end(); ++iter)
			{
				all_output_columns[index++] = iter->first;
			}
			const bool bRes = buffer.setOutputColumnOrder(&all_output_columns[0], num_output_columns);

			if (!bRes) {
				eRet = BAD_REQUESTED_COLUMN; //column ordering is bad -- don't attempt to process further.
				//This is probably due to a bug in the code populating this->outputColumns
				assert(!"Bug in populating this->outputColumns?");
				goto Done;
			}
		}

		//3. Parse a block of data.
		do {
			eRet = this->parseNextBlock(buffer);
			this->cleanupBlock();
			if (eRet != OK)
				goto Done;
			assert(this->isLastBlock() || this->version >= 3); //version 3+ supports multiple blocks
		} while (!this->isLastBlock());
	}

	//Ensure we're at EOF at this point.
	if (this->bShowBasicStatisticsOnly) {
		assert(this->isLastBlock());
	} else {
		UCHAR dummy;
		this->readBytes(&dummy, 1, false); //a dummy read to set eof if we're at the end
		if (!this->isFinished())
		{
			this->statusOutput(INFO, "Did not reach EOF\n");
			eRet = ZDW_LONGER_THAN_EXPECTED_ERR;
		}
	}

Done:
	//Clean-up.
	if (sourceDir)
		free(sourceDir);
	if (this->out && !bStdout)
		fclose(this->out);

	return eRet;
}

bool UnconvertFromZDW_Base::UseVirtualExportBaseNameColumn() const
{
	return indexForVirtualBaseNameColumn != IGNORE;
}

void UnconvertFromZDW_Base::EnableVirtualExportBaseNameColumn()
{
	indexForVirtualBaseNameColumn = USE_VIRTUAL_COLUMN;
}

bool UnconvertFromZDW_Base::UseVirtualExportRowColumn() const
{
	return indexForVirtualRowColumn != IGNORE;
}

void UnconvertFromZDW_Base::EnableVirtualExportRowColumn()
{
	indexForVirtualRowColumn = USE_VIRTUAL_COLUMN;
}


//***************************************************************
//Explicit template class instantiations.
template class UnconvertFromZDWToFile<BufferedOutput>;
template class UnconvertFromZDWToFile<BufferedOrderedOutput>;


UnconvertFromZDWToMemory::~UnconvertFromZDWToMemory()
{ }

//***************************************************************
//
// Used by API to get all the rows out of a ZDW file.
// The block-based structure of the ZDW file is hidden from the caller.
//
ERR_CODE UnconvertFromZDWToMemory::getRow(const char** outColumns)
{
	size_t num;
	return getRow(NULL, NULL, outColumns, num);
}

ERR_CODE UnconvertFromZDWToMemory::getRow(char** buffer, size_t *size, const char** outColumns, size_t &numColumns)
{
	for (;;) {
		switch (this->eState)
		{
			case ZDW_BEGIN:
			{
				//Read ZDW file header.
				ERR_CODE eRet = readHeader();
				if (eRet != OK)
					return eRet;
				assert(this->eState == ZDW_PARSE_BLOCK_HEADER); //set by successful readHeader() call
			}
			break;
			case ZDW_PARSE_BLOCK_HEADER:
			{
				ERR_CODE eRet = handleZDWParseBlockHeader();
				if (eRet != OK)
					return eRet;
			}
			break;
			case ZDW_OUTPUT_BLOCK_HEADER:
			{
				assert(!this->rowsRead);

				if (!bUseInternalBuffer) {
					this->pBufferedOutput->setOutputBuffer(buffer, size);
				}

				const bool bOutputRow = printBlockHeader(*this->pBufferedOutput);
				setState(ZDW_GET_NEXT_ROW);
				if (bOutputRow)
					return OK;
			}
			break;
			case ZDW_GET_NEXT_ROW:
				//Unpacks one row in the current block.
				if (this->rowsRead < this->numLines)
				{
					if (isFinished())
						return ROW_COUNT_ERR; //premature exit (truncated file?)

					//Process and return a row.
					if (!bUseInternalBuffer)
					{
						this->pBufferedOutput->setOutputBuffer(buffer, size);
					}

					this->pBufferedOutput->setOutputColumnPtrs(outColumns);
					ERR_CODE eRet = readNextRow(*this->pBufferedOutput);
					ERR_CODE eRetForNumColumns = this->getNumOutputColumns(numColumns);
					if (eRetForNumColumns != OK)
						numColumns = 0;
					return eRet;
				} else {
					//No more rows to read in this block.
					this->cleanupBlock();

					//Dealloc output buffer.  A new one will be alloced for the next block.
					this->pBufferedOutput.reset();

					setState(isLastBlock() ? ZDW_FINISHING : ZDW_PARSE_BLOCK_HEADER);
				}
			break;
			case ZDW_FINISHING:
				assert(!this->pBufferedOutput.get()); //should no longer exist

				//Ensure we're at EOF at this point.
				UCHAR dummy;
				readBytes(&dummy, 1, false); //a dummy read to set eof if we're at the end

				setState(ZDW_END);
				return isFinished() ? AT_END_OF_FILE : ZDW_LONGER_THAN_EXPECTED_ERR;
			case ZDW_END:
				return AT_END_OF_FILE;
		}
	}
}

ERR_CODE UnconvertFromZDWToMemory::getNumOutputColumns(size_t& num)
{
	for (;;) {
		switch (this->eState)
		{
		case ZDW_BEGIN:
			{
				//Read ZDW file header.
				ERR_CODE eRet = readHeader();
				if (eRet != OK)
					return eRet;

				assert(this->eState == ZDW_PARSE_BLOCK_HEADER); //set by successful readHeader() call
			}
			break;
		case ZDW_PARSE_BLOCK_HEADER:
			{
				ERR_CODE eRet = handleZDWParseBlockHeader();
				if (eRet != OK)
					return eRet;
			}
			break;
		default:
			if (!this->pBufferedOutput)
				return PROCESSING_ERROR;
			num = this->pBufferedOutput->getNumOutputColumns();
			return OK;
		case ZDW_FINISHING:
			return UNSUPPORTED_OPERATION;
		}
	}
	return PROCESSING_ERROR; //shouldn't get here
}

size_t UnconvertFromZDWToMemory::getCurrentRowLength()
{
	assert(this->pBufferedOutput.get());
	return this->pBufferedOutput->getCurrentRowLength();
}

ERR_CODE UnconvertFromZDWToMemory::handleZDWParseBlockHeader()
{
	//Begin parsing this block.  Read the header first.
	ERR_CODE eRet = parseBlockHeader();
	if (eRet != OK)
		return eRet;

	assert(!this->pBufferedOutput.get());
	size_t headerLineLength = 0;
	if (this->bOutputNonEmptyColumnHeader) {
		const string header = getBlockHeaderString();
		headerLineLength = header.size() + 1;
	}
	this->pBufferedOutput.reset(new BufferedOutputInMem(std::max(static_cast<size_t>(this->exportFileLineLength + this->virtualLineLength + 1), headerLineLength), bUseInternalBuffer));
	if (this->namesOfColumnsToOutput.empty())
	{
		//So getNumOutputColumns works in this use case
		pBufferedOutput->SetNumOutputColumns(this->numColumns);
	} else {
		num_output_columns = this->numColumns + this->blankColumnNames.size();
		vector<int> all_output_columns(num_output_columns);
		memcpy(&all_output_columns[0], this->outputColumns, num_output_columns * sizeof(int));
		//If there are blank columns, define them at the end of the input buffer, each outputting to its specified position in the column list.
		size_t index = this->numColumns;
		for (map<int, string>::const_iterator iter = this->blankColumnNames.begin(); iter != this->blankColumnNames.end(); ++iter)
		{
			all_output_columns[index++] = iter->first;
		}
		const bool bRes = pBufferedOutput->setOutputColumnOrder(&all_output_columns[0], num_output_columns);
		if (!bRes)
			return BAD_REQUESTED_COLUMN; //column ordering is bad -- don't attempt to process further.
	}

	setState(ZDW_OUTPUT_BLOCK_HEADER);
	return OK;
}

void UnconvertFromZDWToMemory::getColumnNamesVector(vector<string> &columnNamesVector)
{
	const ULONG numColumns = columnNames.size();
	const ULONG numOutputColumns = numColumns + this->blankColumnNames.size();

	OutputOrderIndexer *pOutputOrderIndexer = new OutputOrderIndexer[numOutputColumns];
	int validNumColumns = 0;
	for (size_t c = 0; c < numColumns; c++)
	{
		const int outColumnIndex = this->namesOfColumnsToOutput.empty() ? c : this->outputColumns[c];
		if (outColumnIndex == IGNORE)
			continue;

		assert(outColumnIndex >= 0 && outColumnIndex < int(numOutputColumns)); //valid range

		 //only list columns that are being outputted
		pOutputOrderIndexer[validNumColumns].index = c;
		pOutputOrderIndexer[validNumColumns].outputIndex = outColumnIndex;
		++validNumColumns;
	}

	//When requesting absent columns for padding, give them a generic text type.
	for (map<int, string>::const_iterator iter = this->blankColumnNames.begin(); iter != this->blankColumnNames.end(); ++iter)
	{
		// if index is a negative number, get the column name from blankColumnNames
		pOutputOrderIndexer[validNumColumns].index = -1 * (iter->first) - 1; //ensure negative value to differentiate
		pOutputOrderIndexer[validNumColumns].outputIndex = iter->first;
		validNumColumns++;
	}

	qsort(pOutputOrderIndexer, validNumColumns, sizeof(OutputOrderIndexer), compareByOutputIndex);

	for (int i = 0; i < validNumColumns; i++)
	{
		if (pOutputOrderIndexer[i].index < 0) {
			columnNamesVector.push_back(blankColumnNames[(pOutputOrderIndexer[i].index + 1) * -1]);
		} else {
			columnNamesVector.push_back(columnNames[pOutputOrderIndexer[i].index]);
		}
	}

	delete[] pOutputOrderIndexer;
}

bool UnconvertFromZDWToMemory::hasColumnName(const string& name) const
{
	return std::find(columnNames.begin(), columnNames.end(), name) != columnNames.end();
}

bool UnconvertFromZDWToMemory::OutputDescToFile(const string &outputDir)
{
	char *sourceDir = NULL;
	const char* outputBasename = NULL;
	InitDirAndBasenameFromFileName(this->inFileName, sourceDir, outputBasename);
	ERR_CODE eRet = this->outputDescToFile(this->columnNames, outputDir, outputBasename, ".sql");
	if (sourceDir)
		free(sourceDir);
	return eRet == OK;
}

vector<pair<uint64_t, string> > UnconvertFromZDWToMemory::getFileLineage()
{
	const string LINEAGE_KEY = "lineage";

	vector<pair<uint64_t, string> > out;

	if (this->eState == ZDW_BEGIN) {
		ERR_CODE eRet = readHeader();
		if (eRet != OK) {
			out.push_back(std::make_pair<uint64_t, string>(0, string("bad ZDW header")));
			return out;
		}
	}

	map<string, string>::const_iterator it = this->metadata.find(LINEAGE_KEY);

	if (it == this->metadata.end())
		return out;

	const string &value = it->second;
	if (value.empty())
		return out;

	size_t basename_pos = 0, row_pos;
	do {
		row_pos = value.find(',', basename_pos);
		if (row_pos == string::npos) {
			out.push_back(std::make_pair<uint64_t, string>(0, string("bad lineage data")));
			return out;
		}

		out.push_back(std::make_pair<uint64_t, string>(strtoull(value.c_str() + row_pos + 1, NULL, 10), value.substr(basename_pos, row_pos - basename_pos)));

		basename_pos = value.find('|', row_pos + 1); //start searching beyond comma
		if (basename_pos != string::npos)
			++basename_pos; //skip pipe
	} while (basename_pos != string::npos);

	return out;
}

} // namespace zdw
} // namespace adobe

