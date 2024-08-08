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

#ifndef UNCONVERTFROMZDW_H
#define UNCONVERTFROMZDW_H

#include "includes.h"
#include "BufferedInput.h"
#include "BufferedOutput.h"
#include "status_output.h"

#include <map>
#include <set>
#include <stdexcept>
#include <string>
#include <vector>
#include <memory>


namespace adobe {
namespace zdw {

//Error codes
enum ERR_CODE
{
	OK=0, //don't change value API
	BAD_PARAMETER=1,
	GZREAD_FAILED=2,
	FILE_CREATION_ERR=3,
	FILE_OPEN_ERR=4,
	UNSUPPORTED_ZDW_VERSION_ERR=5,
	ZDW_LONGER_THAN_EXPECTED_ERR=6,
	UNEXPECTED_DESC_TYPE=7,
	ROW_COUNT_ERR=8,
	CORRUPTED_DATA_ERROR=9,
	HEADER_NOT_READ_YET=10,
	HEADER_ALREADY_READ_ERR=11,
	AT_END_OF_FILE=12,
	BAD_REQUESTED_COLUMN=13,
	NO_COLUMNS_TO_OUTPUT=14,
	PROCESSING_ERROR=15,
	UNSUPPORTED_OPERATION=16,
	METADATA_KEY_NOT_PRESENT=17,

	ERR_CODE_COUNT
};

enum COLUMN_INCLUSION_RULE
{
	FAIL_ON_INVALID_COLUMN,
	SKIP_INVALID_COLUMN,
	EXCLUDE_SPECIFIED_COLUMNS,
	PROVIDE_EMPTY_MISSING_COLUMNS
};


class ZDWException : public std::runtime_error
{
public:
	explicit ZDWException(const ERR_CODE errcode);
	ERR_CODE code;
};


namespace internal {

#define BLOCKSIZE (8)
union charBlock
{
	char c[BLOCKSIZE];
	ULONGLONG n;
};

struct UniquesPart
{
	charBlock m_Char;
	internal::indexBytes m_PrevChar;
};

struct VisitorPart
{
	ULONGLONG m_VID;
	internal::indexBytes m_PrevID;
};

struct MetadataOptions
{
	bool bOutputOnlyMetadata;
	bool bOnlyMetadataKeys;
	bool bAllowMissingKeys;
	std::set<std::string> keys;

	MetadataOptions()
		: bOutputOnlyMetadata(false)
		, bOnlyMetadataKeys(false)
		, bAllowMissingKeys(false)
	{ }
};

} // namespace internal

//Contains most of the algorithmic functionality.
class UnconvertFromZDW_Base
{
	static const size_t DEFAULT_LINE_LENGTH;

public:
	static const int UNCONVERT_ZDW_VERSION;
	static const char UNCONVERT_ZDW_VERSION_TAIL[3];

	static const char ERR_CODE_TEXTS[ERR_CODE_COUNT + 1][30];

	UnconvertFromZDW_Base(const std::string &inFileName,
			const bool bShowStatus = true, const bool bQuiet = true,
			const bool bTestOnly = false, const bool bOutputDescFileOnly = false);
	virtual ~UnconvertFromZDW_Base();

	void setStatusOutputCallback(StatusOutputCallback cb) { statusOutput = cb; }

	static std::string getVersion();

	//Common API.
	std::vector<std::string> getColumnNames() const { return this->columnNames; }
	UCHAR* getColumnTypes() const { return this->columnType; }
	ULONG getRowsRead() const { return this->rowsRead; } //in current block
	ULONG getNumLines() const { return this->numLines; } //in current block
	bool isLastBlock() const { return this->lastBlock != 0; }
	bool isFinished() const { return this->input->eof(); }
	bool isReadOpen() const { return this->input && this->input->is_open(); }

	void printError(const std::string &exeName, const std::string &inFileName);

	void outputNonEmptyColumnHeader(bool bFlag = true) { this->bOutputNonEmptyColumnHeader = bFlag; }
	ERR_CODE readHeader();
	bool setNamesOfColumnsToOutput(const std::string& csv_str, COLUMN_INCLUSION_RULE inclusionRule);
	bool setNamesOfColumnsToOutput(const std::vector<std::string> &csv_vector, COLUMN_INCLUSION_RULE inclusionRule);
	void showBasicStatisticsOnly(bool bVal = true) { this->bShowBasicStatisticsOnly = bVal; }

	ERR_CODE GetSchema(std::ostream& stream);

	void setMetadataOptions(const internal::MetadataOptions& options) { this->metadataOptions = options; }

protected:
	ERR_CODE outputDescToFile(const std::vector<std::string>& columnNames,
		const std::string& outputDir, const char* filestub, const char* ext);
	ERR_CODE outputDescToStdOut(const std::vector<std::string>& columnNames);

	ERR_CODE outputMetadataToFile(const std::string& outputDir, const char* filestub) const;
	ERR_CODE outputMetadataToStdOut() const;

	size_t readBytes(void* buf, const size_t len, const bool bHaltOnReadError = true);
	size_t skipBytes(const size_t len);
	char* GetWord(ULONG index, char* row);

	static std::string GetBaseNameForInFile(const std::string &inFileName);
	bool UseVirtualExportBaseNameColumn() const;
	void EnableVirtualExportBaseNameColumn();

	bool UseVirtualExportRowColumn() const;
	void EnableVirtualExportRowColumn();

	void cleanupBlock();
	ERR_CODE parseBlockHeader();
	std::string getBlockHeaderString() const;

	size_t llutoa(ULONGLONG value);
	size_t lltoa(SLONGLONG value);

	ULONG exportFileLineLength;
	ULONG virtualLineLength;
	std::map<std::string, std::string> metadata; //version 11+
	std::vector<char *> dictionary; //version 9+
	std::vector<ULONG> dictionary_memblock_size;
	internal::UniquesPart *uniques;  //version 1-8
	internal::VisitorPart *visitors; //version 1-7

	USHORT version;
	double decimalFactor;  //version 1-3
	ULONG numLines;
	ULONG numColumnsInExportFile;
	ULONG numColumns;
	UCHAR lastBlock;

	char *row;

	static const size_t TEMP_BUF_SIZE = 512;
	static const size_t TEMP_BUF_LAST_POS = TEMP_BUF_SIZE - 1;
	char temp_buf[TEMP_BUF_SIZE];

	std::string exeName;
	const std::string inFileName;
	const std::string inFileBaseName;

	BufferedInput *input;

	const bool bOutputDescFileOnly; //if set, only output the .desc file, but don't unconvert any data rows

	const bool bShowStatus, bQuiet;
	const bool bTestOnly;          //if set, only validate that data appear to be good without unconverting
	bool bOutputNonEmptyColumnHeader; //if set, output a header line listing non-empty columns at the start of each file block
	bool bShowBasicStatisticsOnly; //if set, show header statistics of data and exit
	bool bFailOnInvalidColumns; //if invalid columns are supplied, do we error out?
	std::map<std::string, int unsigned> namesOfColumnsToOutput;
	bool bExcludeSpecifiedColumns; //if set, namesOfColumnsToOutput is an exclusion set, and not an inclusion set
	bool bOutputEmptyMissingColumns; //if set, output an empty column

	//Header info.
	internal::MetadataOptions metadataOptions;
	int indexForVirtualBaseNameColumn;
	int indexForVirtualRowColumn;
	std::vector<std::string> columnNames;
	UCHAR *columnType;    //column value representation (e.g. string, numeric, char)
	USHORT *columnCharSize; //char size of field (where applicable)
	int *outputColumns; //flags to indicate which columns to output (non-negative values), and in what order
	std::map<int, std::string> blankColumnNames;

	//Used when unpacking a block.
	UCHAR *columnSize, *setColumns;
	ULONGLONG *columnBase;
	internal::storageBytes *columnVal;
	ULONGLONG dictionarySize, numVisitors;
	ULONG rowsRead;
	long numSetColumns;

	StatusOutputCallback statusOutput;

	//State info to assist API simplicity.
	enum STATE
	{
		ZDW_BEGIN,
		ZDW_PARSE_BLOCK_HEADER,
		ZDW_OUTPUT_BLOCK_HEADER,
		ZDW_GET_NEXT_ROW,
		ZDW_FINISHING,
		ZDW_END
	};
	void setState(STATE state) { this->eState = state; }
	STATE eState;

	size_t GetCurrentRowNumber() const { return currentRowNumber; }
	void IncrementCurrentRowNumber() { ++currentRowNumber; }

private:
	void readLineLength();
	void readDictionary();
	void readDictionaryChunk(const size_t size);
	void readVisitorDictionary();
	void readColumnFieldStats();

	ERR_CODE outputDesc(const std::vector<std::string>& columnNames, FILE* out);
	std::vector<std::string> getDesc(const std::vector<std::string>& columnNames,
		const std::string& name_type_separator, const std::string& delimiter) const;
	std::string getColumnDesc(const std::string& name, UCHAR type, size_t index,
		const std::string& name_type_separator, const std::string& delimiter) const;

	ERR_CODE outputMetadata(FILE* out) const;

	size_t currentRowNumber;
};

//***********************************************
template <typename T>
class UnconvertFromZDW : public UnconvertFromZDW_Base
{
public:
	UnconvertFromZDW(const std::string &inFileName,
			const bool bShowStatus = true, const bool bQuiet = true,
			const bool bTestOnly = false, const bool bOutputDescFileOnly = false)
		: UnconvertFromZDW_Base(inFileName, bShowStatus, bQuiet, bTestOnly, bOutputDescFileOnly)
	{ }

protected:
	void outputDefault(T& buffer, const UCHAR type);
	ERR_CODE parseNextBlock(T& buffer);
	bool printBlockHeader(T& buffer);
	ERR_CODE readNextRow(T& buffer);
};

// Note: This class template is used with two BufferedOutput_T types:
//    BufferedOutput and
//    BufferedOrderedOutput.
// The .cpp file contains explicit template instantiations for this template
// with these two types (to allow them to be used in the program
// without requiring the definition of unconvert() to be provided in this header file).
template<typename BufferedOutput_T>
class UnconvertFromZDWToFile : public UnconvertFromZDW<BufferedOutput_T>
{
public:
	UnconvertFromZDWToFile(const std::string &inFileName,
			const bool bShowStatus = true, const bool bQuiet = true,
			const bool bTestOnly = false, const bool bOutputDescFileOnly = false)
		: UnconvertFromZDW<BufferedOutput_T>(inFileName, bShowStatus, bQuiet, bTestOnly, bOutputDescFileOnly)
		, out(NULL)
	{ }

	ERR_CODE unconvert(const char* exeName, const char* outputBasename, const char* ext, const char* outputDir, bool bStdout);

private:
	FILE *out;
};

class UnconvertFromZDWToMemory : public UnconvertFromZDW<BufferedOutputInMem>
{
public:
	// if set bUseInternalBuffer to false, please use getRow(char ** buffer, size_t *size, char** outColumns) to get row data.
	UnconvertFromZDWToMemory(const std::string &inFileName,
			const bool bUseInternalBuffer = true,
			const bool bShowStatus = true, const bool bQuiet = true,
			const bool bTestOnly = false, const bool bOutputDescFileOnly = false)
		: UnconvertFromZDW<BufferedOutputInMem>(inFileName, bShowStatus, bQuiet, bTestOnly, bOutputDescFileOnly)
		, num_output_columns(0)
		, bUseInternalBuffer(bUseInternalBuffer)
	{
		statusOutput = defaultStatusOutputCallback;
	}
	~UnconvertFromZDWToMemory();

	ERR_CODE getRow(const char** outColumns);

	ERR_CODE getRow(char ** buffer, size_t *size, const char** outColumns, size_t &numColumns);

	ERR_CODE getNumOutputColumns(size_t& num);

	size_t getCurrentRowLength();

	// Call getNumOutputColumns or getRow first to retrieve the actual value of line length
	ULONG getLineLength() { return this->exportFileLineLength + this->virtualLineLength; }

	void getColumnNamesVector(std::vector<std::string> &columnNamesVector);
	bool hasColumnName(const std::string& name) const;

	//output desc.sql file to {outputDir} directory
	bool OutputDescToFile(const std::string &outputDir);

	std::vector<std::pair<uint64_t, std::string> > getFileLineage();

protected:
	ERR_CODE handleZDWParseBlockHeader();

private:
	std::unique_ptr<BufferedOutputInMem> pBufferedOutput;
	size_t num_output_columns;
	bool bUseInternalBuffer;
};

} // namespace zdw
} // namespace adobe

#endif
