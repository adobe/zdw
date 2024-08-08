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
// Main wrapper around the UnconvertFromZDWToFile class.
//

#include "zdw/UnconvertFromZDW.h"

#include <algorithm>
#include <cassert>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

using namespace adobe::zdw;
using std::string;


//*****************************
void showVersion()
{
	printf("UnconvertFromZDW, Version %d%s\n",
			UnconvertFromZDW_Base::UNCONVERT_ZDW_VERSION, UnconvertFromZDW_Base::UNCONVERT_ZDW_VERSION_TAIL);
}

//*****************************
void usage(char* executable)
{
	char* exe = strrchr(executable, '/');
	if (exe)
		++exe; //skip '/'
	else
		exe = executable;

	printf("Usage: %s [-(i|o|q|s|t|v|w)] [-c[e|i|x] csvColumnNames] [other options] file1 [file2...]\n", exe);
	printf("\t-  direct outputted text to stdout, and status text to stderr\n"
	       "\t     No .desc file is outputted, except when the -o option is also set.\n"
	       "\t-a <text to append>  specify text to be appended to the output filename\n"
	       "\t-c specify a comma-separated list of column names to output (default = all columns)\n"
	       "\t\t Columns are output in the order they are given.\n"
	       "\t\t Non-existent and duplicate column names result in an error.\n"
	       "\t-ce same as '-c', but provide an empty text column\n"
	       "\t\t when a requested column is not present.\n"
	       "\t-ci same as '-c', but do not error when invalid columns are specified\n"
	       "\t\t Non-existent and duplicate column names after the first entry are ignored.\n"
	       "\t-cx Include all columns except for this comma-separated list\n"
	       "\t-d <outputDirectory>  specify the directory in which to place the resulting files\n"
	       "\t\t (default=the files will be placed in the same directory as the .zdw file)\n"
	       "\t-i read data to unconvert from stdin and by default send it to stdout.\n"
	       "\t     No filenames listed on the command line will be processed.\n"
	       "\t     If a filename is specified, this will be used as the output filename.\n"
	       "\t-o write the .desc file to disk (or stdout) and then exit\n"
	       "\t-q quiet -- no progress output (overrides -v)\n"
	       "\t-s show basic file statistics only\n"
	       "\t-t test integrity of zdw file only\n"
	       "\t-v verbose -- show count of rows during conversion\n"
	       "\t-w give outputted files no extension (default = .sql)\n"
	       "\n"
	       "\t--metadata       Provide to have only the metadata artifact output\n"
	       "\t--metadata-keys  Provide to have only the metadata keys output (no values)\n"
	       "\t--metadata-values=<csv keynames>  If supplied, only the indicated key-value pairs will be output\n"
	       "\t\t Non-existent and duplicate keys result in an error.\n"
	       "\t--metadata-values-allow-missing=<csv keynames>  If supplied, only the indicated key-value pairs will be output\n"
	       "\t\t For keys not present in the file, an empty value will be supplied.\n"
	       "\t\t This option is not compatible with --metadata-values\n"
	       "\n"
	       "\t--non-empty-column-header   output a header line listing non-empty columns in the next file block\n"
	       "\n"
	       "\t--help     show this help\n"
	       "\t--version  show the version number\n"
	       "\n");
}

//*****************************
void ShowHelp(char* executable)
{
	showVersion();
	usage(executable);
}

//************************************
//Error message when a command line parameter is bad.
int badParam(char* exeName, const char* paramStr)
{
	fprintf(stderr, "%s: Unknown parameter '%s'\n\n", exeName, paramStr);
	fprintf(stderr, "    Run with --help for usage info.\n");
	return BAD_PARAMETER;
}

//************************************
//Error message when a command line argument is missing.
int missingParam(char* exeName, const char* paramStr)
{
	fprintf(stderr, "%s: Missing argument after parameter '%s'\n\n", exeName, paramStr);
	fprintf(stderr, "    Run with --help for usage info.\n");
	return BAD_PARAMETER;
}

int extraOption(char* exeName, const char* optionStr)
{
	fprintf(stderr, "%s: Extra option '%s' not allowed in tandem with other mutually exclusive options.\n\n", exeName, optionStr);
	fprintf(stderr, "    Run with --help for usage info.\n");
	return BAD_PARAMETER;
}

int emptyFilename(char* exeName)
{
	fprintf(stderr, "%s: Empty filename not allowed\n\n", exeName);
	fprintf(stderr, "    Run with --help for usage info.\n");
	return BAD_PARAMETER;
}

//********************************************
ERR_CODE unconvertFile(
	string const& filename,
	const string& outputFileExtension,
	const string& namesOfColumnsToOutput,
	const char* specifiedDir,
	const char* outputBasename,
	const char* exeName,
	bool bShowStatus,
	bool bQuiet,
	bool bTestOnly,
	bool bOutputDescFileOnly,
	bool bToStdout,
	COLUMN_INCLUSION_RULE columnInclusionRule,
	bool bShowBasicStatisticsOnly,
	bool bNonEmptyColumnHeader,
	const internal::MetadataOptions& metadataOptions)
{
	assert(exeName);

	ERR_CODE eRet = OK;
	if (namesOfColumnsToOutput.empty() || bShowBasicStatisticsOnly) {
		UnconvertFromZDWToFile<BufferedOutput> unconvertFromZDW(filename, bShowStatus, bQuiet, bTestOnly, bOutputDescFileOnly);
		unconvertFromZDW.setMetadataOptions(metadataOptions);
		if (bShowBasicStatisticsOnly)
			unconvertFromZDW.showBasicStatisticsOnly();
		unconvertFromZDW.outputNonEmptyColumnHeader(bNonEmptyColumnHeader);
		eRet = unconvertFromZDW.unconvert(exeName, outputBasename, outputFileExtension.c_str(), specifiedDir, bToStdout);
	} else {
		UnconvertFromZDWToFile<BufferedOrderedOutput> unconvertFromZDW(filename, bShowStatus, bQuiet, bTestOnly, bOutputDescFileOnly);
		unconvertFromZDW.setMetadataOptions(metadataOptions);
		const bool bRes = unconvertFromZDW.setNamesOfColumnsToOutput(namesOfColumnsToOutput, columnInclusionRule);
		if (!bRes) {
			eRet = BAD_REQUESTED_COLUMN;
		} else {
			unconvertFromZDW.outputNonEmptyColumnHeader(bNonEmptyColumnHeader);
			eRet = unconvertFromZDW.unconvert(exeName, outputBasename, outputFileExtension.c_str(), specifiedDir, bToStdout);
		}
	}

	//Abnormal termination?
	if (eRet != OK) {
		//None of the requested columns were output.
		//If we're only looking at the file schema, return with no error.
		//Otherwise, the error code will inform that no data values are coming back.
		if (eRet == NO_COLUMNS_TO_OUTPUT && bOutputDescFileOnly)
			return OK;

		fprintf(stderr, "Error code=%d (%s): ", eRet, UnconvertFromZDW_Base::ERR_CODE_TEXTS[eRet < ERR_CODE_COUNT ? eRet : ERR_CODE_COUNT]);
		fprintf(stderr, "%s: %s failed\n\n", exeName, !filename.empty() ? filename.c_str() : "from stdin");
	}

	return eRet;
}

//************************************
int main(int argc, char* argv[])
{
	string specifiedDir;
	const char *ext = NULL;
	const char *outputBasename = NULL; //the name of the input file, by default
	bool showStatus = false;
	bool bStdin = false, bStdout = false;
	bool bOutputDescFileOnly = false;
	bool bTestOnly = false;
	bool bQuiet = false;
	COLUMN_INCLUSION_RULE inclusionRule = FAIL_ON_INVALID_COLUMN;
	bool bShowBasicStatisticsOnly = false;
	bool bOutputBlockHeaderNonEmptyColumns = false;
	string defaultExtension = ".sql";
	string namesOfColumnsToOutput;

	internal::MetadataOptions metadataOptions;

	if (argc < 2)
	{
		ShowHelp(argv[0]);
	}

	//Step 1.
	//Parse argument list to set up parameters.
	//Validate entire argument list before processing any data.
	//If any dash parameter is unrecognized, error out.
	int i, c;
	bool gave_c_option = false;
	for (i = 1; i < argc; i++)
	{
		const char *arg = argv[i];
		if (arg[0] == '-')
		{
			//Validate single-dash flags
			switch (arg[1]) {
				case 'a':
				case 'i':
				case 'o':
				case 'q':
				case 's':
				case 't':
				case 'v':
				case 'w':
					if (arg[2] != '\0')
						return badParam(argv[0], arg);
					break;
				case 'c':
					if (gave_c_option)
						return extraOption(argv[0], arg);
					gave_c_option = true;
					if (arg[2] != '\0') {
						//-c[e|i|x] flag
						if ((arg[2] != 'e' && arg[2] != 'i' && arg[2] != 'x') || arg[3] != '\0')
							return badParam(argv[0], arg);
					}
				break;

				//Double-dash arguments validated below
			}
			//Options that need an argument.
			switch (arg[1]) {
				case 'a':
				case 'c':
				case 'd':
					if (argc <= i + 1)
						return missingParam(argv[0], arg);
				break;
			}

			//Execute flag.
			switch (arg[1])
			{
				case '\0': //i.e. '-'
					bStdout = true;
					break;
				case 'a':
					//custom file extension
					ext = argv[++i];
					break;
				case 'c':
					switch (arg[2]) {
						case 'e': inclusionRule = PROVIDE_EMPTY_MISSING_COLUMNS; break;
						case 'i': inclusionRule = SKIP_INVALID_COLUMN; break;
						case 'x': inclusionRule = EXCLUDE_SPECIFIED_COLUMNS; break;
						case '\0': default: inclusionRule = FAIL_ON_INVALID_COLUMN; break;
					}
					namesOfColumnsToOutput = argv[++i];
					break;
				case 'd':
					//output dir
					specifiedDir = argv[++i];
					//Cut off any trailing /
					c = specifiedDir.size() - 1;
					if (specifiedDir[c] == '/')
						specifiedDir.resize(c);
					break;
				case 'i': //read from stdin and output to stdout
					bStdin = true;
					break;
				case 'o':
					bOutputDescFileOnly = true;
					break;
				case 'q':
					bQuiet = true;
					break;
				case 's':
					bShowBasicStatisticsOnly = true;
					break;
				case 't':
					bTestOnly = true;
					break;
				case 'v':
					showStatus = true;
					break;
				case 'w': //no default extension
					defaultExtension.resize(0);
					break;
				case '-': //double dash arguments, i.e., '--[text]'
					{
						const char* flag = arg + 2;
						if (!strcmp(flag, "help"))
						{
							ShowHelp(argv[0]);
							return OK;
						}
						if (!strcmp(flag, "ver") || !strcmp(flag, "version")) {
							showVersion();
							return OK;
						}
						if (!strcmp(flag, "non-empty-column-header")) {
							bOutputBlockHeaderNonEmptyColumns = true;
							break;
						}
						if (!strcmp(flag, "metadata")) {
							metadataOptions.bOutputOnlyMetadata = true;
							break;
						}
						if (!strcmp(flag, "metadata-keys")) {
							metadataOptions.bOnlyMetadataKeys = true;
							break;
						}
						if (!strncmp(flag, "metadata-values=", 16) || !strncmp(flag, "metadata-values-allow-missing=", 30)) {
							if (!metadataOptions.keys.empty())
								return extraOption(argv[0], arg);

							const bool bAllowMissing = !strncmp(flag, "metadata-values-allow-missing=", 30);
							metadataOptions.bAllowMissingKeys = bAllowMissing;

							const char *value = flag + (bAllowMissing ? 30 : 16);
							while (value) {
								const char *nextVal = strchr(value, ',');
								if (nextVal) {
									const string v(value, static_cast<size_t>(nextVal - value));
									if (!(metadataOptions.keys.insert(v)).second)
										return badParam(argv[0], arg); //duplicate key
									++nextVal; //advance beyond comma
								} else {
									const string v(value);
									if (!(metadataOptions.keys.insert(v)).second)
										return badParam(argv[0], arg); //duplicate key
								}
								value = nextVal;
							}

							if (metadataOptions.keys.empty())
								return badParam(argv[0], arg);
							break;
						}
					}

					//any other "--text" param is invalid
					return badParam(argv[0], arg);
				default:
					return badParam(argv[0], arg);
			}
		}
	}

	//Concurrent argument guards
	if (bOutputDescFileOnly && metadataOptions.bOutputOnlyMetadata) {
		fprintf(stderr, "-o and --metadata options are incompatible.  Aborting.\n");
		return BAD_PARAMETER;
	}

	//Step 2.
	//Process files listed on the command line.
	for (i = 1; i < argc; i++)
	{
		const char *arg = argv[i];
		if (arg[0] == '-')
		{
			//Skip the argument given to these flags.
			switch (arg[1])
			{
				case 'a':
				case 'c':
				case 'd':
					++i;
				break;
			}
		} else {
			if (arg[0] == '\0') {
				return emptyFilename(argv[0]);
			}
			if (bStdin) {
				//Filename is considered an output base filename when reading from stdin.
				//That is, no filenames on the command line are processed as input files.
				outputBasename = arg;
			} else {
				//Build extension to give to outputted files.
				string outputFileExtension = defaultExtension;
				if (ext)
					outputFileExtension += ext;

				//Process a file.
				ERR_CODE eRet = unconvertFile(
					arg, outputFileExtension, namesOfColumnsToOutput, specifiedDir.c_str(),
					NULL, //output basename is the same as of the input filename
					argv[0],
					showStatus, bQuiet, bTestOnly, bOutputDescFileOnly, bStdout,
					inclusionRule,
					bShowBasicStatisticsOnly,
					bOutputBlockHeaderNonEmptyColumns,
					metadataOptions
				);
				if (eRet != OK)
					return eRet;
			}
		}
	}

	//Step 3.
	//Process ZDW data being read from stdin.
	if (bStdin) {
		//Build extension to give to outputted files.
		string outputFileExtension = defaultExtension;
		if (ext)
			outputFileExtension = ext;

		ERR_CODE eRet = unconvertFile(
			"", //indicates to read data from stdin
			outputFileExtension, namesOfColumnsToOutput, specifiedDir.c_str(),
			outputBasename, //an output filename might be set
			argv[0],
			showStatus, bQuiet, bTestOnly, bOutputDescFileOnly, bStdout,
			inclusionRule,
			bShowBasicStatisticsOnly,
			bOutputBlockHeaderNonEmptyColumns,
			metadataOptions
		);
		if (eRet != OK)
			return eRet;
	}

	return OK;
}

