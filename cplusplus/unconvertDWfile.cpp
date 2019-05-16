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

#include "UnconvertFromZDW.h"

#include <stdio.h>
#include <stdlib.h>
#include <algorithm>

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
	if(exe)
		++exe; //skip '/'
	else
		exe = executable;

	printf("Usage: %s [-(i|o|q|s|t|v|w)] [-a string2append] [-c[e|i|x] csvColumnNames] [-d outputDirectory] file1 [file2...]\n", exe);
	printf("\t-  direct outputted text to stdout, and status text to stderr\n"
	       "\t     No .desc file is outputted, except when the -o option is also set.\n"
	       "\t-a specify a string to be appended to the output filename\n"
	       "\t-c specify a comma-separated list of column names to output (default = all columns)\n"
	       "\t\t Columns are output in the order they are given.\n"
	       "\t\t Non-existent and duplicate column names result in an error.\n"
	       "\t-ce same as '-c', but provide an empty text column\n"
	       "\t\t when a requested column is not present.\n"
	       "\t-ci same as '-c', but do not error when invalid columns are specified\n"
	       "\t\t Non-existent and duplicate column names after the first entry are ignored.\n"
	       "\t-cx Include all columns except for this comma-separated list\n"
	       "\t-d specify the directory in which to place the resulting files\n"
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
	return ZDW::BAD_PARAMETER;
}

//************************************
//Error message when a command line argument is missing.
int missingParam(char* exeName, const char* paramStr)
{
	fprintf(stderr, "%s: Missing argument after parameter '%s'\n\n", exeName, paramStr);
	fprintf(stderr, "    Run with --help for usage info.\n");
	return ZDW::BAD_PARAMETER;
}

int extraOption(char* exeName, const char* optionStr)
{
	fprintf(stderr, "%s: Extra option '%s' not allowed in tandem with other mutually exclusive options.\n\n", exeName, optionStr);
	fprintf(stderr, "    Run with --help for usage info.\n");
	return ZDW::BAD_PARAMETER;
}

int emptyFilename(char* exeName)
{
	fprintf(stderr, "%s: Empty filename not allowed\n\n", exeName);
	fprintf(stderr, "    Run with --help for usage info.\n");
	return ZDW::BAD_PARAMETER;
}
//********************************************
ZDW::ERR_CODE unconvertFile(
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
	ZDW::COLUMN_INCLUSION_RULE columnInclusionRule,
	bool bShowBasicStatisticsOnly)
{
	assert(exeName);

	ZDW::ERR_CODE eRet = ZDW::OK;
	if (namesOfColumnsToOutput.empty() || bShowBasicStatisticsOnly) {
		UnconvertFromZDWToFile<BufferedOutput> unconvertFromZDW(filename, bShowStatus, bQuiet, bTestOnly, bOutputDescFileOnly);
		if (bShowBasicStatisticsOnly)
			unconvertFromZDW.showBasicStatisticsOnly();
		eRet = unconvertFromZDW.unconvert(exeName, outputBasename, outputFileExtension.c_str(), specifiedDir, bToStdout);
	} else {
		UnconvertFromZDWToFile<BufferedOrderedOutput> unconvertFromZDW(filename, bShowStatus, bQuiet, bTestOnly, bOutputDescFileOnly);
		const bool bRes = unconvertFromZDW.setNamesOfColumnsToOutput(namesOfColumnsToOutput, columnInclusionRule);
		if (!bRes)
			eRet = ZDW::BAD_REQUESTED_COLUMN;
		else
			eRet = unconvertFromZDW.unconvert(exeName, outputBasename, outputFileExtension.c_str(), specifiedDir, bToStdout);
	}

	//Abnormal termination?
	if (eRet != ZDW::OK) {
		//None of the requested columns were outputted.
		//If we're only looking at the file schema, return with no error.
		//Otherwise, the error code will inform that no data values are coming back.
		if (eRet == ZDW::NO_COLUMNS_TO_OUTPUT && bOutputDescFileOnly)
			return ZDW::OK;

		fprintf(stderr, "Error code=%d (%s): ", eRet, UnconvertFromZDW_Base::ERR_CODE_TEXTS[eRet < ZDW::ERR_CODE_COUNT ? eRet : ZDW::ERR_CODE_COUNT]);
		UnconvertFromZDW_Base::printError(exeName, !filename.empty() ? filename : "from stdin");
	}

	return eRet;
}

//************************************
int main(int argc, char* argv[])
{
	string specifiedDir;
	char *ext = NULL;
	char *outputBasename = NULL; //the name of the input file, by default
	bool showStatus = false;
	bool bStdin = false, bStdout = false;
	bool bOutputDescFileOnly = false;
	bool bTestOnly = false;
	bool bQuiet = false;
	ZDW::COLUMN_INCLUSION_RULE inclusionRule = ZDW::FAIL_ON_INVALID_COLUMN;
	bool bShowBasicStatisticsOnly = false;
	string defaultExtension = ".sql";
	string namesOfColumnsToOutput;

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
		if (argv[i][0] == '-')
		{
			//Validate single-dash flags
			switch (argv[i][1]) {
				case 'a':
				case 'i':
				case 'o':
				case 'q':
				case 's':
				case 't':
				case 'v':
				case 'w':
					if (argv[i][2] != '\0')
						return badParam(argv[0], argv[i]);
					break;
				case 'c':
					if (gave_c_option)
						return extraOption(argv[0], argv[i]);
					gave_c_option = true;
					if (argv[i][2] != '\0') {
						//-c[e|i|x] flag
						if ((argv[i][2] != 'e' && argv[i][2] != 'i' && argv[i][2] != 'x') || argv[i][3] != '\0')
							return badParam(argv[0], argv[i]);
					}
				break;
			}
			//Options that need an argument.
			switch (argv[i][1]) {
				case 'a':
				case 'c':
				case 'd':
					if (argc <= i+1)
						return missingParam(argv[0], argv[i]);
				break;
			}

			//Execute flag.
			switch (argv[i][1])
			{
				case '\0': //i.e. '-'
					bStdout = true;
					break;
				case 'a':
					//custom file extension
					ext = argv[++i];
					break;
				case 'c':
					switch (argv[i][2]) {
						case 'e': inclusionRule = ZDW::PROVIDE_EMPTY_MISSING_COLUMNS; break;
						case 'i': inclusionRule = ZDW::SKIP_INVALID_COLUMN; break;
						case 'x': inclusionRule = ZDW::EXCLUDE_SPECIFIED_COLUMNS; break;
						case '\0': default: inclusionRule = ZDW::FAIL_ON_INVALID_COLUMN; break;
					}
					namesOfColumnsToOutput = argv[++i];
					break;
				case 'd':
					//output dir
					specifiedDir = argv[++i];
					//Cut off any trailing /
					c = specifiedDir.size() - 1;
					if(specifiedDir[c] == '/')
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
				case '-': //i.e., '--[text]'
					{
						const char* flag = argv[i]+2;
						if (strlen(flag) == 0) //"--" for outputting to stdout is deprecated
							bStdout = true;
						else if (!strcmp(flag, "help"))
						{
							ShowHelp(argv[0]);
							return ZDW::OK;
						}
						else if (!strcmp(flag, "ver") || !strcmp(flag, "version")) {
							showVersion();
							return ZDW::OK;
						}
					}
					//any other "--text" param is invalid
					return badParam(argv[0], argv[i]);
				default:
					return badParam(argv[0], argv[i]);
			}
		}
	}

	//Step 2.
	//Process files listed on the command line.
	for (i = 1; i < argc; i++)
	{
		if (argv[i][0] == '-')
		{
			//Skip the argument given to these flags.
			switch (argv[i][1])
			{
				case 'a':
				case 'c':
				case 'd':
					++i;
				break;
			}
		} else {
			if(argv[i][0] == '\0') {
				return emptyFilename(argv[0]);
			}
			if (bStdin) {
				//Filename is considered an output base filename when reading from stdin.
				//That is, no filenames on the command line are processed as input files.
				outputBasename = argv[i];
			} else {
				//Build extension to give to outputted files.
				string outputFileExtension = defaultExtension;
				if (ext)
					outputFileExtension += ext;

				//Process a file.
				ZDW::ERR_CODE eRet = unconvertFile(
					argv[i], outputFileExtension, namesOfColumnsToOutput, specifiedDir.c_str(),
					NULL, //output basename is the same as of the input filename
					argv[0],
					showStatus, bQuiet, bTestOnly, bOutputDescFileOnly, bStdout,
					inclusionRule,
					bShowBasicStatisticsOnly
				);
				if (eRet != ZDW::OK)
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

		ZDW::ERR_CODE eRet = unconvertFile(
			"", //indicates to read data from stdin
			outputFileExtension, namesOfColumnsToOutput, specifiedDir.c_str(),
			outputBasename, //an output filename might be set
			argv[0],
			showStatus, bQuiet, bTestOnly, bOutputDescFileOnly, bStdout,
			inclusionRule,
			bShowBasicStatisticsOnly
		);
		if (eRet != ZDW::OK)
			return eRet;
	}

	return ZDW::OK;
}
