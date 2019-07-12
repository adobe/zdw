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
#include "memory.h"

#include <cstring>
#include <cassert>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>

using namespace adobe::zdw;
using std::map;
using std::strcmp;
using std::string;


//******************************************************************
void showVersion()
{
	printf("ConvertToZDW, Version %i%s\n",
			ConvertToZDW::CONVERT_ZDW_CURRENT_VERSION, ConvertToZDW::CONVERT_ZDW_VERSION_TAIL);
}

//******************************************************************
void usage(const char* executable)
{
	const char* exe = strrchr(executable, '/');
	if (exe)
		++exe; //skip '/'
	else
		exe = executable;

	printf("Usage: %s [-d <dir>] [-(b|J|q|r|v)] [other options] file1 [file2] ...\n", exe);
	printf(
		"\t-b  compress .zdw with bzip2 [default=use gzip]\n"
		"\t-J  compress .zdw with xz [default=use gzip]\n"
		"\t-d  output to directory <dir> [default=same directory as source file]\n"
		"\t-i  streaming input from stdin; file1 is used as the implied name for the input stream\n"
		"\t-q  quiet operation (no status or progress messages) [default=not quiet]\n"
		"\t-r  remove the old files\n"
		"\t-t  trim trailing spaces from fields (for MySQL 5 exports)\n"
		"\t-v  validate the new file\n"
		"\n"
		"\t--zargs=X          arguments to pass in to the file compression process\n"
		"\t--mem-limit=<MB>   limit the MB of RAM used (default=3072 MB)\n"
		"\n"
		"\t--version11                feature flag to enable creation of v11 file format (i.e., w/ metadata block in header)\n"
		"\t--metadata:<key>=<value>   supply a key-value pair to store as file metadata for every file being converted\n"
		"\t--metadata-file=<filename> supply a filepath to specify key-value pairs (formatted as '<key>=<value>' pairs, each on a separate line) to store as file metadata for every file being converted\n"
		"\n"
		"\t--help     show this help\n"
		"\t--version  show the version number\n"
		"Input files must have a .sql extension.\n"
		"\n");
}

//******************************************************************
void ShowHelp(const char* executable)
{
	showVersion();
	usage(executable);
}

//************************************
ConvertToZDW::ERR_CODE outputErrorMsg(ConvertToZDW::ERR_CODE res)
{
	int errorCodeTextIndex = res;
	if (errorCodeTextIndex >= ConvertToZDW::ERR_CODE_COUNT)
		errorCodeTextIndex = ConvertToZDW::UNKNOWN_ERROR;
	fprintf(stderr, "ZDW conversion failed.  Internal error code=%i (%s)\n",
			res, ConvertToZDW::ERR_CODE_TEXTS[errorCodeTextIndex]);

	return res;
}

//************************************
//Error message when a command line parameter is bad.
int badParam(const char* exeName, const char* paramStr)
{
	fprintf(stderr, "%s: Unknown parameter '%s'\n\n", exeName, paramStr);
	fprintf(stderr, "    Run with --help for usage info.\n");
	return ConvertToZDW::BAD_PARAMETER;
}

//************************************
int main(int argc, char* argv[])
{
	const char* program = argv[0];
	if (argc < 2)
	{
		ShowHelp(program);
		return ConvertToZDW::NO_ARGS;
	}

	ConvertToZDW::ERR_CODE iRet = ConvertToZDW::OK;
	bool bStreamingInput = false;
	bool removeOldFiles = false;
	bool trimTrailingSpaces = false;
	bool validate = false;
	bool bQuiet = false;
	ConvertToZDW::Compressor compressor = ConvertToZDW::GZIP;
	const char* pOutputDir = NULL; //default = current dir
	const char* zArgs = NULL;
	map<string, string> metadata;
	bool bVersion11EnableFlag = false; //TODO temp feature flag for version 10 migration -- remove on completion

	//Parse flags.
	int i;
	int filenum = 0;
	for (i = 1; i < argc; i++)
	{
		if (argv[i][0] == '-')
		{
			switch (argv[i][1])
			{
				case 'b': compressor = ConvertToZDW::BZIP2; break;
				case 'J': compressor = ConvertToZDW::XZ;    break;
				case 'd':
					if (++i >= argc)
					{
						usage(program);
						return ConvertToZDW::MISSING_ARGUMENT;
					}
					pOutputDir = argv[i];
					break;
				case 'i': bStreamingInput = true; break;
				case 'q': bQuiet = true; break;
				case 'r': removeOldFiles = true; break;
				case 't': trimTrailingSpaces = true; break;
				case 'v': validate = true; break;
				case '-': //i.e., '--[text]'
					{
						const char* flag = argv[i] + 2;
						if (!strcmp(flag, "help"))
						{
							ShowHelp(program);
							return ConvertToZDW::OK;
						}
						if (!strcmp(flag, "ver") || !strcmp(flag, "version")) {
							showVersion();
							return ConvertToZDW::OK;
						}
						if (!strncmp(flag, "mem-limit=", 10)) {
							if (!Memory::set_memory_threshold_MB(atof(flag + 10)))
								return badParam(program, argv[i]);
							break;
						}
						if (!strncmp(flag, "metadata:", 9)) {
							const char *key = flag+9;
							const char *value = strchr(key, '=');
							if (!value)
								return badParam(program, argv[i]);
							metadata[string(key, value - key)] = string(value + 1);
							break;
						}
						if (!strncmp(flag, "metadata-file=", 14)) {
							const char *filepath = flag + 14;
							const int line_res = ConvertToZDW::loadMetadataFile(filepath, metadata);
							if (line_res) {
								fprintf(stderr, "%s: Metadata file load error '%s' (line %d) \n\n", program, filepath, line_res);
								return ConvertToZDW::BAD_PARAMETER;
							}
							break;
						}
						if (!strncmp(flag, "zargs=", 6)) {
							zArgs = flag + 6;
							break;
						}
						if (!strcmp(flag, "version11"))
						{
							bVersion11EnableFlag = true;
							break;
						}
					}
					//any other "--text" param is invalid
					usage(program);
					return badParam(program, argv[i]);
				default:
					return badParam(program, argv[i]);
			}
		} else {
			if (bStreamingInput && filenum > 0)
				return outputErrorMsg(ConvertToZDW::TOO_MANY_INPUT_FILES);
			++filenum;
		}
	}
	if (filenum == 0)
		return outputErrorMsg(ConvertToZDW::NO_INPUT_FILES);
	if (bStreamingInput && isatty(0)) //connected to a terminal -- nothing being piped to stdin (file descriptor = 0)
		return outputErrorMsg(ConvertToZDW::NO_INPUT_FILES);

	//Parse files.
	filenum = 0;
	for (i = 1; i < argc; i++)
	{
		if (argv[i][0] == '-')
		{
			//Skip the argument given to these flags.
			switch (argv[i][1])
			{
				case 'd':
					++i;
				break;
			}
		} else {
			//Make sure we're only processing one file name when reading data from stdin.
			assert(!bStreamingInput || filenum == 0);
			++filenum;

			char filename[1024];
			char filestub[1024];
			ConvertToZDW convert(bQuiet, bStreamingInput);
			convert.compressor = compressor;
			if (bVersion11EnableFlag)
				convert.enableVersion11();
			if (trimTrailingSpaces)
				convert.trimTrailingSpaces();
			const ConvertToZDW::ERR_CODE res = convert.convertFile(argv[i], program, validate, filestub, pOutputDir, zArgs, metadata);

			if (res != ConvertToZDW::OK)
			{
				if (!bQuiet)
				{
					outputErrorMsg(res);
				}
				iRet = ConvertToZDW::CONVERSION_FAILED; //to preserve existing error code API?
			}
			if (removeOldFiles)
			{
				if (res != ConvertToZDW::OK)
				{
					fprintf(stderr, "Could not remove original %s file because conversion was not good\n", filestub);
				} else {
					//Delete files converted from.
					sprintf(filename, "%s.desc.%s", filestub, convert.getInputFileExtension());
					unlink(filename);
					sprintf(filename, "%s.%s", filestub, convert.getInputFileExtension());
					unlink(filename);
				}
			}
		}
	}

	return iRet;
}

