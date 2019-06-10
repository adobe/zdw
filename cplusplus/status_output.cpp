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

#include "zdw/status_output.h"
#include <stdarg.h>
#include <stdio.h>


namespace adobe {
namespace zdw {

// ERROR to stderr, INFO to stdout
void defaultStatusOutputCallback(const StatusOutputLevel level, const char *format, ...)
{
	FILE *outptr = (level == ERROR) ? stderr : stdout;

	va_list argptr;
	va_start(argptr, format);
	vfprintf(outptr, format, argptr);
	fflush(outptr);
	va_end(argptr);
}

// Always output to stderr
void stdErrStatusOutputCallback(const StatusOutputLevel level, const char *format, ...)
{
	va_list argptr;
	va_start(argptr, format);
	vfprintf(stderr, format, argptr);
	fflush(stderr);
	va_end(argptr);
}

} // namespace zdw
} // namespace adobe

