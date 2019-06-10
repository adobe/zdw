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

#ifndef ZDW_STATUS_OUTPUT_H
#define ZDW_STATUS_OUTPUT_H


namespace adobe {
namespace zdw {

enum StatusOutputLevel
{
	INFO,
	ERROR,
};

typedef void (*StatusOutputCallback)(const StatusOutputLevel, const char *, ...);


// ERROR to stderr, INFO to stdout
void defaultStatusOutputCallback(const StatusOutputLevel level, const char *format, ...);

// Always output to stderr
void stdErrStatusOutputCallback(const StatusOutputLevel level, const char *format, ...);

} // namespace zdw
} // namespace adobe

#endif
