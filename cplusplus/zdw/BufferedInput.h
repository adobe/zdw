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

#ifndef BUFFEREDINPUT_H
#define BUFFEREDINPUT_H

#include <string>


namespace adobe {
namespace zdw {

class BufferedInput
{
public:
	// open a pipe via a command and read input from the pipe
	BufferedInput(const std::string& command, const size_t capacity = 16 * 1024);

	// read from standard input
	BufferedInput();

	~BufferedInput();

	void close();
	bool rewind();

	//Returns: whether any more data can be returned
	bool eof() const;

	//Returns: whether file handle appears to be open
	bool is_open() const { return (this->fp != NULL) || this->bFromStdin; }

	//Input source.
	//Either must be set explicitly in order to avoid inadvertent reads from an unexpected source.
	void readFromStdin(const bool bVal = true) { this->bFromStdin = bVal; }

	void reset() { index = length = 0; }

	bool can_read_more_data() const;

	void refill_buffer();

	//Returns: number of bytes read
	size_t read(void* data, size_t size);

	//Skip ahead the indicated number of bytes without outputting any of the data.
	//Returns: number of bytes skipped
	size_t skip(size_t size);

	bool open();

	// Obtains a line of data (or as much of a line as can fit) into the supplied buffer.
	//
	// If there isn't enough data in the buffer for the entire line, as much of the line as possible will be stored
	// in the buffer, including a null terminator.
	//
	// The filled buffer will include newline termination, if there is newline termination available (e.g., a
	// line at the end of the file with no terminating newline will result in a buffer without newline termination)
	//
	// Returns a pointer to the filled buffer or nullptr if there is no more data to read into the buffer.
	const char* getline(char* buf, const size_t size);

private:
	std::string command;
	FILE* fp;
	char *buffer;
	size_t capacity;

	size_t index, length; //data that is sitting in the buffer
	bool bEOF;
	bool bFromStdin; //if set, read from stdin instead of 'fp'
};

} // namespace zdw
} // namespace adobe

#endif
