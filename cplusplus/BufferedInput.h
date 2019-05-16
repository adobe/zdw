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

#include <cassert>
#include <stdio.h>
#include <stdlib.h>
#include <string>
#include <string.h>
#include <zlib.h>

class BufferedInput
{
public:
	// either open a pipe with a command, and read input from the pipe,
	// or open a gz file and read via zlib API
	BufferedInput(const char* command, const size_t capacity=16*1024, bool is_gz_file=false)
		: fp(NULL)
		, gzFp(NULL)
		, buffer(NULL)
		, capacity(capacity)
		, index(0), length(0)
		, bEOF(false)
		, bFromStdin(false)
		, is_gz_file(is_gz_file)
	{
		assert(command);
		this->command = command;

		open();
	}

	// read from standard input
	BufferedInput()
		: fp(NULL)
		, gzFp(NULL)
		, buffer(NULL)
		, capacity(0)
		, index(0), length(0)
		, bEOF(false)
		, bFromStdin(true)
		, is_gz_file(false)
	{
	}

	~BufferedInput()
	{
		close();
		delete[] buffer;
	}

	void close() {
		if(is_gz_file) {
			if (gzFp) {
				gzclose(gzFp);
				gzFp = NULL;
			}
		} else if (fp) {
			pclose(fp);
			fp = NULL;
		}
	}
	void rewind() {
		bEOF = false;
		index = length = 0;
		if (is_gz_file) {
			if (gzFp)
				gzrewind(gzFp);
		} else {
			assert(!command.empty());
			close();
			open();
		}
	}

	//Returns: whether any more data can be returned
	bool eof() const {
		if (this->bFromStdin) {
			return feof(stdin) != 0;
		} else {
			return bEOF && index >= length;
		}
	}

	//Returns: whether file handle appears to be open
	bool is_open() const {return (this->fp != NULL) || this->bFromStdin || this->gzFp; }

	//Input source.
	//Either must be set explicitly in order to avoid inadvertent reads from an unexpected source.
	void readFromStdin(const bool bVal=true) {this->bFromStdin = bVal;}

	void reset() {index = length = 0;}

	bool can_read_more_data() const {
		if (eof())
			return false;
		if (is_gz_file) {
			if (!this->gzFp)
				return false;
		} else if (!this->fp){
			return false;
		}
		return true;
	}

	void refill_buffer()
	{
		this->index = this->length = 0;
		do {
			//Iterate reads when we don't receive the entire buffer immediately.
			if(is_gz_file)
			{
				this->length += gzread(this->gzFp, this->buffer + this->length,
					this->capacity - this->length);
				this->bEOF = gzeof(this->gzFp) != 0;
			} else {
				this->length += fread(this->buffer + this->length,
					1, this->capacity - this->length, this->fp);
				this->bEOF = feof(this->fp) != 0;
			}
		} while (this->length < this->capacity && !this->bEOF);
	}

	const char* getline(char* buf, const size_t size)
	{
		assert(buf);
		assert(size);

		if (this->bFromStdin)
			return fgets(buf, size, stdin);

		const size_t size_minus_1 = size-1;
		size_t out_pos=0;
		while (can_read_more_data()) {
			while (this->index < this->length) {
				if (out_pos == size_minus_1) {
					//Filled output buffer
					buf[out_pos] = 0;
					return buf;
				}
				const char ch = this->buffer[this->index++];
				buf[out_pos++] = ch;
				if (ch == '\n') {
					buf[out_pos] = 0;
					return buf;
				}
			}

			refill_buffer();
		}

		return out_pos ? buf : NULL;
	}

	//Returns: number of bytes read
	size_t read(void* data, size_t size)
	{
		if (this->bFromStdin) {
			//Reads requested number of bytes from stdin.
			return fread(data, 1, size, stdin);
		}

		if (!can_read_more_data())
			return 0;

		size_t bytesRead = 0;

		//Asking for more data than is currently in the buffer?
		size_t bytesInBuffer = this->length - this->index;
		if (size > bytesInBuffer)
		{
			//Flush what is available first.
			if (bytesInBuffer)
			{
				memcpy(data, this->buffer + this->index, bytesInBuffer);

				//If we've hit EOF, then there is no more data to read in.
				if (this->bEOF)
					return bytesInBuffer;

				//Prepare to fill the output buffer with more data.
				size -= bytesInBuffer;
				data = static_cast<char*>(data) + bytesInBuffer;
				bytesRead = bytesInBuffer;
			}

			//Should we read more into the buffer for this call?
			if (size >= this->capacity)
			{
				//Buffer is not large enough to store the rest of the requested data.
				//Just pass the rest of the data through without buffering.
				if(is_gz_file)
				{
					bytesRead += gzread(this->gzFp, data, size);
					this->bEOF = gzeof(this->gzFp) != 0;
				} else {
					bytesRead += fread(data, 1, size, this->fp);
					this->bEOF = feof(this->fp) != 0;
				}

				//Buffer is now empty -- refill next call
				this->index = this->length = 0;

				return bytesRead;
			}

			refill_buffer();
			bytesInBuffer = this->length;
		}

		//Return the amount of data requested.
		//If there isn't enough data available to fill the request, return what's left
		if (size > bytesInBuffer)
			size = bytesInBuffer;
		memcpy(data, this->buffer + this->index, size);
		this->index += size;

		return bytesRead + size; //total bytes returned
	}

	//Skip ahead the indicated number of bytes without outputting any of the data.
	//Returns: number of bytes skipped
	size_t skip(size_t size) {
		//Read blocks for speed.
		static const size_t BLOCK_SIZE = 16;
		char tempBlock[BLOCK_SIZE];
		size_t i, bytes_read;

		if (this->bFromStdin) {
			//Skips ahead the requested number of bytes in stdin.
			size_t advanced=0;

			const size_t blocks=size/BLOCK_SIZE;
			for (i=0; i<blocks; ++i) {
				bytes_read = fread(tempBlock, 1, BLOCK_SIZE, stdin);
				advanced += bytes_read;
				size -= bytes_read;
			}
			assert(size < BLOCK_SIZE);
			advanced += fread(tempBlock, 1, size, stdin);
			return advanced;
		}

		//no more data to be read
		if (eof())
			return 0;
		if (is_gz_file) {
			if (!this->gzFp)
				return 0;
		} else if (!this->fp){
			return 0;
		}

		const size_t bytesInBuffer = this->length - this->index;
		if (size <= bytesInBuffer) {
			//Move ahead by this many bytes within the buffer.
			this->index += size;
			return size;
		}

		//We want to skip more data than what is left in the buffer.

		//1. Advance to the end of the buffer.
		this->index = this->length;
		size -= bytesInBuffer;
		size_t advanced = bytesInBuffer;

		//2. Skip ahead the remaining number of bytes.
		const size_t blocks=size/BLOCK_SIZE;
		for (i=0; i<blocks; ++i) {
			if(is_gz_file)
			{
				bytes_read = gzread(this->gzFp, tempBlock, BLOCK_SIZE);
			} else {
				bytes_read = fread(tempBlock, 1, BLOCK_SIZE, this->fp);
			}
			advanced += bytes_read;
			size -= bytes_read;
		}
		assert(size < BLOCK_SIZE);
		if(is_gz_file)
		{
			advanced += gzread(this->gzFp, tempBlock, size);
			this->bEOF = gzeof(this->gzFp) != 0;
		} else {
			advanced += fread(tempBlock, 1, size, this->fp);
			this->bEOF = feof(this->fp) != 0;
		}

		return advanced;
	}

	void open()
	{
		assert(!gzFp);
		assert(!fp);

		if(is_gz_file)
			gzFp = gzopen(command.c_str(), "rb");
		else
			fp = popen(command.c_str(), "r");

		if (gzFp || fp)
		{
			if (!this->buffer) {
				assert(capacity > 0);
				this->buffer = new char[capacity];
			}
		}
	}

private:
	std::string command;
	FILE* fp;
	gzFile gzFp;
	char *buffer;
	size_t capacity;

	size_t index, length; //data that is sitting in the buffer
	bool bEOF;
	bool bFromStdin; //if set, read from stdin instead of 'fp'
	bool is_gz_file;
};

#endif
