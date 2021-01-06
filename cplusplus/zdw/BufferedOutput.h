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

#ifndef BUFFEREDOUTPUT_H
#define BUFFEREDOUTPUT_H

#include <vector>
#include <string>


namespace adobe {
namespace zdw {

class BufferedOrderedOutput
{
private:
	//not implemented
	BufferedOrderedOutput(BufferedOrderedOutput const &);
	BufferedOrderedOutput &operator=(BufferedOrderedOutput const &);

	//Used for reordering column outputs.
	class ByteBuffer
	{
	public:
		ByteBuffer(int unsigned startSize = 16);
		ByteBuffer(const ByteBuffer& rhs);

		void swap(ByteBuffer& rhs);

		ByteBuffer& operator=(const ByteBuffer& rhs);
		~ByteBuffer();

		//Write data to the start of the buffer.
		inline void write(const void* data, const size_t dataSize);

		inline void reset() { this->size = 0; }

		inline void print(std::string& str) const { str.append(this->pBuffer, this->size); }

	private:
		char* pBuffer;
		int size;
		int unsigned capacity;
	};

public:
	BufferedOrderedOutput(FILE* fp);
	~BufferedOrderedOutput();

	bool write(const void* data, const size_t size);

	//Have an empty string outputted on the current column.
	void writeEmpty();

	//Output a separator between columns.
	bool writeSeparator(const void*, const size_t) { return true; } //we don't need to write any separator now

	//Completes the current row/line of text.
	bool writeEndline(const void* data, const size_t size);

	bool writeRawLine(const void* data, const size_t size);

	//Call to reorder column outputs in each row/line of text.
	//Input: a vector with an unordered sequence of zero-based indices; values of -1 are ignored
	//
	//Returns: whether the ordering provided was accepted
	bool setOutputColumnOrder(const int* pOutputOrder, const int numColumns);

	void setOutputColumnPtrs(const char**) { } //not needed in this class template version

private:
	FILE* const fp;

	//for (re)ordering column outputs within a line of text
	std::vector<int> outputIndex; //input --> output index remapping
	std::vector<ByteBuffer> outputColumnBuffer; //stores data for the column at each index
	int curColumnIndex;
};


class BufferedOutput
{
private:
	//not implemented
	BufferedOutput(BufferedOutput const &);
	BufferedOutput &operator=(BufferedOutput const &);

public:
	BufferedOutput(FILE* fp, const size_t capacity = 16 * 1024);
	~BufferedOutput();

	//Returns: whether operation succeeded
	bool flush();

	bool write(const void* data, const size_t size);

	void writeEmpty() { }

	//Output a separator between columns.
	bool writeSeparator(const void* data, const size_t size) { return write(data, size); }

	//Completes the current row/line of text.
	bool writeEndline(const void* data, const size_t size) { return write(data, size); }

	bool writeRawLine(const void* data, const size_t size) { return write(data, size); }

	bool setOutputColumnOrder(const int*, const int) { return true; } //not needed here -- use BufferedOrderedOutput if this functionality is desired
	void setOutputColumnPtrs(const char**) { } //not needed in this class template version

private:
	FILE* const fp;
	const size_t capacity;
	char* const buffer;

	size_t index;
};


struct OutputOrderIndexer
{
	int index;
	int outputIndex;
};


extern int compareByIndex(const void* first, const void* second);
extern int compareByOutputIndex(const void* first, const void* second);


//Class to output each row's column values to user-specified char**s.
class BufferedOutputInMem
{
public:
	BufferedOutputInMem(size_t neededBufferSize, bool bUseInternalBuffer = true);
	~BufferedOutputInMem();

	// invoke setOutputBuffer before parsing
	void setOutputBuffer(char** buffer, size_t *size);

	//Call whenever output needs to be directed to a new location.
	void setOutputColumnPtrs(const char** outColumns);

	bool setOutputColumnOrder(const int* pOutputOrder, const int numColumns);

	bool write(const void* data, const size_t size);

	void writeEmpty() { }
	bool writeSeparator(const void* /*data*/, const size_t /*size*/);
	bool writeEndline(const void* /*data*/, const size_t /*size*/);
	bool writeRawLine(const void* data, const size_t size);

	size_t getNumOutputColumns() { return this->numColumns; }

	//Set by UnconvertFromZDWToMemory to have this class report the number of columns in the ZDW file
	//Used when the caller does not explicitly request a set of columns
	void SetNumOutputColumns(size_t num) { this->numColumns = num; }

	size_t getCurrentRowLength() { return this->currentRowLength; }

private:
	void reorderOutputColumn();

	void checkBufferSize(const size_t requiredSize);

private:
	char **ppBuffer;
	char *pBuffer;
	size_t *pBufferSize;
	size_t index, columnNum;
	const char **pColumns; //where to output column values
	const char **pColumnsBuffer;  // used to reorder

	int *pOutputOrder;
	int numColumns;

	size_t currentRowLength; // the actual length of the row
	size_t neededBufferSize;
	bool bUseInternalBuffer;
	bool bNeedReorder;
};

} // namespace zdw
} // namespace adobe

#endif
