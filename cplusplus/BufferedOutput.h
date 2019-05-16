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
#include <stdio.h>
#include <stdlib.h>
#include <assert.h>
#include <string.h>
#include <string>

class BufferedOrderedOutput
{
private:
	//not implemented
	BufferedOrderedOutput(BufferedOrderedOutput const &);
	BufferedOrderedOutput &operator=(BufferedOrderedOutput const &);

	//Used for reordering column outputs.
	class ByteBuffer {
	public:
		ByteBuffer(int unsigned startSize = 16)
		: pBuffer(new char[startSize])
		, size(0)
		, capacity(startSize)
		{ }
		ByteBuffer(const ByteBuffer& rhs)
		: pBuffer(new char[rhs.capacity])
		, size(rhs.size)
		, capacity(rhs.capacity)
		{
			memcpy(this->pBuffer, rhs.pBuffer, this->size);
		}
		void swap(ByteBuffer& rhs) {
			using std::swap;
			swap(this->pBuffer, rhs.pBuffer);
			swap(this->size, rhs.size);
			swap(this->capacity, rhs.capacity);
		}
		ByteBuffer& operator=(const ByteBuffer& rhs) { ByteBuffer(rhs).swap(*this); return *this; }
		~ByteBuffer() { delete[] this->pBuffer; }

		//Write data to the start of the buffer.
		inline void write(const void* data, const size_t dataSize) {
			if (this->capacity < dataSize) {
				//Reserve more memory.
				const int newSize = dataSize * 2; //exponential growth
				delete[] this->pBuffer;

				this->capacity = 0;
				this->pBuffer = NULL; //basic exception safety

				this->pBuffer = new char[newSize];
				this->capacity = newSize;
			}
			memcpy(this->pBuffer, data, dataSize);
			this->size = dataSize;
		}
		inline void reset() {
			this->size = 0;
		}
		inline void print(std::string& str) const { str.append(this->pBuffer, this->size); }

	private:
		char* pBuffer;
		int size;
		int unsigned capacity;
	};

public:
	BufferedOrderedOutput(FILE* fp)
		: fp(fp)
		, curColumnIndex(0)
	{ }
	~BufferedOrderedOutput()
	{ }

	bool write(const void* data, const size_t size)
	{
		//If we are reordering column outputs, then save the output for when the row is complete.
		assert(!this->outputIndex.empty());
		const int unsigned outIndex = this->outputIndex[this->curColumnIndex++];

		//Should not be calling write() for columns that are not written out to any location
		assert(outIndex < this->outputColumnBuffer.size());

		//Save data for each column in a buffer for special reordering.
		this->outputColumnBuffer[outIndex].write(data, size);
		return true; //done for now -- column contents will be written out at the end of the line
	}

	//Have an empty string outputted on the current column.
	void writeEmpty()
	{
		assert(!this->outputIndex.empty());
		const int unsigned outIndex = this->outputIndex[this->curColumnIndex++];

		//Should not be calling writeEmpty() for columns that are not written out to any location
		assert(outIndex < this->outputColumnBuffer.size());

		//Save data for each column in a buffer for special reordering.
		this->outputColumnBuffer[outIndex].reset();
	}

	//Output a separator between columns.
	bool writeSeparator(const void*, const size_t) {
		return true; //we don't need to write any separator now
	}

	//Completes the current row/line of text.
	bool writeEndline(const void* data, const size_t size) {
		this->curColumnIndex = 0; //ready to receive next line

		if (!this->fp)
			return true; //nothing to do

		//Output column values in specified order.
		//A single fwrite is noticeably faster than one for each column value.
		std::string str;
		str.reserve(this->outputColumnBuffer.size() * 16); //prepare an expected size
		std::vector<ByteBuffer>::const_iterator colBuf;
		for (colBuf = this->outputColumnBuffer.begin(); colBuf != this->outputColumnBuffer.end(); ++colBuf) {
			if (colBuf != this->outputColumnBuffer.begin())
				str.append(1, '\t'); //force column separators to be tabs for now
			colBuf->print(str);
		}
		str.append(static_cast<const char*>(data), size); //append the endline chars
		return (fwrite(str.c_str(), str.size(), 1, this->fp) == 1);
	}

	//Call to reorder column outputs in each row/line of text.
	//Input: a vector with an unordered sequence of zero-based indices; values of -1 are ignored
	//
	//Returns: whether the ordering provided was accepted
	bool setOutputColumnOrder(const int* pOutputOrder, const int numColumns) {
		//Start empty.
		this->outputIndex.clear();
		this->outputColumnBuffer.clear();

		//Populate column order and buffer to hold each column output.
		int max_val = -1; //enforce no gaps in the sequence
		for (int i = 0; i < numColumns; ++i) {
			const int val = pOutputOrder[i];
			if (val != -1) { //skip -1 values (use this to indicate a column is being omitted)
				assert(val >= 0); //negative values are not supported
				this->outputIndex.push_back(val); //this column should be outputted at this index
				this->outputColumnBuffer.push_back(ByteBuffer());

				if (val > max_val)
					max_val = val;
			}
		}

		//the sequence should have no gaps or repetition in the ordering
		//i.e. the number of elements added should equal the largest value encountered
		//     (actually, one greater than the highest zero-based index)
		//CAVEAT: this doesn't catch pathological cases (e.g. "2, 2, 2" where the size is one greater than the max_val)
		return (max_val + 1) == int(this->outputIndex.size());
	}
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
	BufferedOutput(FILE* fp, const size_t capacity = 16 * 1024)
		: fp(fp)
		, capacity(capacity)
		, buffer(fp ? new char[capacity] : NULL)
		, index(0)
	{
	}
	~BufferedOutput()
	{
		flush();
		delete[] buffer;
	}

	//Returns: whether operation succeeded
	bool flush()
	{
		if (!this->index)
			return true; //nothing to write

		assert(this->fp);
		const size_t out = fwrite(buffer, this->index, 1, this->fp);
		this->index = 0;
		return out == 1;
	}
	bool write(const void* data, const size_t size)
	{
		if (!this->fp)
			return true; //nowhere to write -- nothing to do

		bool bRet = true;
		if (this->index + size >= this->capacity)
			bRet &= flush();
		if (size >= this->capacity)
		{
			//Buffer is not large enough to store -- write the data immediately.
			const size_t out = fwrite(data, size, 1, this->fp);
			bRet &= (out == 1);
		} else {
			//Store for a later write to the file.
			memcpy(this->buffer + this->index, data, size);
			this->index += size;
		}
		return bRet;
	}
	void writeEmpty() { }

	//Output a separator between columns.
	bool writeSeparator(const void* data, const size_t size) { return write(data, size); }

	//Completes the current row/line of text.
	bool writeEndline(const void* data, const size_t size) { return write(data, size); }

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

	BufferedOutputInMem(size_t neededBufferSize, bool bUseInternalBuffer = true)
		: ppBuffer(NULL)
		, pBuffer(NULL)
		, pBufferSize(NULL)
		, index(0), columnNum(0)
		, pColumns(NULL)
		, pColumnsBuffer(NULL)
		, pOutputOrder(NULL)
		, numColumns(0)
		, neededBufferSize(neededBufferSize)
		, bUseInternalBuffer(bUseInternalBuffer)
		, bNeedReorder(false)
	{
		if (bUseInternalBuffer)
		{
			this->pBuffer = new char[neededBufferSize];
		}
	}

	~BufferedOutputInMem()
	{
		delete[] this->pOutputOrder;
		delete[] this->pColumnsBuffer;
		if (bUseInternalBuffer)
			delete [] this->pBuffer;
	}

	// invoke setOutputBuffer before parsing
	void setOutputBuffer(char** buffer, size_t *size)
	{
		assert(buffer);
		assert(*buffer);
		assert(size);
		assert(*size > 0);

		this->ppBuffer = buffer;
		this->pBuffer = *buffer;
		this->pBufferSize = size;

		checkBufferSize(neededBufferSize);
	}

	//Call whenever output needs to be directed to a new location.
	void setOutputColumnPtrs(const char** outColumns) {
		this->pColumns = outColumns;
		this->pColumns[0] = this->pBuffer; //first column will always be at start of buffer
	}

	bool setOutputColumnOrder(const int* pOutputOrder, const int numColumns) {

		if (!pOutputOrder || numColumns == 0)
		{
			return true;
		}

		this->pOutputOrder = new int[numColumns];
		int validNumColumns = 0;
		for (int i = 0; i < numColumns; i++)
		{
			if (pOutputOrder[i] == -1)
			{
				continue;
			}
			this->pOutputOrder[validNumColumns] = pOutputOrder[i];
			validNumColumns++;
		}

		this->numColumns = validNumColumns;
		this->pColumnsBuffer = new const char*[validNumColumns];

		this->bNeedReorder = true;
		return true;
	}

	bool write(const void* data, const size_t size) {
		assert(ppBuffer || bUseInternalBuffer);
		assert(pBuffer);

		//Add data to current column value.
		memcpy(this->pBuffer + this->index, data, size);
		this->index += size;
		assert(bUseInternalBuffer || this->index < *pBufferSize);
		return true;
	}
	void writeEmpty() { }

	bool writeSeparator(const void* /*data*/, const size_t /*size*/) {
		assert(ppBuffer || bUseInternalBuffer);
		assert(pBuffer);

		//Done with current column.
		//Terminate it and begin next column.
		this->pBuffer[this->index++] = 0; //null-delimiter
		assert(bUseInternalBuffer || this->index < *pBufferSize);
		this->pColumns[++this->columnNum] = this->pBuffer + this->index;
		return true;
	}

	bool writeEndline(const void* /*data*/, const size_t /*size*/) {
		assert(ppBuffer || bUseInternalBuffer);
		assert(pBuffer);

		//Done with line.  Terminate and reset.
		this->pBuffer[this->index] = 0; //null-delimiter
		this->currentRowLength = this->index;

		if (this->bNeedReorder)
		{
			reorderOutputColumn();
		}

		this->index = this->columnNum = 0; //get ready to read next line
		return true;
	}

	size_t getNumOutputColumns() {
		return this->numColumns;
	}
	//Set by UnconvertFromZDWToMemory to have this class report the number of columns in the ZDW file
	//Used when the caller does not explicitly request a set of columns
	void SetNumOutputColumns(size_t num) {
		this->numColumns = num;
	}

	size_t getCurrentRowLength() {
		return this->currentRowLength;
	}

private:
	void reorderOutputColumn()
	{
		if (!pOutputOrder || numColumns == 0 || !pColumns)
		{
			return;
		}

		for (int i = 0; i < numColumns; i++)
		{
			pColumnsBuffer[pOutputOrder[i]] = pColumns[i];
		}
		memcpy(pColumns, pColumnsBuffer, sizeof(const char **) * numColumns);
	}

	void checkBufferSize(const size_t requiredSize)
	{
		assert(pBufferSize);
		assert(ppBuffer);
		if (requiredSize > *pBufferSize){
			delete [] pBuffer;
			pBuffer = new char [requiredSize];
			*ppBuffer = pBuffer;
			*pBufferSize = requiredSize;
		}
	}

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

#endif
