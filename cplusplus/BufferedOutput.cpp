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

#include "zdw/BufferedOutput.h"
#include <assert.h>
#include <stdio.h>
#include <string.h>


namespace adobe {
namespace zdw {

BufferedOrderedOutput::ByteBuffer::ByteBuffer(int unsigned startSize)
	: pBuffer(new char[startSize])
	, pos(0)
	, size(0)
	, capacity(startSize)
{ }

BufferedOrderedOutput::ByteBuffer::ByteBuffer(const ByteBuffer& rhs)
	: pBuffer(new char[rhs.capacity])
	, pos(rhs.pos == rhs.pBuffer ? pBuffer : rhs.pos)
	, size(rhs.size)
	, capacity(rhs.capacity)
{
	memcpy(this->pBuffer, rhs.pBuffer, this->size);
}

void BufferedOrderedOutput::ByteBuffer::swap(ByteBuffer& rhs)
{
	using std::swap;
	swap(this->pBuffer, rhs.pBuffer);
	swap(this->pos, rhs.pos);
	swap(this->size, rhs.size);
	swap(this->capacity, rhs.capacity);
}

BufferedOrderedOutput::ByteBuffer& BufferedOrderedOutput::ByteBuffer::operator=(const ByteBuffer& rhs)
{
	ByteBuffer(rhs).swap(*this); return *this;
}

BufferedOrderedOutput::ByteBuffer::~ByteBuffer()
{
	delete[] this->pBuffer;
}

//Write data to the start of the buffer.
inline void BufferedOrderedOutput::ByteBuffer::write(const void* data, const size_t dataSize)
{
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
	this->pos = this->pBuffer;
	this->size = dataSize;
}

inline void BufferedOrderedOutput::ByteBuffer::writePtr(const void* data, size_t dataSize)
{
	this->pos = data;
	this->size = dataSize;
}

inline void BufferedOrderedOutput::ByteBuffer::print(std::string& str) const
{
	str.append(static_cast<const char*>(this->pos), this->size);
}


BufferedOrderedOutput::BufferedOrderedOutput(FILE* fp)
	: fp(fp)
	, curColumnIndex(0)
{
	if (fp) {
		setbuf(fp, NULL); //disable additional buffering layer
	}
	this->outStr.reserve(16 * 1024);
}

BufferedOrderedOutput::~BufferedOrderedOutput()
{ }

bool BufferedOrderedOutput::write(const void* data, const size_t size)
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

bool BufferedOrderedOutput::writePtr(const void* data, const size_t size)
{
	//If we are reordering column outputs, then save a pointer to the output for when the row is complete.
	assert(!this->outputIndex.empty());
	const int unsigned outIndex = this->outputIndex[this->curColumnIndex++];

	//Should not be calling write() for columns that are not written out to any location
	assert(outIndex < this->outputColumnBuffer.size());

	//Save data for each column in a buffer for special reordering.
	this->outputColumnBuffer[outIndex].writePtr(data, size);
	return true; //done for now -- column contents will be written out at the end of the line
}

//Have an empty string outputted on the current column.
void BufferedOrderedOutput::writeEmpty()
{
	assert(!this->outputIndex.empty());
	const int unsigned outIndex = this->outputIndex[this->curColumnIndex++];

	//Should not be calling writeEmpty() for columns that are not written out to any location
	assert(outIndex < this->outputColumnBuffer.size());

	//Save data for each column in a buffer for special reordering.
	this->outputColumnBuffer[outIndex].reset();
}

//Completes the current row/line of text.
bool BufferedOrderedOutput::writeEndline(const void* data, const size_t size)
{
	this->curColumnIndex = 0; //ready to receive next line

	if (!this->fp) {
		return true; //nothing to do
	}

	//Output column values in specified order.
	assert(!this->outputColumnBuffer.empty());

	//A single fwrite is noticeably faster than one for each column value.
	this->outStr.clear();
	std::vector<ByteBuffer>::const_iterator colBuf = this->outputColumnBuffer.begin();
	colBuf->print(this->outStr);
	for ( ; colBuf != this->outputColumnBuffer.end(); ++colBuf) {
		this->outStr.append(1, '\t'); //force column separators to be tabs for now
		colBuf->print(this->outStr);
	}
	this->outStr.append(static_cast<const char*>(data), size); //append the endline chars
	return (fwrite(this->outStr.c_str(), this->outStr.size(), 1, this->fp) == 1);
}

bool BufferedOrderedOutput::writeRawLine(const void* data, const size_t size)
{
	if (!this->fp) {
		return true; //nothing to do
	}

	return fwrite(data, size, 1, this->fp) == 1;
}

//Call to reorder column outputs in each row/line of text.
//Input: a vector with an unordered sequence of zero-based indices; values of -1 are ignored
//
//Returns: whether the ordering provided was accepted
bool BufferedOrderedOutput::setOutputColumnOrder(const int* pOutputOrder, const int numColumns)
{
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

			if (val > max_val) {
				max_val = val;
			}
		}
	}

	//the sequence should have no gaps or repetition in the ordering
	//i.e. the number of elements added should equal the largest value encountered
	//     (actually, one greater than the highest zero-based index)
	//CAVEAT: this doesn't catch pathological cases (e.g. "2, 2, 2" where the size is one greater than the max_val)
	return (max_val + 1) == int(this->outputIndex.size());
}


BufferedOutput::BufferedOutput(FILE* fp, const size_t capacity)
	: fp(fp)
	, capacity(capacity)
	, buffer(fp ? new char[capacity] : NULL)
	, index(0)
{
	if (fp) {
		setbuf(fp, NULL); //disable additional buffering layer
	}
}

BufferedOutput::~BufferedOutput()
{
	flush();
	delete[] buffer;
}

//Returns: whether operation succeeded
bool BufferedOutput::flush()
{
	if (!this->index) {
		return true; //nothing to write
	}

	assert(this->fp);
	const size_t out = fwrite(buffer, this->index, 1, this->fp);
	this->index = 0;
	return out == 1;
}

bool BufferedOutput::write(const void* data, const size_t size)
{
	if (!this->fp) {
		return true; //nowhere to write -- nothing to do
	}

	bool bRet = true;
	if (this->index + size >= this->capacity) {
		bRet &= flush();
	}
	if (size >= this->capacity) {
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


int compareByIndex(const void* first, const void* second)
{
	return (reinterpret_cast<const OutputOrderIndexer *>(first))->index
			- (reinterpret_cast<const OutputOrderIndexer *>(second))->index;
}

int compareByOutputIndex(const void* first, const void* second)
{
	return (reinterpret_cast<const OutputOrderIndexer *>(first))->outputIndex
			- (reinterpret_cast<const OutputOrderIndexer *>(second))->outputIndex;
}


BufferedOutputInMem::BufferedOutputInMem(size_t neededBufferSize, bool bUseInternalBuffer)
	: ppBuffer(NULL)
	, pBuffer(NULL)
	, pBufferSize(NULL)
	, index(0), columnNum(0)
	, pColumns(NULL)
	, pColumnsBuffer(NULL)
	, pOutputOrder(NULL)
	, numColumns(0)
	, currentRowLength(0)
	, neededBufferSize(neededBufferSize)
	, bUseInternalBuffer(bUseInternalBuffer)
	, bNeedReorder(false)
{
	if (bUseInternalBuffer) {
		this->pBuffer = new char[neededBufferSize];
	}
}

BufferedOutputInMem::~BufferedOutputInMem()
{
	delete[] this->pOutputOrder;
	delete[] this->pColumnsBuffer;
	if (bUseInternalBuffer) {
		delete [] this->pBuffer;
	}
}

// invoke setOutputBuffer before parsing
void BufferedOutputInMem::setOutputBuffer(char** buffer, size_t *size)
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
void BufferedOutputInMem::setOutputColumnPtrs(const char** outColumns)
{
	this->pColumns = outColumns;
	this->pColumns[0] = this->pBuffer; //first column will always be at start of buffer
}

bool BufferedOutputInMem::setOutputColumnOrder(const int* pOutputOrder, const int numColumns)
{
	if (!pOutputOrder || numColumns == 0) {
		return true;
	}

	this->pOutputOrder = new int[numColumns];
	int validNumColumns = 0;
	for (int i = 0; i < numColumns; i++) {
		if (pOutputOrder[i] == -1) {
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

bool BufferedOutputInMem::write(const void* data, const size_t size)
{
	assert(ppBuffer || bUseInternalBuffer);
	assert(pBuffer);

	//Add data to current column value.
	memcpy(this->pBuffer + this->index, data, size);
	this->index += size;
	assert(bUseInternalBuffer || this->index < *pBufferSize);
	return true;
}

bool BufferedOutputInMem::writeSeparator(const void* /*data*/, const size_t /*size*/)
{
	assert(ppBuffer || bUseInternalBuffer);
	assert(pBuffer);

	//Done with current column.
	//Terminate it and begin next column.
	this->pBuffer[this->index++] = 0; //null-delimiter
	assert(bUseInternalBuffer || this->index < *pBufferSize);
	this->pColumns[++this->columnNum] = this->pBuffer + this->index;
	return true;
}

bool BufferedOutputInMem::writeEndline(const void* /*data*/, const size_t /*size*/)
{
	assert(ppBuffer || bUseInternalBuffer);
	assert(pBuffer);

	//Done with line.  Terminate and reset.
	this->pBuffer[this->index] = 0; //null-delimiter
	this->currentRowLength = this->index;

	if (this->bNeedReorder) {
		reorderOutputColumn();
	}

	this->index = this->columnNum = 0; //get ready to read next line
	return true;
}

bool BufferedOutputInMem::writeRawLine(const void* data, const size_t size)
{
	assert(ppBuffer || bUseInternalBuffer);
	assert(pBuffer);

	memcpy(this->pBuffer, data, size);
	this->pBuffer[size] = 0;
	this->currentRowLength = size;

	assert(bUseInternalBuffer || size < *pBufferSize);
	return true;
}

void BufferedOutputInMem::reorderOutputColumn()
{
	if (!pOutputOrder || numColumns == 0 || !pColumns) {
		return;
	}

	for (int i = 0; i < numColumns; i++) {
		pColumnsBuffer[pOutputOrder[i]] = pColumns[i];
	}
	memcpy(pColumns, pColumnsBuffer, sizeof(const char **) * numColumns);
}

void BufferedOutputInMem::checkBufferSize(const size_t requiredSize)
{
	assert(pBufferSize);
	assert(ppBuffer);
	if (requiredSize > *pBufferSize) {
		delete [] pBuffer;
		pBuffer = new char [requiredSize];
		*ppBuffer = pBuffer;
		*pBufferSize = requiredSize;
	}
}

} // namespace zdw
} // namespace adobe

