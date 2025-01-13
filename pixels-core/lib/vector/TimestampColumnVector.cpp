//
// Created by liyu on 12/23/23.
//

#include "vector/TimestampColumnVector.h"

TimestampColumnVector::TimestampColumnVector(int precision, bool encoding): ColumnVector(VectorizedRowBatch::DEFAULT_SIZE, encoding) {
    TimestampColumnVector(VectorizedRowBatch::DEFAULT_SIZE, precision, encoding);
}

TimestampColumnVector::TimestampColumnVector(uint64_t len, int precision, bool encoding): ColumnVector(len, encoding) {
    this->precision = precision;
    if(encoding) {
        posix_memalign(reinterpret_cast<void **>(&this->times), 64,
                       len * sizeof(long));
    } else {
        this->times = nullptr;
    }
}
void TimestampColumnVector::add(int64_t val){
    if(writeIndex>=length){
		ensureSize(writeIndex*2,1);
	}
	isNull[writeIndex]=false;
	writeIndex++;
}
void TimestampColumnVector::add(int val){
    if(writeIndex>=length){
		ensureSize(writeIndex*2,1);
	}
	isNull[writeIndex]=false;
    times[writeIndex]=  roundMillisToPrecision(val, precision);
	writeIndex++;
}
void TimestampColumnVector::ensureSize(uint64_t size,bool presever){
ColumnVector::ensureSize(size,presever);
	if(size<=length){
		return;
	}
	long* oldTS=times;
	posix_memalign(reinterpret_cast<void **>(&times), 32,
		size * sizeof(int64_t));
	if(presever){
		std::copy(oldTS,oldTS+length,times);
	}
	memoryUsage+= (uint64_t)sizeof(uint64_t)*(size-length);
	resize(size);
}

void TimestampColumnVector::close() {
    if(!closed) {
        ColumnVector::close();
        if(encoding && this->times != nullptr) {
            free(this->times);
        }
        this->times = nullptr;
    }
}

void TimestampColumnVector::print(int rowCount) {
    throw InvalidArgumentException("not support print longcolumnvector.");
//    for(int i = 0; i < rowCount; i++) {
//        std::cout<<longVector[i]<<std::endl;
//		std::cout<<intVector[i]<<std::endl;
//    }
}

TimestampColumnVector::~TimestampColumnVector() {
    if(!closed) {
        TimestampColumnVector::close();
    }
}

void * TimestampColumnVector::current() {
    if(this->times == nullptr) {
        return nullptr;
    } else {
        return this->times + readIndex;
    }
}

/**
     * Set a row from a value, which is the days from 1970-1-1 UTC.
     * We assume the entry has already been isRepeated adjusted.
     *
     * @param elementNum
     * @param days
 */
void TimestampColumnVector::set(int elementNum, long ts) {
    if(elementNum >= writeIndex) {
        writeIndex = elementNum + 1;
    }
    times[elementNum] = ts;
    // TODO: isNull
}