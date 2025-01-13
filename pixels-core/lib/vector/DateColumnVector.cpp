//
// Created by yuly on 06.04.23.
//

#include "vector/DateColumnVector.h"
#include <iostream>
#include <iomanip> // 需要 std::get_time
#include <sstream>
#include <ctime>
DateColumnVector::
DateColumnVector(uint64_t len, bool encoding) : ColumnVector(len, encoding)
{

posix_memalign(reinterpret_cast<void **>(&dates), 32,
					   len * sizeof(int32_t));

	memoryUsage += (long)sizeof(int) * len;
}

void DateColumnVector::add(int value)
{
	if (writeIndex >= length)
	{
		ensureSize(writeIndex * 2, 1);
	}
	dates[writeIndex] = value;
	isNull[writeIndex] = false;
	writeIndex++;
}
void DateColumnVector::add(std::string &val)
{
	int year, month, day;

	// 用字符串流手动解析
	std::istringstream ss(val);

	// 用 "-" 作为分隔符分割日期
	char dash1, dash2;
	ss >> year >> dash1 >> month >> dash2 >> day;

	// 检查分隔符是否正确
	if (ss.fail() || dash1 != '-' || dash2 != '-')
	{
		std::cerr << "Invalid date format!" << std::endl;
		return;
	}

	// 填充 std::tm 结构体
	struct tm time = {};
	time.tm_year = year - 1900; // tm_year 从1900年开始
	time.tm_mon = month - 1;	// tm_mon 范围是 0-11，所以需要减去1
	time.tm_mday = day;

	// 将 tm 结构体转换为时间戳（秒）
	time_t timestamp = mktime(&time);

	if (timestamp == -1)
	{
		std::cerr << "Error converting to timestamp!" << std::endl;
		return;
	}
	this->add(timestamp);
}
void DateColumnVector::ensureSize(uint64_t size, bool preserveData)
{
	ColumnVector::ensureSize(size, preserveData);
	if (length < size)
	{
		int *oldTime = dates;
		posix_memalign(reinterpret_cast<void **>(&dates), 32,
					   size * sizeof(int32_t));
		if (preserveData)
		{
			std::copy(oldTime, oldTime + length, dates);
		}
		memoryUsage += (uint64_t)sizeof(int) * (size - length);
		delete[] oldTime;
		resize(size);
	}
}

void DateColumnVector::close()
{
	if (!closed)
	{
		if (encoding && dates != nullptr)
		{
			free(dates);
		}
		dates = nullptr;
		ColumnVector::close();
	}
}

void DateColumnVector::print(int rowCount)
{
	for (int i = 0; i < rowCount; i++)
	{
		std::cout << dates[i] << std::endl;
	}
}

DateColumnVector::~DateColumnVector()
{
	if (!closed)
	{
		DateColumnVector::close();
	}
}

/**
 * Set a row from a value, which is the days from 1970-1-1 UTC.
 * We assume the entry has already been isRepeated adjusted.
 *
 * @param elementNum
 * @param days
 */
void DateColumnVector::set(int elementNum, int days)
{
	if (elementNum >= writeIndex)
	{
		writeIndex = elementNum + 1;
	}
	dates[elementNum] = days;
	// TODO: isNull
}

void *DateColumnVector::current()
{
	if (dates == nullptr)
	{
		return nullptr;
	}
	else
	{
		return dates + readIndex;
	}
}
