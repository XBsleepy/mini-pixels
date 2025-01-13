//
// Created by yuly on 06.04.23.
//

#include "vector/DateColumnVector.h"
#include <iostream>
#include <iomanip> // 需要 std::get_time
#include <sstream>
#include <ctime>
#include <chrono>
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

	std::istringstream ss(val);

	
	char dash1, dash2;
	ss >> year >> dash1 >> month >> dash2 >> day;

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
	time.tm_isdst=-1;
	// 将 tm 结构体转换为时间戳（秒）
	time_t timestamp = mktime(&time);
	std::tm epoch = {0};
	epoch.tm_year = 70;
	epoch.tm_mon = 0;
	epoch.tm_mday = 1;
	long epoch_ts = mktime(&epoch);
	std::chrono::system_clock::time_point tp = std::chrono::system_clock::from_time_t(timestamp);
	std::chrono::system_clock::time_point epoch_tp = std::chrono::system_clock::from_time_t(epoch_ts);
	auto days=std::chrono::duration_cast<std::chrono::hours>(tp-epoch_tp).count()/24;
	this->add(days);
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
