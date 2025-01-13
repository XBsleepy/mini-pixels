//
// Created by liyu on 12/23/23.
//

#ifndef DUCKDB_TIMESTAMPCOLUMNVECTOR_H
#define DUCKDB_TIMESTAMPCOLUMNVECTOR_H

#include "vector/ColumnVector.h"
#include "vector/VectorizedRowBatch.h"
#include <array>
class TimestampColumnVector : public ColumnVector
{
public:
    int precision;
    long *times;

    /**
     * Use this constructor by default. All column vectors
     * should normally be the default size.
     */

    explicit TimestampColumnVector(int precision, bool encoding = false);
    explicit TimestampColumnVector(uint64_t len, int precision, bool encoding = false);
    void *current() override;
    void set(int elementNum, long ts);
    ~TimestampColumnVector();
    void print(int rowCount) override;
    void close() override;
    void ensureSize(uint64_t size, bool preserver) override;
    void add(int64_t val) override;
    void add(int val) override;

    long roundMicrosToPrecision(long micros, int precision)
    {
        std::array<long, 7> PRECISION_ROUND_FACTOR_FOR_MICROS = {
            1000'000L, 100'000L, 10'000L, 1000L, 100L, 10L, 1L};
        long roundFactor = PRECISION_ROUND_FACTOR_FOR_MICROS[precision];
        return micros / roundFactor * roundFactor;
    }

    int roundMillisToPrecision(int millis, int precision)
    {
        std::array<int, 4> PRECISION_ROUND_FACTOR_FOR_MILLIS = {1000, 100, 10, 1};
        int roundFactor = PRECISION_ROUND_FACTOR_FOR_MILLIS[precision];
        return millis / roundFactor * roundFactor;
    }

private:
    bool isLong;
};
#endif // DUCKDB_TIMESTAMPCOLUMNVECTOR_H
