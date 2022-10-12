//
// Created by Yifei Yang on 3/17/22.
//

#ifndef FPDB_FPDB_EXECUTOR_INCLUDE_FPDB_EXECUTOR_PHYSICAL_BLOOMFILTER_BLOOMFILTERCREATEKERNEL_H
#define FPDB_FPDB_EXECUTOR_INCLUDE_FPDB_EXECUTOR_PHYSICAL_BLOOMFILTER_BLOOMFILTERCREATEKERNEL_H

#include <fpdb/executor/physical/bloomfilter/BloomFilter.h>
#include <fpdb/tuple/TupleSet.h>

using namespace fpdb::tuple;

namespace fpdb::executor::physical::bloomfilter {

/**
 * Op to add elements to an empty bloom filter created by BloomFilterCreatePreparePOp
 */
class BloomFilterCreateKernel {

public:
  BloomFilterCreateKernel(const std::vector<std::string> columnNames, double desiredFalsePositiveRate);
  BloomFilterCreateKernel() = default;
  BloomFilterCreateKernel(const BloomFilterCreateKernel&) = default;
  BloomFilterCreateKernel& operator=(const BloomFilterCreateKernel&) = default;
  ~BloomFilterCreateKernel() = default;
  
  static BloomFilterCreateKernel make(const std::vector<std::string> columnNames, double desiredFalsePositiveRate);

  const std::vector<std::string> getColumnNames() const;
  double getDesiredFalsePositiveRate() const;

  tl::expected<void, std::string> bufferTupleSet(const std::shared_ptr<TupleSet> &tupleSet);
  tl::expected<void, std::string> buildBloomFilter();
  const std::optional<std::shared_ptr<BloomFilter>> &getBloomFilter() const;

  void clear();

private:
  tl::expected<void, std::string> addRecordBatchToBloomFilter(const ::arrow::RecordBatch &recordBatch,
                                                              const std::vector<int> &columnIndices);

  std::vector<std::string> columnNames_;
  double desiredFalsePositiveRate_;

  std::optional<std::shared_ptr<TupleSet>> receivedTupleSet_;
  std::optional<std::shared_ptr<BloomFilter>> bloomFilter_;

// caf inspect
public:
  template <class Inspector>
  friend bool inspect(Inspector& f, BloomFilterCreateKernel& kernel) {
    return f.object(kernel).fields(f.field("columnNames", kernel.columnNames_),
                                   f.field("desiredFalsePositiveRate", kernel.desiredFalsePositiveRate_));
  }
};

}


#endif //FPDB_FPDB_EXECUTOR_INCLUDE_FPDB_EXECUTOR_PHYSICAL_BLOOMFILTER_BLOOMFILTERCREATEKERNEL_H
