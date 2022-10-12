//
// Created by Yifei Yang on 3/17/22.
//

#include <fpdb/executor/physical/bloomfilter/BloomFilterCreateKernel.h>
#include <fpdb/tuple/ArrayHasher.h>

namespace fpdb::executor::physical::bloomfilter {

BloomFilterCreateKernel::BloomFilterCreateKernel(const std::vector<std::string> columnNames,
                                                 double desiredFalsePositiveRate):
  columnNames_(columnNames),
  desiredFalsePositiveRate_(desiredFalsePositiveRate) {}

BloomFilterCreateKernel BloomFilterCreateKernel::make(const std::vector<std::string> columnNames,
                                                      double desiredFalsePositiveRate) {
  return {columnNames, desiredFalsePositiveRate};
}

const std::vector<std::string> BloomFilterCreateKernel::getColumnNames() const {
  return columnNames_;
}

double BloomFilterCreateKernel::getDesiredFalsePositiveRate() const {
  return desiredFalsePositiveRate_;
}

tl::expected<void, std::string> BloomFilterCreateKernel::bufferTupleSet(const std::shared_ptr<TupleSet> &tupleSet) {
  // buffer tupleSet
  if (!receivedTupleSet_.has_value()) {
    receivedTupleSet_ = tupleSet;
  }
  else {
    // here we should use "concatenate" instead of "append" to avoid modification on the original tupleSet,
    // because the original one is also passed to downstream operators
    auto expConcatenatedTupleSet = TupleSet::concatenate({(*receivedTupleSet_), tupleSet});
    if (!expConcatenatedTupleSet.has_value()) {
      return tl::make_unexpected(expConcatenatedTupleSet.error());
    }
    receivedTupleSet_ = *expConcatenatedTupleSet;
  }
  return {};
}

tl::expected<void, std::string> BloomFilterCreateKernel::buildBloomFilter() {
  // check
  if (!receivedTupleSet_.has_value()) {
    return tl::make_unexpected("No tupleSet received");
  }
  bloomFilter_ = std::make_shared<BloomFilter>((*receivedTupleSet_)->numRows(), desiredFalsePositiveRate_);
  if (!(*bloomFilter_)->valid()) {
    return {};
  }
  (*bloomFilter_)->init();

  // Make column indices
  std::vector<int> columnIndices;
  auto schema = (*receivedTupleSet_)->schema();
  for (const auto &columnName: columnNames_) {
    auto columnIndex = schema->GetFieldIndex(ColumnName::canonicalize(columnName));
    if (columnIndex == -1) {
      return tl::make_unexpected(fmt::format("Column '{}' does not exist", columnName));
    }
    columnIndices.emplace_back(columnIndex);
  }

  ::arrow::Result<std::shared_ptr<::arrow::RecordBatch>> recordBatchResult;
  ::arrow::Status status;
  auto table = (*receivedTupleSet_)->table();
  ::arrow::TableBatchReader reader(*table);
  reader.set_chunksize(DefaultChunkSize);

  // Read a batch
  recordBatchResult = reader.Next();
  if (!recordBatchResult.ok()) {
    return tl::make_unexpected(recordBatchResult.status().message());
  }
  auto recordBatch = *recordBatchResult;

  while (recordBatch) {
    auto result = addRecordBatchToBloomFilter(*recordBatch, columnIndices);
    if (!result.has_value()) {
      return tl::make_unexpected(result.error());
    }

    // Read a batch
    recordBatchResult = reader.Next();
    if (!recordBatchResult.ok()) {
      return tl::make_unexpected(recordBatchResult.status().message());
    }
    recordBatch = *recordBatchResult;
  }

  return {};
}

const std::optional<std::shared_ptr<BloomFilter>> &BloomFilterCreateKernel::getBloomFilter() const {
  return bloomFilter_;
}

tl::expected<void, std::string>
BloomFilterCreateKernel::addRecordBatchToBloomFilter(const ::arrow::RecordBatch &recordBatch,
                                                     const std::vector<int> &columnIndices) {
  // Create hashers to get int value of different types
  std::vector<std::shared_ptr<ArrayHasher>> columnHashers;
  for (const auto &columnIndex: columnIndices) {
    auto expColumnHasher = ArrayHasher::make(recordBatch.column(columnIndex));
    if (!expColumnHasher.has_value()) {
      return tl::make_unexpected(expColumnHasher.error());
    }
    columnHashers.emplace_back(*expColumnHasher);
  }

  for (int64_t r = 0; r < recordBatch.num_rows(); ++r) {
    int64_t hash = ArrayHasher::hash(columnHashers, r);
    (*bloomFilter_)->add(hash);
  }

  return {};
}

void BloomFilterCreateKernel::clear() {
  receivedTupleSet_.reset();
  bloomFilter_.reset();
}

}
