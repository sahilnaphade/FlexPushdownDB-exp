//
// Created by Yifei Yang on 11/23/22.
//

#include <fpdb/executor/physical/bloomfilter/ArrowBloomFilter.h>
#include <fpdb/executor/physical/Globals.h>

namespace fpdb::executor::physical::bloomfilter {

ArrowBloomFilter::ArrowBloomFilter(int64_t capacity, const std::vector<std::string> &columnNames):
  BloomFilterBase(BloomFilterType::ARROW_BLOOM_FILTER, capacity, capacity <= BLOOM_FILTER_MAX_INPUT_SIZE),
  columnNames_(columnNames) {}

std::shared_ptr<ArrowBloomFilter> ArrowBloomFilter::make(int64_t capacity, 
                                                         const std::vector<std::string> &columnNames) {
  return std::make_shared<ArrowBloomFilter>(capacity, columnNames);
}

const std::shared_ptr<arrow::compute::BlockedBloomFilter> &ArrowBloomFilter::getBlockedBloomFilter() const {
  return blockedBloomFilter_;
}

tl::expected<void, std::string> ArrowBloomFilter::build(const std::shared_ptr<TupleSet> &tupleSet) {
  // make hasher
  auto expHasher = RecordBatchHasher::make(tupleSet->schema(), columnNames_);
  if (!expHasher.has_value()) {
    return tl::make_unexpected(expHasher.error());
  }
  hasher_ = *expHasher;
  
  // init bloom filter
  using namespace arrow::compute;
  blockedBloomFilter_ = std::make_shared<BlockedBloomFilter>();
  auto hardwareFlags = arrow::internal::CpuInfo::GetInstance()->hardware_flags();
  auto builder = BloomFilterBuilder::Make(BloomFilterBuildStrategy::SINGLE_THREADED);
  auto res = builder->Begin(1, hardwareFlags, arrow::default_memory_pool(), tupleSet->numRows(), 0, 
                            blockedBloomFilter_.get());
  if (!res.ok()) {
    return tl::make_unexpected(res.message());
  }
  
  // push batches into bloom filter
  arrow::TableBatchReader reader{*tupleSet->table()};
  auto expRecordBatch = reader.Next();
  if (!expRecordBatch.ok()) {
    return tl::make_unexpected(expRecordBatch.status().message());
  }
  auto recordBatch = *expRecordBatch;
  while (recordBatch) {
    uint32_t* hashes = (uint32_t*) malloc(sizeof(uint32_t) * recordBatch->num_rows());
    hasher_->hash(recordBatch, hashes);
    res = builder->PushNextBatch(0, recordBatch->num_rows(), hashes);
    if (!res.ok()) {
      // clear
      free(hashes);
      return tl::make_unexpected(res.message());
    }

    // clear
    free(hashes);
    expRecordBatch = reader.Next();
    if (!expRecordBatch.ok()) {
      return tl::make_unexpected(expRecordBatch.status().message());
    }
    recordBatch = *expRecordBatch;
  }

  return {};
}

::nlohmann::json ArrowBloomFilter::toJson() const {
  // TODO
  ::nlohmann::json jObj;
  return jObj;
}

tl::expected<std::shared_ptr<ArrowBloomFilter>, std::string> ArrowBloomFilter::fromJson(const nlohmann::json &jObj) {
  // TODO
  return make(0, {});
}

}
