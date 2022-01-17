//
// Created by Matt Woicik on 1/19/21.
//

#ifndef NORMAL_NORMAL_EXECUTOR_INCLUDE_NORMAL_EXECUTOR_PHYSICAL_S3SELECT_H
#define NORMAL_NORMAL_EXECUTOR_INCLUDE_NORMAL_EXECUTOR_PHYSICAL_S3SELECT_H

#include <normal/executor/physical/s3/S3SelectScanAbstractPOp.h>
#include <aws/s3/model/InputSerialization.h>

#ifdef __AVX2__
#include <normal/tuple/arrow/CSVToArrowSIMDChunkParser.h>
#endif

namespace normal::executor::physical::s3 {

// This is for controlling the maximum number of Select requests converting data at the same time
// as per maxConcurrentArrowConversions in normal-pushdown/include/normal/pushdown/Globals.h
extern std::mutex SelectConvertLock;
extern int activeSelectConversions;

class S3SelectPOp: public S3SelectScanAbstractPOp {
public:
  S3SelectPOp(std::string name,
         std::string s3Bucket,
         std::string s3Object,
         std::string filterSql,
         std::vector<std::string> projectColumnNames,
         int64_t startOffset,
         int64_t finishOffset,
         std::shared_ptr<Table> table,
         std::shared_ptr<AWSClient> awsClient,
         bool scanOnStart = true,
         bool toCache = false,
         std::vector<std::shared_ptr<normal::cache::SegmentKey>> weightedSegmentKeys = {});
  S3SelectPOp() = default;
  S3SelectPOp(const S3SelectPOp&) = default;
  S3SelectPOp& operator=(const S3SelectPOp&) = default;

private:
#ifdef __AVX2__
  std::shared_ptr<CSVToArrowSIMDChunkParser> generateSIMDCSVParser();
#endif
  std::shared_ptr<S3CSVParser> generateCSVParser();
  Aws::Vector<unsigned char> s3Result_{};

  // Scan range not supported in AWS for GZIP and BZIP2 CSV. We also don't support this for parquet yet either
  // as that is more complicated due to involving parquet metadata and we haven't had a chance to implement this yet
  bool scanRangeSupported();
  Aws::S3::Model::InputSerialization getInputSerialization();
  std::shared_ptr<TupleSet> s3Select(uint64_t startOffset, uint64_t endOffset);
  std::shared_ptr<TupleSet> s3SelectParallelReqs();
  // Wrapper function to encapsulate a thread spawned when making parallel requests
  void s3SelectIndividualReq(int reqNum, uint64_t startOffset, uint64_t endOffset);

  void processScanMessage(const ScanMessage &message) override;

  std::shared_ptr<TupleSet> readTuples() override;
  int getPredicateNum();
  void sendSegmentWeight();

  std::string filterSql_;   // "where ...."
  std::shared_ptr<S3CSVParser> parser_;

  /**
   * used to compute filter weight
   */
  std::vector<std::shared_ptr<normal::cache::SegmentKey>> weightedSegmentKeys_;

// caf inspect
public:
  template <class Inspector>
  friend bool inspect(Inspector& f, S3SelectPOp& op) {
    return f.object(op).fields(f.field("name", op.name_),
                               f.field("type", op.type_),
                               f.field("projectColumnNames", op.projectColumnNames_),
                               f.field("queryId", op.queryId_),
                               f.field("opContext", op.opContext_),
                               f.field("producers", op.producers_),
                               f.field("consumers", op.consumers_),
                               f.field("s3Bucket", op.s3Bucket_),
                               f.field("s3Object", op.s3Object_),
                               f.field("filterSql", op.filterSql_),
                               f.field("startOffset", op.startOffset_),
                               f.field("finishOffset", op.finishOffset_),
                               f.field("table", op.table_),
                               f.field("scanOnStart", op.scanOnStart_),
                               f.field("toCache", op.toCache_),
                               f.field("weightedSegmentKeys", op.weightedSegmentKeys_));
  }
};

}

#endif //NORMAL_NORMAL_EXECUTOR_INCLUDE_NORMAL_EXECUTOR_PHYSICAL_S3SELECT_H
