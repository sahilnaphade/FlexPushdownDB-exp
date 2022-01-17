//
// Created by Matt Woicik on 1/19/21.
//

#ifndef NORMAL_NORMAL_EXECUTOR_INCLUDE_NORMAL_EXECUTOR_PHYSICAL_S3_S3GETPOP_H
#define NORMAL_NORMAL_EXECUTOR_INCLUDE_NORMAL_EXECUTOR_PHYSICAL_S3_S3GETPOP_H

#include <normal/executor/physical/s3/S3SelectScanAbstractPOp.h>
#include <aws/s3/model/GetObjectResult.h>

#ifdef __AVX2__
#include <normal/tuple/arrow/CSVToArrowSIMDChunkParser.h>
#endif

namespace normal::executor::physical::s3 {

// This is for controlling the maximum number of GET requests converting data at the same time
// as per maxConcurrentGetConversions in <normal/executor/physical/Globals.h>
extern std::mutex GetConvertLock;
extern int activeGetConversions;

class S3GetPOp : public S3SelectScanAbstractPOp {
public:
  S3GetPOp(std::string name,
           std::string s3Bucket,
           std::string s3Object,
           std::vector<std::string> projectColumnNames,
           int64_t startOffset,
           int64_t finishOffset,
           std::shared_ptr<Table> table,
           std::shared_ptr<AWSClient> awsClient,
           bool scanOnStart = true,
           bool toCache = false);
  S3GetPOp() = default;
  S3GetPOp(const S3GetPOp&) = default;
  S3GetPOp& operator=(const S3GetPOp&) = default;

private:
  void processScanMessage(const ScanMessage &message) override;

  std::shared_ptr<TupleSet> readTuples() override;
  std::shared_ptr<TupleSet> readCSVFile(std::shared_ptr<arrow::io::InputStream> &arrowInputStream);
  std::shared_ptr<TupleSet> readParquetFile(std::basic_iostream<char, std::char_traits<char>> &retrievedFile);
  std::shared_ptr<TupleSet> s3GetFullRequest();
  Aws::S3::Model::GetObjectResult s3GetRequestOnly(const std::string &s3Object, uint64_t startOffset, uint64_t endOffset);

  // Whether we can process different portions of the response in parallel
  // For now we only support this for uncompressed CSV, but eventually we work
  // with parquet more we should be able to turn on a flag to have arrow do this as well,
  // the methods will just be a bit different
  bool parallelTuplesetCreationSupported();

#ifdef __AVX2__
  void s3GetIndividualReq(int reqNum, const std::string &s3Object, uint64_t startOffset, uint64_t endOffset);
  std::shared_ptr<TupleSet> s3GetParallelReqs(bool tempFixForAirmettleCSV150MB);
#endif

  std::unordered_map<int, std::vector<char>> reqNumToAdditionalOutput_;
#ifdef __AVX2__
  std::unordered_map<int, std::shared_ptr<CSVToArrowSIMDChunkParser>> reqNumToParser_;
#endif

// caf inspect
public:
  template <class Inspector>
  friend bool inspect(Inspector& f, S3GetPOp& op) {
    return f.object(op).fields(f.field("name", op.name_),
                               f.field("type", op.type_),
                               f.field("projectColumnNames", op.projectColumnNames_),
                               f.field("queryId", op.queryId_),
                               f.field("opContext", op.opContext_),
                               f.field("producers", op.producers_),
                               f.field("consumers", op.consumers_),
                               f.field("s3Bucket", op.s3Bucket_),
                               f.field("s3Object", op.s3Object_),
                               f.field("startOffset", op.startOffset_),
                               f.field("finishOffset", op.finishOffset_),
                               f.field("table", op.table_),
                               f.field("scanOnStart", op.scanOnStart_),
                               f.field("toCache", op.toCache_));
  }
};

}

#endif //NORMAL_NORMAL_EXECUTOR_INCLUDE_NORMAL_EXECUTOR_PHYSICAL_S3_S3GETPOP_H
