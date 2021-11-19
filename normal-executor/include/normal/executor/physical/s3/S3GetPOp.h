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
             std::vector<std::string> returnedS3ColumnNames,
             std::vector<std::string> neededColumnNames,
             int64_t startOffset,
             int64_t finishOffset,
             std::shared_ptr<Table> table,
             std::shared_ptr<AWSClient> awsClient,
             bool scanOnStart,
             bool toCache,
             long queryId,
             std::shared_ptr<std::vector<std::shared_ptr<normal::cache::SegmentKey>>> weightedSegmentKeys);

    static std::shared_ptr<S3GetPOp> make(const std::string& name,
                                          const std::string& s3Bucket,
                                          const std::string& s3Object,
                                          const std::vector<std::string>& returnedS3ColumnNames,
                                          const std::vector<std::string>& neededColumnNames,
                                          int64_t startOffset,
                                          int64_t finishOffset,
                                          const std::shared_ptr<Table>& table,
                                          const std::shared_ptr<AWSClient>& awsClient,
                                          bool scanOnStart = true,
                                          bool toCache = false,
                                          long queryId = 0,
                                          const std::shared_ptr<std::vector<std::shared_ptr<normal::cache::SegmentKey>>>& weightedSegmentKeys = nullptr);

  private:
    std::shared_ptr<TupleSet2> readCSVFile(std::shared_ptr<arrow::io::InputStream> &arrowInputStream);
    std::shared_ptr<TupleSet2> readParquetFile(std::basic_iostream<char, std::char_traits<char>> &retrievedFile);
    std::shared_ptr<TupleSet2> s3GetFullRequest();
    Aws::S3::Model::GetObjectResult s3GetRequestOnly(const std::string &s3Object, uint64_t startOffset, uint64_t endOffset);

    // Whether we can process different portions of the response in parallel
    // For now we only support this for uncompressed CSV, but eventually we work
    // with parquet more we should be able to turn on a flag to have arrow do this as well,
    // the methods will just be a bit different
    bool parallelTuplesetCreationSupported();

#ifdef __AVX2__
    void s3GetIndividualReq(int reqNum, const std::string &s3Object, uint64_t startOffset, uint64_t endOffset);
    std::shared_ptr<TupleSet2> s3GetParallelReqs(bool tempFixForAirmettleCSV150MB);
#endif

    // Used for collecting all results for split requests that are run in parallel, and for having a
    // locks on shared variables when requests are split.
    std::mutex splitReqLock_;
    std::map<int, std::shared_ptr<arrow::Table>> splitReqNumToTable_;
    std::unordered_map<int, std::vector<char>> reqNumToAdditionalOutput_;
    #ifdef __AVX2__
    std::unordered_map<int, std::shared_ptr<CSVToArrowSIMDChunkParser>> reqNumToParser_;
    #endif


    void processScanMessage(const ScanMessage &message) override;

    std::shared_ptr<TupleSet2> readTuples() override;
    int getPredicateNum() override;
};

}

#endif //NORMAL_NORMAL_EXECUTOR_INCLUDE_NORMAL_EXECUTOR_PHYSICAL_S3_S3GETPOP_H
