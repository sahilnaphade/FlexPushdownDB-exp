//
// Created by matt on 14/8/20.
//

#ifndef NORMAL_NORMAL_EXECUTOR_INCLUDE_NORMAL_EXECUTOR_PHYSICAL_S3_S3SELECTSCANKERNEL_H
#define NORMAL_NORMAL_EXECUTOR_INCLUDE_NORMAL_EXECUTOR_PHYSICAL_S3_S3SELECTSCANKERNEL_H


#include <normal/executor/physical/s3/S3SelectCSVParseOptions.h>
#include <normal/catalogue/Table.h>
#include <normal/aws/AWSClient.h>
#include <normal/tuple/TupleSet.h>
#include <normal/tuple/FileType.h>
#include <string>
#include <vector>
#include <memory>

using namespace normal::catalogue;
using namespace normal::aws;
using namespace normal::tuple;

namespace normal::executor::physical::s3 {

class S3SelectScanKernel {

  typedef std::function<void(const std::shared_ptr<TupleSet> &)> TupleSetEventCallback;

public:
  S3SelectScanKernel(std::string s3Bucket,
					 std::string s3Object,
					 std::string sql,
					 std::optional<int64_t> startPos,
					 std::optional<int64_t> finishPos,
					 FileType fileType,
					 std::optional<S3SelectCSVParseOptions> csvParseOptions,
           std::shared_ptr<Table> table,
           std::shared_ptr<AWSClient> awsClient);

  static std::unique_ptr<S3SelectScanKernel> make(const std::string &s3Bucket,
												  const std::string &s3Object,
												  const std::string &sql,
												  std::optional<int64_t> startPos,
												  std::optional<int64_t> finishPos,
												  FileType fileType,
												  const std::optional<S3SelectCSVParseOptions> &csvParseOptions,
                          const std::shared_ptr<Table>& table,
                          const std::shared_ptr<AWSClient>& awsClient);

  tl::expected<std::shared_ptr<TupleSet>, std::string>
  scan(const std::vector<std::string> &columnNames);

  tl::expected<void, std::string> s3Select(const std::string &sql,
										   const std::vector<std::string> &columnNames,
										   const TupleSetEventCallback &tupleSetEventCallback);

  [[nodiscard]] const std::string &getS3Bucket() const;
  [[nodiscard]] const std::string &getS3Object() const;

  [[nodiscard]] std::optional<int64_t> getStartPos() const;
  [[nodiscard]] std::optional<int64_t> getFinishPos() const;

private:
  std::string s3Bucket_;
  std::string s3Object_;
  std::string sql_;
  std::optional<int64_t> startPos_;
  std::optional<int64_t> finishPos_;
  FileType fileType_;
  std::optional< S3SelectCSVParseOptions> csvParseOptions_;
  std::shared_ptr<Table> table_;
  std::shared_ptr<AWSClient> awsClient_;

};

}

#endif //NORMAL_NORMAL_EXECUTOR_INCLUDE_NORMAL_EXECUTOR_PHYSICAL_S3_S3SELECTSCANKERNEL_H
