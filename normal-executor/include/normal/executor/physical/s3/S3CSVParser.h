//
// Created by matt on 14/12/19.
//

#ifndef NORMAL_NORMAL_EXECUTOR_INCLUDE_NORMAL_EXECUTOR_PHYSICAL_S3_S3CSVPARSER_H
#define NORMAL_NORMAL_EXECUTOR_INCLUDE_NORMAL_EXECUTOR_PHYSICAL_S3_S3CSVPARSER_H

#include <normal/tuple/TupleSet.h>
#include <aws/core/Aws.h>
#include <string>

using namespace normal::tuple;

namespace normal::executor::physical::s3 {

class S3CSVParser {

private:

  static const int CSV_READER_BUFFER_SIZE = 128 * 1024;

  std::vector<std::string> columnNames_;
  std::shared_ptr<arrow::Schema> schema_;
  char csvDelimiter_;

  std::vector<unsigned char> partial{};

public:
  S3CSVParser(std::vector<std::string> columnNames,
				 std::shared_ptr<arrow::Schema> schema,
				 char csvDelimiter);

  static std::shared_ptr<S3CSVParser> make(const std::vector<std::string>& columnNames,
                                              const std::shared_ptr<arrow::Schema>& schema,
                                              char csvDelimiter);

  std::shared_ptr<TupleSet> parseCompletePayload(
      const std::vector<unsigned char, Aws::Allocator<unsigned char>>::iterator &from,
      const std::vector<unsigned char, Aws::Allocator<unsigned char>>::iterator &to);

  tl::expected<std::optional<std::shared_ptr<TupleSet>>, std::string> parse(Aws::Vector<unsigned char> &Vector);

};

}

#endif //NORMAL_NORMAL_EXECUTOR_INCLUDE_NORMAL_EXECUTOR_PHYSICAL_S3_S3CSVPARSER_H