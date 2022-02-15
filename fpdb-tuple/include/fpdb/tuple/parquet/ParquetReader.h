//
// Created by matt on 12/8/20.
//

#ifndef FPDB_FPDB_TUPLE_INCLUDE_FPDB_TUPLE_PARQUETREADER_H
#define FPDB_FPDB_TUPLE_INCLUDE_FPDB_TUPLE_PARQUETREADER_H

#include <fpdb/tuple/FileReader.h>
#include <fpdb/tuple/TupleSet.h>
#include <tl/expected.hpp>
#include <arrow/io/api.h>
#include <parquet/arrow/reader.h>
#include <string>
#include <memory>

namespace fpdb::tuple::parquet {

class ParquetReader : public FileReader {

public:
  explicit ParquetReader(const std::string &path,
                         const std::shared_ptr<FileFormat> &format,
                         const std::shared_ptr<::arrow::Schema> &schema);
  ~ParquetReader() = default;

  static std::shared_ptr<ParquetReader> make(const std::string &path,
                                             const std::shared_ptr<FileFormat> &format,
                                             const std::shared_ptr<::arrow::Schema> &schema);

  tl::expected<std::shared_ptr<TupleSet>, std::string> read(const std::vector<std::string> &columnNames) override;

  tl::expected<std::shared_ptr<TupleSet>, std::string>
  read(const std::vector<std::string> &columnNames, int64_t startPos, int64_t finishPos) override;

};

}

#endif //FPDB_FPDB_TUPLE_INCLUDE_FPDB_TUPLE_PARQUETREADER_H
