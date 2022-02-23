//
// Created by matt on 12/8/20.
//

#ifndef FPDB_FPDB_TUPLE_INCLUDE_FPDB_TUPLE_CSVREADER_H
#define FPDB_FPDB_TUPLE_INCLUDE_FPDB_TUPLE_CSVREADER_H

#include "fpdb/tuple/FileReader.h"
#include "fpdb/tuple/TupleSet.h"
#include <tl/expected.hpp>
#include <arrow/io/api.h>
#include <parquet/arrow/reader.h>
#include <string>
#include <memory>

namespace fpdb::tuple::csv {

class CSVReader : public FileReader {

public:
  explicit CSVReader(const std::string &path,
                     const std::shared_ptr<FileFormat> &format,
                     const std::shared_ptr<::arrow::Schema> &schema);
  ~CSVReader() = default;

  static std::shared_ptr<CSVReader> make(const std::string &path,
                                         const std::shared_ptr<FileFormat> &format,
                                         const std::shared_ptr<::arrow::Schema> &schema);

  tl::expected<std::shared_ptr<TupleSet>, std::string> read(const std::vector<std::string> &columnNames) override;

  // FIXME: this one parses all columns as string
  tl::expected<std::shared_ptr<TupleSet>, std::string>
  read(const std::vector<std::string> &columnNames, int64_t startPos, int64_t finishPos) override;

private:
#ifdef __AVX2__
  tl::expected<std::shared_ptr<TupleSet>, std::string> readUsingSimdParser(const std::vector<std::string> &columnNames);
#endif

  tl::expected<std::shared_ptr<TupleSet>, std::string> readUsingArrowImpl(const std::vector<std::string> &columnNames);

};

}

#endif //FPDB_FPDB_TUPLE_INCLUDE_FPDB_TUPLE_CSVREADER_H
