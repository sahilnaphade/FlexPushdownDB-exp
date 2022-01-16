//
// Created by matt on 12/8/20.
//

#ifndef NORMAL_NORMAL_TUPLE_INCLUDE_NORMAL_TUPLE_CSVREADER_H
#define NORMAL_NORMAL_TUPLE_INCLUDE_NORMAL_TUPLE_CSVREADER_H

#include <string>
#include <memory>

#include <tl/expected.hpp>
#include <arrow/io/api.h>
#include <parquet/arrow/reader.h>

#include "normal/tuple/FileReader.h"
#include "normal/tuple/TupleSet.h"

namespace normal::tuple {

class CSVReader : public FileReader {

public:
  explicit CSVReader(std::string path);
  CSVReader() = default;
  CSVReader(const CSVReader&) = default;
  CSVReader& operator=(const CSVReader&) = default;

  static tl::expected<std::shared_ptr<CSVReader>, std::string> make(const std::string &path);

  [[nodiscard]] tl::expected<std::shared_ptr<TupleSet>, std::string>
  read(const std::vector<std::string> &columnNames, unsigned long startPos, unsigned long finishPos) override;

private:
  std::string path_;

// caf inspect
public:
  template <class Inspector>
  friend bool inspect(Inspector& f, CSVReader& reader) {
    return f.object(reader).fields(f.field("type", reader.type_),
                                   f.field("path", reader.path_));
  }
};

}

#endif //NORMAL_NORMAL_TUPLE_INCLUDE_NORMAL_TUPLE_CSVREADER_H
