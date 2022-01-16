//
// Created by matt on 12/8/20.
//

#ifndef NORMAL_NORMAL_TUPLE_INCLUDE_NORMAL_TUPLE_PARQUETREADER_H
#define NORMAL_NORMAL_TUPLE_INCLUDE_NORMAL_TUPLE_PARQUETREADER_H

#include <string>
#include <memory>

#include <tl/expected.hpp>
#include <arrow/io/api.h>
#include <parquet/arrow/reader.h>

#include <normal/tuple/FileReader.h>
#include <normal/tuple/TupleSet.h>

namespace normal::tuple {

class ParquetReader : public FileReader {

public:
  explicit ParquetReader(std::string Path);
  ParquetReader() = default;
  ParquetReader(const ParquetReader&) = default;
  ParquetReader& operator=(const ParquetReader&) = default;
  ~ParquetReader() override;

  static tl::expected<std::shared_ptr<ParquetReader>, std::string> make(const std::string &Path);

  tl::expected<void, std::string> close();
  [[nodiscard]] tl::expected<std::shared_ptr<TupleSet>, std::string>
  read(const std::vector<std::string> &columnNames, unsigned long startPos, unsigned long finishPos) override;

private:
  std::string path_;
  std::shared_ptr<arrow::io::ReadableFile> inputStream_;
  std::unique_ptr<::parquet::arrow::FileReader> arrowReader_;
  std::shared_ptr<parquet::FileMetaData> metadata_;

  [[nodiscard]] tl::expected<void, std::string> open();

// caf inspect
public:
  template <class Inspector>
  friend bool inspect(Inspector& f, ParquetReader& reader) {
    return f.object(reader).fields(f.field("type", reader.type_),
                                   f.field("path", reader.path_));
  }
};

}

#endif //NORMAL_NORMAL_TUPLE_INCLUDE_NORMAL_TUPLE_PARQUETREADER_H
