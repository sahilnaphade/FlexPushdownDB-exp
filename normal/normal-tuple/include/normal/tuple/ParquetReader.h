//
// Created by matt on 12/8/20.
//

#ifndef NORMAL_NORMAL_TUPLE_INCLUDE_NORMAL_TUPLE_PARQUETREADER_H
#define NORMAL_NORMAL_TUPLE_INCLUDE_NORMAL_TUPLE_PARQUETREADER_H

#include <string>
#include <utility>
#include <memory>
#include <arrow/io/api.h>

#include <normal/tuple/TupleSet2.h>
#include <parquet/arrow/reader.h>
#include <filesystem>

namespace normal::tuple {

class ParquetReader {

public:
  explicit ParquetReader(std::string Path);
  virtual ~ParquetReader();
  static tl::expected<std::shared_ptr<ParquetReader>, std::string> make(const std::string &Path);

  tl::expected<void, std::string> close();
  [[nodiscard]] tl::expected<std::shared_ptr<TupleSet2>, std::string> readRange(int startPos, int finishPos);

private:
  std::string path_;
  std::shared_ptr<arrow::io::ReadableFile> inputStream_;
  std::unique_ptr<::parquet::arrow::FileReader> arrowReader_;
  std::shared_ptr<parquet::FileMetaData> metadata_;

  [[nodiscard]] tl::expected<void, std::string> open();

};

}

#endif //NORMAL_NORMAL_TUPLE_INCLUDE_NORMAL_TUPLE_PARQUETREADER_H
