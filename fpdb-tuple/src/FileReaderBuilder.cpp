//
// Created by matt on 12/8/20.
//

#include <fpdb/tuple/FileReaderBuilder.h>
#include <fpdb/tuple/csv/CSVReader.h>
#include <fpdb/tuple/parquet/ParquetReader.h>

using namespace fpdb::tuple;

std::shared_ptr<FileReader> FileReaderBuilder::make(const std::string &path,
                                                    const std::shared_ptr<FileFormat> &format,
                                                    const std::shared_ptr<::arrow::Schema> &schema) {
  switch (format->getType()) {
    case FileFormatType::CSV:
      return csv::CSVReader::make(path, format, schema);
    case FileFormatType::PARQUET:
      return parquet::ParquetReader::make(path, format, schema);
  }
}
