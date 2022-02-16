//
// Created by matt on 12/8/20.
//

#include <fpdb/tuple/csv/CSVReader.h>
#include <fpdb/tuple/csv/CSVParser.h>
#include <fpdb/tuple/csv/CSVFormat.h>
#include <filesystem>
#include <iostream>

#ifdef __AVX2__
#include <fpdb/tuple/arrow/CSVToArrowSIMDStreamParser.h>
#endif

using namespace fpdb::tuple;

namespace fpdb::tuple::csv {

CSVReader::CSVReader(const std::string &path,
                     const std::shared_ptr<FileFormat> &format,
                     const std::shared_ptr<::arrow::Schema> &schema) :
  FileReader(path, format, schema) {}

std::shared_ptr<CSVReader> CSVReader::make(const std::string &path,
                                           const std::shared_ptr<FileFormat> &format,
                                           const std::shared_ptr<::arrow::Schema> &schema) {
  auto absolutePath = std::filesystem::absolute(path);
  return std::make_shared<CSVReader>(absolutePath, format, schema);
}

tl::expected<std::shared_ptr<TupleSet>, std::string> CSVReader::read(const std::vector<std::string> &columnNames) {

#ifdef __AVX2__
  return readUsingSimdParser(columnNames);
#else
  return readUsingArrowImpl(columnNames);
#endif

}

tl::expected<std::shared_ptr<TupleSet>, std::string>
CSVReader::read(const std::vector<std::string> &columnNames, int64_t startPos, int64_t finishPos) {
  CSVParser parser(path_, columnNames, startPos, finishPos);
  return parser.parse();
}

tl::expected<std::shared_ptr<TupleSet>, std::string>
CSVReader::readUsingSimdParser(const std::vector<std::string> &columnNames) {
  std::shared_ptr<TupleSet> tupleSet;
  auto csvFormat = std::static_pointer_cast<CSVFormat>(format_);
  std::ifstream is(path_, std::ifstream::binary);

  // make output schema
  ::arrow::FieldVector fields;
  for (const auto &columnName: columnNames) {
    auto field = schema_->GetFieldByName(columnName);
    if (!field) {
      is.close();
      return tl::make_unexpected(fmt::format("Read CSV Error: column {} not found", columnName));
    }
    fields.emplace_back(field);
  }
  auto outputSchema = ::arrow::schema(fields);

  // read and parse
  auto simdParser = CSVToArrowSIMDStreamParser(path_,
                                               CSVToArrowSIMDStreamParser::DefaultParseChunkSize,
                                               is,
                                               true,
                                               schema_,
                                               outputSchema,
                                               false,
                                               csvFormat->getFieldDelimiter());
  try {
    tupleSet = simdParser.constructTupleSet();
  } catch (const std::runtime_error &err) {
    is.close();
    return tl::make_unexpected(err.what());
  }

  is.close();
  return tupleSet;
}

tl::expected<std::shared_ptr<TupleSet>, std::string>
CSVReader::readUsingArrowImpl(const std::vector<std::string> &columnNames) {
  std::shared_ptr<TupleSet> tupleSet;
  auto csvFormat = std::static_pointer_cast<CSVFormat>(format_);

  // open the file
  auto expInFile = ::arrow::io::ReadableFile::Open(path_);
  if (!expInFile.ok()) {
    return tl::make_unexpected(expInFile.status().message());
  }
  auto inFile = *expInFile;

  // set options
  std::unordered_map<std::string, std::shared_ptr<::arrow::DataType>> columnTypes;
  for (const auto &field: schema_->fields()) {
    columnTypes.emplace(field->name(), field->type());
  }
  auto ioContext = arrow::io::IOContext();
  auto parse_options = arrow::csv::ParseOptions::Defaults();
  auto read_options = arrow::csv::ReadOptions::Defaults();
  auto convert_options = arrow::csv::ConvertOptions::Defaults();
  read_options.use_threads = false;
  read_options.skip_rows = 1; // Skip the header
  read_options.column_names = schema_->field_names();
  parse_options.delimiter = csvFormat->getFieldDelimiter();
  convert_options.column_types = columnTypes;
  convert_options.include_columns = columnNames;

  // read the file
  auto expTableReader = arrow::csv::TableReader::Make(ioContext,
                                                      inFile,
                                                      read_options,
                                                      parse_options,
                                                      convert_options);
  if (!expTableReader.ok()) {
    close(inFile);
    return tl::make_unexpected(expTableReader.status().message());
  }
  auto expTupleSet = TupleSet::make(*expTableReader);
  if (!expTupleSet.has_value()) {
    close(inFile);
    return expTupleSet;
  }

  return *expTupleSet;
}

}
