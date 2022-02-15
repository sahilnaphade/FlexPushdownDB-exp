//
// Created by Yifei Yang on 2/13/22.
//

#include <fpdb/tuple/csv/CSVFormat.h>

namespace fpdb::tuple::csv {

CSVFormat::CSVFormat(char fieldDelimiter) :
  FileFormat(FileFormatType::CSV),
  fieldDelimiter_(fieldDelimiter) {}

char CSVFormat::getFieldDelimiter() const {
  return fieldDelimiter_;
}

}
