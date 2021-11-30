//
// Created by Yifei Yang on 11/11/21.
//

#include <normal/catalogue/format/CSVFormat.h>

namespace normal::catalogue::format {

CSVFormat::CSVFormat(char fieldDelimiter) :
  Format(CSV),
  fieldDelimiter_(fieldDelimiter) {}

char CSVFormat::getFieldDelimiter() const {
  return fieldDelimiter_;
}

}
