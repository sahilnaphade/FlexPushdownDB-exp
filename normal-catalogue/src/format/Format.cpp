//
// Created by Yifei Yang on 11/11/21.
//

#include <normal/catalogue/format/Format.h>

namespace normal::catalogue::format {

Format::Format(FormatType type) : type_(type) {}

FormatType Format::getType() const {
  return type_;
}

}
