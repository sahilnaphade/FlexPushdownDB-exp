//
// Created by Yifei Yang on 2/13/22.
//

#include <fpdb/tuple/FileFormat.h>

namespace fpdb::tuple {

FileFormat::FileFormat(FileFormatType type) : type_(type) {}

FileFormatType FileFormat::getType() const {
  return type_;
}

}
