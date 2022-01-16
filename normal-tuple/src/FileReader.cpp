//
// Created by matt on 12/8/20.
//

#include <normal/tuple/FileReader.h>

namespace normal::tuple {

FileReader::FileReader(FileType type):
  type_(type) {}

FileType FileReader::getType() const {
  return type_;
}

}
