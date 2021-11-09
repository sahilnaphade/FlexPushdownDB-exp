//
// Created by matt on 15/4/20.
//

#include <normal/catalogue/Partition.h>

namespace normal::catalogue {

const long &Partition::getNumBytes() const {
  return numBytes;
}

void Partition::setNumBytes(const long &NumBytes) {
  numBytes = NumBytes;
}

}
