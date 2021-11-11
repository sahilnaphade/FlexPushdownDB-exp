//
// Created by matt on 15/4/20.
//

#include <normal/catalogue/Partition.h>

namespace normal::catalogue {
Partition::Partition() {}

const long &Partition::getNumBytes() const {
  return numBytes_;
}

void Partition::setNumBytes(long numBytes) {
  numBytes_ = numBytes;
}

void Partition::addMinMax(const string &columnName,
                          const pair<shared_ptr<Expression>, shared_ptr<Expression>> &minMax) {
  zoneMap_.emplace(columnName, minMax);
}

}
