//
// Created by matt on 15/4/20.
//

#include <normal/catalogue/Partition.h>

namespace normal::catalogue {
Partition::Partition(long numBytes, 
                     const shared_ptr<unordered_map<string, pair<Expression, Expression>>> &zoneMap):
  numBytes_(numBytes),
  zoneMap_(zoneMap) {}

const long &Partition::getNumBytes() const {
  return numBytes_;
}

}
