//
// Created by matt on 15/4/20.
//

#ifndef NORMAL_NORMAL_CATALOGUE_INCLUDE_NORMAL_CATALOGUE_PARTITION_H
#define NORMAL_NORMAL_CATALOGUE_INCLUDE_NORMAL_CATALOGUE_PARTITION_H

#include <normal/expression/gandiva/Expression.h>

#include <memory>
#include <string>
#include <optional>
#include <unordered_map>

using namespace normal::expression::gandiva;
using namespace std;

/**
 * Base class for partition meta data
 */
namespace normal::catalogue {

class Partition {

public:
  Partition(long numBytes,
            const shared_ptr<unordered_map<string, pair<Expression, Expression>>> &zoneMap);
  virtual ~Partition() = default;

  virtual string toString() = 0;

  virtual bool equalTo(shared_ptr<Partition> other) = 0;

  virtual size_t hash() = 0;

  [[nodiscard]] const long &getNumBytes() const;

private:
  long numBytes_ = 0;
  shared_ptr<unordered_map<string, pair<Expression, Expression>>> zoneMap_;   // <columnName, <min, max>>

};

struct PartitionPointerHash {
  inline size_t operator()(const shared_ptr<Partition> &partition) const {
    return partition->hash();
  }
};

struct PartitionPointerPredicate {
  inline bool operator()(const shared_ptr<Partition> &lhs, const shared_ptr<Partition> &rhs) const {
    return lhs->equalTo(rhs);
  }
};

}

#endif //NORMAL_NORMAL_CATALOGUE_INCLUDE_NORMAL_CATALOGUE_PARTITION_H
