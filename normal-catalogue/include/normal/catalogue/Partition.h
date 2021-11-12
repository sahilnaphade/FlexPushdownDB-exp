//
// Created by matt on 15/4/20.
//

#ifndef NORMAL_NORMAL_CATALOGUE_INCLUDE_NORMAL_CATALOGUE_PARTITION_H
#define NORMAL_NORMAL_CATALOGUE_INCLUDE_NORMAL_CATALOGUE_PARTITION_H

#include <normal/tuple/Scalar.h>

#include <memory>
#include <string>
#include <optional>
#include <unordered_map>

using namespace normal::tuple;
using namespace std;

/**
 * Base class for partition meta data
 */
namespace normal::catalogue {

class Partition {

public:
  Partition();

  virtual ~Partition() = default;

  virtual string toString() = 0;

  virtual bool equalTo(shared_ptr<Partition> other) = 0;

  virtual size_t hash() = 0;

  [[nodiscard]] const long &getNumBytes() const;

  void setNumBytes(long numBytes);
  void addMinMax(const string &columnName,
                 const pair<shared_ptr<Scalar>, shared_ptr<Scalar>> &minMax);

private:
  long numBytes_ = 0;
  unordered_map<string, pair<shared_ptr<Scalar>, shared_ptr<Scalar>>> zoneMap_;   // <columnName, <min, max>>

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
