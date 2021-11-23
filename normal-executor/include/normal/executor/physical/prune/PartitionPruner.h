//
// Created by Yifei Yang on 11/21/21.
//

#ifndef NORMAL_NORMAL_EXECUTOR_INCLUDE_NORMAL_EXECUTOR_PHYSICAL_PRUNE_PARTITIONPRUNER_H
#define NORMAL_NORMAL_EXECUTOR_INCLUDE_NORMAL_EXECUTOR_PHYSICAL_PRUNE_PARTITIONPRUNER_H

#include <normal/catalogue/Partition.h>
#include <normal/expression/gandiva/Expression.h>
#include <normal/tuple/Scalar.h>

using namespace normal::catalogue;
using namespace normal::expression::gandiva;
using namespace normal::tuple;

namespace normal::executor::physical {

class PartitionPruner {

public:
  static unordered_map<shared_ptr<Partition>, shared_ptr<Expression>, PartitionPointerHash, PartitionPointerPredicate>
  prune(const vector<shared_ptr<Partition>> &partitions, const shared_ptr<Expression> &predicate);

private:
  /**
   * Prune predicate given min max value on a specific column, the predicate should be canonicalized
   * @param predicate
   * @param columnName
   * @param min
   * @param max
   * @return
   */
  static shared_ptr<Expression> prunePredicate(const shared_ptr<Expression> &predicate, const string &columnName,
                                               const shared_ptr<Scalar> &statsMin, const shared_ptr<Scalar> &statsMax);

  static shared_ptr<Scalar> litToScalar(const shared_ptr<Expression> &literal);

  static bool checkValid(const shared_ptr<Scalar> &predMin, const shared_ptr<Scalar> &predMax,
                         bool predMinOpen, bool predMaxOpen,
                         const shared_ptr<Scalar> &min, const shared_ptr<Scalar> &max);

  static bool lt(const shared_ptr<Scalar> &v1, const shared_ptr<Scalar> &v2);
  static bool lte(const shared_ptr<Scalar> &v1, const shared_ptr<Scalar> &v2);

};

}

#endif //NORMAL_NORMAL_EXECUTOR_INCLUDE_NORMAL_EXECUTOR_PHYSICAL_PRUNE_PARTITIONPRUNER_H
