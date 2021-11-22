//
// Created by Yifei Yang on 11/21/21.
//

#ifndef NORMAL_NORMAL_EXECUTOR_INCLUDE_NORMAL_EXECUTOR_PHYSICAL_PRUNE_PARTITIONPRUNER_H
#define NORMAL_NORMAL_EXECUTOR_INCLUDE_NORMAL_EXECUTOR_PHYSICAL_PRUNE_PARTITIONPRUNER_H

#include <normal/catalogue/Partition.h>
#include <normal/expression/gandiva/Expression.h>

using namespace normal::catalogue;
using namespace normal::expression::gandiva;

namespace normal::executor::physical {

class PartitionPruner {

public:
  static unordered_map<shared_ptr<Partition>, shared_ptr<Expression>, PartitionPointerHash, PartitionPointerPredicate>
  prune(const vector<shared_ptr<Partition>> &partitions, const shared_ptr<Expression> &predicate);

};

}

#endif //NORMAL_NORMAL_EXECUTOR_INCLUDE_NORMAL_EXECUTOR_PHYSICAL_PRUNE_PARTITIONPRUNER_H
