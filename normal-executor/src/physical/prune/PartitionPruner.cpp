//
// Created by Yifei Yang on 11/21/21.
//

#include <normal/executor/physical/prune/PartitionPruner.h>

namespace normal::executor::physical {

unordered_map<shared_ptr<Partition>, shared_ptr<Expression>, PartitionPointerHash, PartitionPointerPredicate>
PartitionPruner::prune(const vector<shared_ptr<Partition>> &partitions, const shared_ptr<Expression> &predicate) {
  return unordered_map<shared_ptr<Partition>, shared_ptr<Expression>, PartitionPointerHash, PartitionPointerPredicate>();
}

}
