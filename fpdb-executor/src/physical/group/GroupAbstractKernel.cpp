//
// Created by Yifei Yang on 4/20/22.
//

#include <fpdb/executor/physical/group/GroupAbstractKernel.h>

namespace fpdb::executor::physical::group {

GroupAbstractKernel::GroupAbstractKernel(GroupKernelType type,
                                         const std::vector<std::string> &groupColumnNames,
                                         const std::vector<std::shared_ptr<AggregateFunction>> &aggregateFunctions):
  type_(type),
  groupColumnNames_(ColumnName::canonicalize(groupColumnNames)),
  aggregateFunctions_(aggregateFunctions) {}

GroupKernelType GroupAbstractKernel::getType() const {
  return type_;
}

}
