//
// Created by Yifei Yang on 11/8/21.
//

#ifndef NORMAL_NORMAL_PLAN_INCLUDE_NORMAL_PLAN_PREPHYSICAL_FILTERABLESCANPREPOP_H
#define NORMAL_NORMAL_PLAN_INCLUDE_NORMAL_PLAN_PREPHYSICAL_FILTERABLESCANPREPOP_H

#include <normal/plan/prephysical/PrePhysicalOp.h>
#include <normal/expression/gandiva/Expression.h>
#include <normal/connector/partition/PartitioningScheme.h>

using namespace normal::expression::gandiva;

namespace normal::plan::prephysical {

class FilterableScanPrePOp: public PrePhysicalOp {
private:
  shared_ptr<Expression> predicate_;
  shared_ptr<PartitioningScheme> partitioningScheme_;
};

}


#endif //NORMAL_NORMAL_PLAN_INCLUDE_NORMAL_PLAN_PREPHYSICAL_FILTERABLESCANPREPOP_H
