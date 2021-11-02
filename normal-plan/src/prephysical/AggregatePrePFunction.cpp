//
// Created by matt on 2/4/20.
//

#include <normal/plan/prephysical/AggregatePrePFunction.h>

namespace normal::plan::prephysical {

AggregatePrePFunction::AggregatePrePFunction(AggregatePrePFunctionType type,
                                             const std::shared_ptr<expression::gandiva::Expression> &expression) :
  type_(type),
  expression_(expression) {}

}