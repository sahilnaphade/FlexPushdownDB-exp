//
// Created by matt on 2/4/20.
//

#ifndef NORMAL_NORMAL_SQL_INCLUDE_NORMAL_SQL_LOGICAL_AGGREGATELOGICALFUNCTION_H
#define NORMAL_NORMAL_SQL_INCLUDE_NORMAL_SQL_LOGICAL_AGGREGATELOGICALFUNCTION_H

#include <normal/plan/prephysical/AggregatePrePFunctionType.h>
#include <normal/expression/Expression.h>
#include <normal/expression/gandiva/Expression.h>

#include <string>
#include <memory>

namespace normal::plan::prephysical {

class AggregatePrePFunction {
public:
  AggregatePrePFunction(AggregatePrePFunctionType type,
                        const std::shared_ptr<expression::gandiva::Expression> &expression);
  virtual ~AggregatePrePFunction() = default;

private:
  AggregatePrePFunctionType type_;
  std::shared_ptr<expression::gandiva::Expression> expression_;

};

}

#endif //NORMAL_NORMAL_SQL_INCLUDE_NORMAL_SQL_LOGICAL_AGGREGATELOGICALFUNCTION_H
