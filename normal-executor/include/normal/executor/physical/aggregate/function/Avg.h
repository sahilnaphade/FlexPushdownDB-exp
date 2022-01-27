//
// Created by Yifei Yang on 1/25/22.
//

#ifndef NORMAL_NORMAL_EXECUTOR_INCLUDE_NORMAL_EXECUTOR_PHYSICAL_AGGREGATE_FUNCTION_AVG_H
#define NORMAL_NORMAL_EXECUTOR_INCLUDE_NORMAL_EXECUTOR_PHYSICAL_AGGREGATE_FUNCTION_AVG_H

#include <normal/executor/physical/aggregate/function/AvgBase.h>

namespace normal::executor::physical::aggregate {
  
class Avg : public AvgBase {
  
public:
  Avg(const string &outputColumnName,
      const shared_ptr<normal::expression::gandiva::Expression> &expression);
  Avg() = default;
  Avg(const Avg&) = default;
  Avg& operator=(const Avg&) = default;

  tl::expected<shared_ptr<AggregateResult>, string> compute(const shared_ptr<TupleSet> &tupleSet) override;

// caf inspect
public:
  template <class Inspector>
  friend bool inspect(Inspector& f, Avg& func) {
    return f.object(func).fields(f.field("type", func.type_),
                                 f.field("outputColumnName", func.outputColumnName_),
                                 f.field("expression", func.expression_));
  }
};

}


#endif //NORMAL_NORMAL_EXECUTOR_INCLUDE_NORMAL_EXECUTOR_PHYSICAL_AGGREGATE_FUNCTION_AVG_H
