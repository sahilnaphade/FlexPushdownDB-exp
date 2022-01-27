//
// Created by Yifei Yang on 1/25/22.
//

#ifndef NORMAL_NORMAL_EXECUTOR_INCLUDE_NORMAL_EXECUTOR_PHYSICAL_AGGREGATE_FUNCTION_AVGREDUCE_H
#define NORMAL_NORMAL_EXECUTOR_INCLUDE_NORMAL_EXECUTOR_PHYSICAL_AGGREGATE_FUNCTION_AVGREDUCE_H

#include <normal/executor/physical/aggregate/function/AvgBase.h>

namespace normal::executor::physical::aggregate {

/**
 * Avg reduce function, the difference from Avg is that sum and count values are already computed from producers
 */
class AvgReduce : public AvgBase {

public:
  AvgReduce(const string &outputColumnName,
            const shared_ptr<normal::expression::gandiva::Expression> &expression);
  AvgReduce() = default;
  AvgReduce(const AvgReduce&) = default;
  AvgReduce& operator=(const AvgReduce&) = default;

  set<string> involvedColumnNames() const override;

  tl::expected<shared_ptr<AggregateResult>, string> compute(const shared_ptr<TupleSet> &tupleSet) override;

// caf inspect
public:
  template <class Inspector>
  friend bool inspect(Inspector& f, AvgReduce& func) {
    return f.object(func).fields(f.field("type", func.type_),
                                 f.field("outputColumnName", func.outputColumnName_),
                                 f.field("expression", func.expression_));
  }
};

}


#endif //NORMAL_NORMAL_EXECUTOR_INCLUDE_NORMAL_EXECUTOR_PHYSICAL_AGGREGATE_FUNCTION_AVGREDUCE_H
