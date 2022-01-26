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

using namespace std;

namespace normal::plan::prephysical {

class AggregatePrePFunction {

public:
  // column name for count(*)
  constexpr static const char *const COUNT_STAR_COLUMN = "COUNT_*";
  // column names for sum and count result for parallel avg
  constexpr static const char *const AVG_PARALLEL_SUM_COLUMN_PREFIX = "avg_parallel_sum-";
  constexpr static const char *const AVG_PARALLEL_COUNT_COLUMN_PREFIX = "avg_parallel_count-";

  AggregatePrePFunction(AggregatePrePFunctionType type,
                        const shared_ptr<expression::gandiva::Expression> &expression);
  virtual ~AggregatePrePFunction() = default;

  AggregatePrePFunctionType getType() const;
  const shared_ptr<expression::gandiva::Expression> &getExpression() const;
  string getTypeString() const;
  set<string> involvedColumnNames() const;


private:
  AggregatePrePFunctionType type_;
  shared_ptr<expression::gandiva::Expression> expression_;

};

}

#endif //NORMAL_NORMAL_SQL_INCLUDE_NORMAL_SQL_LOGICAL_AGGREGATELOGICALFUNCTION_H
