//
// Created by matt on 20/10/20.
//

#ifndef NORMAL_NORMAL_EXECUTOR_INCLUDE_NORMAL_EXECUTOR_PHYSICAL_GROUP_GROUPKERNEL2_H
#define NORMAL_NORMAL_EXECUTOR_INCLUDE_NORMAL_EXECUTOR_PHYSICAL_GROUP_GROUPKERNEL2_H

#include <normal/executor/physical/aggregate/function/AggregateFunction.h>
#include <normal/executor/physical/aggregate/AggregateResult.h>
#include <normal/tuple/TupleKey.h>
#include <normal/tuple/TupleSet.h>
#include <normal/tuple/ArrayAppender.h>
#include <vector>
#include <string>
#include <memory>
#include <unordered_map>

using namespace normal::executor::physical::aggregate;
using namespace normal::tuple;
using namespace normal::expression;

namespace normal::executor::physical::group {

using ArrayAppenderVector = vector<shared_ptr<ArrayAppender>>;

/**
 * Appender map for one incoming tupleSet
 */
using GroupArrayAppenderVectorMap = unordered_map<shared_ptr<TupleKey>,
													   ArrayAppenderVector,
													   TupleKeyPointerHash,
													   TupleKeyPointerPredicate>;

/**
 * Array vector map for one incoming tupleSet
 */
using GroupArrayVectorMap = unordered_map<shared_ptr<TupleKey>,
											   ::arrow::ArrayVector,
											   TupleKeyPointerHash,
											   TupleKeyPointerPredicate>;

/**
 * Aggregate result map for results accumulated by all incoming tupleSets
 */
using GroupAggregateResultVectorMap = unordered_map<shared_ptr<TupleKey>,
                                    vector<vector<shared_ptr<AggregateResult>>>,
                                    TupleKeyPointerHash,
                                    TupleKeyPointerPredicate>;

class GroupKernel2 {

public:
  GroupKernel2(const vector<string>& groupColumnNames,
               vector<shared_ptr<AggregateFunction>> aggregateFunctions);
  GroupKernel2() = default;
  GroupKernel2(const GroupKernel2&) = default;
  GroupKernel2& operator=(const GroupKernel2&) = default;

  /**
   * Groups the input tuple set and computes intermediate aggregates
   *
   * @param tupleSet
   * @return
   */
  [[nodiscard]] tl::expected<void, string> group(const TupleSet &tupleSet);

  /**
   * Computes final aggregates and generates output tuple set
   *
   * @return
   */
  [[nodiscard]] tl::expected<shared_ptr<TupleSet>, string> finalise();

private:
  vector<string> groupColumnNames_;
  vector<shared_ptr<AggregateFunction>> aggregateFunctions_;

  vector<int> groupColumnIndices_;
  vector<int> aggregateColumnIndices_;
  std::optional<shared_ptr<arrow::Schema>> inputSchema_;
  std::optional<shared_ptr<arrow::Schema>> aggregateInputSchema_;

  GroupArrayAppenderVectorMap groupArrayAppenderVectorMap_;
  GroupArrayVectorMap groupArrayVectorMap_;
  GroupAggregateResultVectorMap groupAggregateResultVectorMap_;

  /**
   * Collect aggregate column names from all aggregate functions
   * @return
   */
  vector<string> getAggregateColumnNames();

  /**
   * Caches input schema, indices to group columns, and makes output schema
   *
   * @param tupleSet
   * @return
   */
  tl::expected<void, string> cache(const TupleSet &tupleSet);

  /**
   * Groups a single record batch
   *
   * @param recordBatch
   * @return
   */
  tl::expected<GroupArrayVectorMap, string> groupRecordBatch(const ::arrow::RecordBatch &recordBatch);

  /**
   * Groups and computes intermediate aggregates for a single table
   *
   * @param recordBatch
   * @return
   */
  tl::expected<void, string> groupTable(const ::arrow::Table &table);

  /**
   * Computes intermediate aggregates for a table
   *
   * @param table
   */
  tl::expected<void, string> computeGroupAggregates();

  /**
   * Computes final aggregates and generates output tuple set, given that there is no aggregate result
   *
   * @return
   */
  tl::expected<shared_ptr<TupleSet>, string> finaliseEmpty();
  bool hasResult();

// caf inspect
public:
  template <class Inspector>
  friend bool inspect(Inspector& f, GroupKernel2& kernel) {
    return f.object(kernel).fields(f.field("groupColumnNames", kernel.groupColumnNames_),
                                   f.field("aggregateFunctions", kernel.aggregateFunctions_));
  }
};

}

#endif //NORMAL_NORMAL_EXECUTOR_INCLUDE_NORMAL_EXECUTOR_PHYSICAL_GROUP_GROUPKERNEL2_H
