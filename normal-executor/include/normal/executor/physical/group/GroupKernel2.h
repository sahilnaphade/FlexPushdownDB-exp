//
// Created by matt on 20/10/20.
//

#ifndef NORMAL_NORMAL_EXECUTOR_INCLUDE_NORMAL_EXECUTOR_PHYSICAL_GROUP_GROUPKERNEL2_H
#define NORMAL_NORMAL_EXECUTOR_INCLUDE_NORMAL_EXECUTOR_PHYSICAL_GROUP_GROUPKERNEL2_H

#include <normal/executor/physical/group/GroupKey2.h>
#include <normal/executor/physical/aggregate/AggregationFunction.h>
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

using ArrayAppenderVector = std::vector<std::shared_ptr<ArrayAppender>>;

using GroupArrayAppenderVectorMap = std::unordered_map<std::shared_ptr<GroupKey2>,
													   ArrayAppenderVector,
													   GroupKey2PointerHash,
													   GroupKey2PointerPredicate>;

using GroupArrayVectorMap = std::unordered_map<std::shared_ptr<GroupKey2>,
											   ::arrow::ArrayVector,
											   GroupKey2PointerHash,
											   GroupKey2PointerPredicate>;

using GroupAggregationResultVectorMap = std::unordered_map<std::shared_ptr<GroupKey2>,
														   std::vector<std::shared_ptr<AggregationResult>>,
														   GroupKey2PointerHash,
														   GroupKey2PointerPredicate>;

class GroupKernel2 {

public:
  GroupKernel2(const std::vector<std::string>& groupColumnNames,
         const std::vector<std::string>& aggregateColumnNames,
			   std::vector<std::shared_ptr<AggregationFunction>> aggregateFunctions);

  /**
   * Groups the input tuple set and computes intermediate aggregates
   *
   * @param tupleSet
   * @return
   */
  [[nodiscard]] tl::expected<void, std::string> group(TupleSet &tupleSet);

  /**
   * Computes final aggregates and generates output tuple set
   *
   * @return
   */
  [[nodiscard]] tl::expected<std::shared_ptr<TupleSet>, std::string> finalise();

  bool hasInput();

private:
  std::vector<std::string> groupColumnNames_;
  std::vector<std::string> aggregateColumnNames_;
  std::vector<std::shared_ptr<AggregationFunction>> aggregateFunctions_;

  std::vector<int> groupColumnIndices_;
  std::vector<int> aggregateColumnIndices_;
  std::optional<std::shared_ptr<arrow::Schema>> inputSchema_;
  std::optional<std::shared_ptr<arrow::Schema>> outputSchema_;
  std::optional<std::shared_ptr<arrow::Schema>> aggregateSchema_;

  GroupArrayAppenderVectorMap groupArrayAppenderVectorMap_;
  GroupArrayVectorMap groupArrayVectorMap_;
  GroupAggregationResultVectorMap groupAggregationResultVectorMap_;

  // FIXME: this is a workaround because we cannot append a single scalar to appender directly
  GroupArrayVectorMap groupKeyBuffer_;

  /**
   * Caches input schema, indices to group columns, and makes output schema
   *
   * @param tupleSet
   * @return
   */
  [[nodiscard]] tl::expected<void, std::string> cache(const TupleSet &tupleSet);

  /**
   * Builds the output schema
   *
   * @return
   */
  tl::expected<std::shared_ptr<arrow::Schema>, std::string> makeOutputSchema();

  /**
   * Groups a single record batch
   *
   * @param recordBatch
   * @return
   */
  [[nodiscard]] tl::expected<GroupArrayVectorMap, std::string>
  groupRecordBatch(const ::arrow::RecordBatch &recordBatch);

  /**
   * Groups and computes intermediate aggregates for a single table
   *
   * @param recordBatch
   * @return
   */
  [[nodiscard]] tl::expected<void, std::string> groupTable(const ::arrow::Table &table);

  /**
   * Computes intermediate aggregates for a table
   *
   * @param table
   */
  void computeGroupAggregates();
};

}

#endif //NORMAL_NORMAL_EXECUTOR_INCLUDE_NORMAL_EXECUTOR_PHYSICAL_GROUP_GROUPKERNEL2_H
