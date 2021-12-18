//
// Created by matt on 3/8/20.
//

#ifndef NORMAL_NORMAL_EXECUTOR_INCLUDE_NORMAL_EXECUTOR_PHYSICAL_HASHJOIN_RECORDBATCHHASHJOINER_H
#define NORMAL_NORMAL_EXECUTOR_INCLUDE_NORMAL_EXECUTOR_PHYSICAL_HASHJOIN_RECORDBATCHHASHJOINER_H

#include <normal/tuple/TupleSetIndex.h>
#include <normal/tuple/TupleSet.h>
#include <normal/tuple/ArrayAppender.h>
#include <normal/tuple/ColumnName.h>
#include <set>
#include <memory>
#include <utility>

using namespace normal::tuple;

namespace normal::executor::physical::join {

class RecordBatchHashJoiner {
public:
  RecordBatchHashJoiner(std::shared_ptr<TupleSetIndex> buildTupleSetIndex,
					std::vector<std::string> probeJoinColumnNames,
					std::shared_ptr<::arrow::Schema> outputSchema,
          std::vector<std::shared_ptr<std::pair<bool, int>>> neededColumnIndice);

  static tl::expected<std::shared_ptr<RecordBatchHashJoiner>, std::string>
  make(const std::shared_ptr<TupleSetIndex> &buildTupleSetIndex,
	   const std::vector<std::string> &probeJoinColumnNames,
	   const std::shared_ptr<::arrow::Schema> &outputSchema,
	   const std::vector<std::shared_ptr<std::pair<bool, int>>> &neededColumnIndice);

  tl::expected<void, std::string> join(const std::shared_ptr<::arrow::RecordBatch> &recordBatch);

  tl::expected<std::shared_ptr<TupleSet>, std::string> toTupleSet();

private:
  std::shared_ptr<TupleSetIndex> buildTupleSetIndex_;
  std::vector<std::string> probeJoinColumnNames_;
  std::shared_ptr<::arrow::Schema> outputSchema_;
  std::vector<std::shared_ptr<std::pair<bool, int>>> neededColumnIndice_;
  std::vector<::arrow::ArrayVector> joinedArrayVectors_;

};

}

#endif //NORMAL_NORMAL_EXECUTOR_INCLUDE_NORMAL_EXECUTOR_PHYSICAL_HASHJOIN_RECORDBATCHHASHJOINER_H
