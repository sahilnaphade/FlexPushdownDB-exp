//
// Created by matt on 3/8/20.
//

#ifndef NORMAL_NORMAL_EXECUTOR_INCLUDE_NORMAL_EXECUTOR_PHYSICAL_HASHJOIN_RECORDBATCHHASHJOINER_H
#define NORMAL_NORMAL_EXECUTOR_INCLUDE_NORMAL_EXECUTOR_PHYSICAL_HASHJOIN_RECORDBATCHHASHJOINER_H

#include <normal/tuple/TupleSetIndex.h>
#include <normal/tuple/TupleSet.h>
#include <normal/tuple/ArrayAppender.h>
#include <normal/tuple/ColumnName.h>
#include <unordered_set>
#include <memory>
#include <utility>

using namespace normal::tuple;
using namespace std;

namespace normal::executor::physical::join {

class RecordBatchHashJoiner {
public:
  RecordBatchHashJoiner(shared_ptr<TupleSetIndex> buildTupleSetIndex,
                        vector<string> probeJoinColumnNames,
                        shared_ptr<::arrow::Schema> outputSchema,
                        vector<shared_ptr<pair<bool, int>>> neededColumnIndice,
                        int64_t buildRowOffset);

  static tl::expected<shared_ptr<RecordBatchHashJoiner>, string> make(
          const shared_ptr<TupleSetIndex> &buildTupleSetIndex,
          const vector<string> &probeJoinColumnNames,
          const shared_ptr<::arrow::Schema> &outputSchema,
          const vector<shared_ptr<pair<bool, int>>> &neededColumnIndice,
          int64_t buildRowOffset);

  tl::expected<void, string> join(const shared_ptr<::arrow::RecordBatch> &probeRecordBatch,
                                  int64_t probeRowOffset);

  tl::expected<shared_ptr<TupleSet>, string> toTupleSet();

  const unordered_set<int64_t> &getBuildRowMatchIndexes() const;
  const unordered_set<int64_t> &getProbeRowMatchIndexes() const;

private:
  shared_ptr<TupleSetIndex> buildTupleSetIndex_;
  vector<string> probeJoinColumnNames_;
  shared_ptr<::arrow::Schema> outputSchema_;
  vector<shared_ptr<pair<bool, int>>> neededColumnIndice_;

  vector<::arrow::ArrayVector> joinedArrayVectors_;
  int64_t buildRowOffset_;
  unordered_set<int64_t> buildRowMatchIndexes_;
  unordered_set<int64_t> probeRowMatchIndexes_;

};

}

#endif //NORMAL_NORMAL_EXECUTOR_INCLUDE_NORMAL_EXECUTOR_PHYSICAL_HASHJOIN_RECORDBATCHHASHJOINER_H
