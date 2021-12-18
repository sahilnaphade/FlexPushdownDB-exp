//
// Created by Yifei Yang on 12/15/21.
//

#ifndef NORMAL_NORMAL_EXECUTOR_INCLUDE_NORMAL_EXECUTOR_PHYSICAL_HASHJOIN_RECORDBATCHHASHSEMIJOINER_H
#define NORMAL_NORMAL_EXECUTOR_INCLUDE_NORMAL_EXECUTOR_PHYSICAL_HASHJOIN_RECORDBATCHHASHSEMIJOINER_H

#include <normal/tuple/TupleSetIndex.h>
#include <normal/tuple/ColumnName.h>
#include <memory>
#include <utility>
#include <unordered_set>

using namespace normal::tuple;

namespace normal::executor::physical::join {

class RecordBatchHashSemiJoiner {

public:
  RecordBatchHashSemiJoiner(const shared_ptr<TupleSetIndex> &buildTupleSetIndex,
                            const vector<string> &probeJoinColumnNames,
                            int64_t rowIndexOffset);

  static shared_ptr<RecordBatchHashSemiJoiner> make(const shared_ptr<TupleSetIndex> &buildTupleSetIndex,
                                                    const vector<string> &probeJoinColumnNames,
                                                    int64_t rowIndexOffset);

  tl::expected<void, string> join(const shared_ptr<::arrow::RecordBatch> &recordBatch);

  const unordered_set<int64_t> &getRowMatchIndexes() const;

private:
  shared_ptr<TupleSetIndex> buildTupleSetIndex_;
  vector<string> probeJoinColumnNames_;
  int64_t rowIndexOffset_;
  unordered_set<int64_t> rowMatchIndexes_;
  
};

}


#endif //NORMAL_NORMAL_EXECUTOR_INCLUDE_NORMAL_EXECUTOR_PHYSICAL_HASHJOIN_RECORDBATCHHASHSEMIJOINER_H
