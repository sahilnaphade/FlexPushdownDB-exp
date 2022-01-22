//
// Created by Yifei Yang on 3/26/21.
//

#ifndef NORMAL_NORMAL_EXECUTOR_INCLUDE_NORMAL_EXECUTOR_PHYSICAL_SHUFFLE_RECORDBATCHSHUFFLER2_H
#define NORMAL_NORMAL_EXECUTOR_INCLUDE_NORMAL_EXECUTOR_PHYSICAL_SHUFFLE_RECORDBATCHSHUFFLER2_H

#include <normal/tuple/TupleSet.h>
#include <normal/tuple/ArrayAppender.h>
#include <normal/tuple/ArrayHasher.h>
#include <arrow/api.h>
#include <tl/expected.hpp>

using namespace normal::tuple;
using namespace std;

namespace normal::executor::physical::shuffle {

/**
* Class to shuffle a record batch into N tuple sets.
*/
class RecordBatchShuffler2 {

public:
  RecordBatchShuffler2(vector<int> shuffleColumnIndices,
                       size_t numSlots,
                       shared_ptr<::arrow::Schema> schema,
                       vector<vector<shared_ptr<ArrayAppender>>> shuffledAppendersVector);

  static tl::expected<shared_ptr<RecordBatchShuffler2>, string> make(const vector<string> &columnNames,
                                                                     size_t numSlots,
                                                                     const shared_ptr<::arrow::Schema> &schema,
                                                                     size_t numRows);

  tl::expected<void, string> shuffle(const shared_ptr<::arrow::RecordBatch> &recordBatch);

  tl::expected<vector<shared_ptr<TupleSet>>, string> toTupleSets();

private:
  size_t hash(const vector<shared_ptr<ArrayHasher>> &hashers, int64_t row);

  vector<int> shuffleColumnIndices_;
  size_t numSlots_;
  shared_ptr<::arrow::Schema> schema_;
  vector<vector<shared_ptr<ArrayAppender>>> shuffledAppendersVector_;
};

}


#endif //NORMAL_NORMAL_EXECUTOR_INCLUDE_NORMAL_EXECUTOR_PHYSICAL_SHUFFLE_RECORDBATCHSHUFFLER2_H
