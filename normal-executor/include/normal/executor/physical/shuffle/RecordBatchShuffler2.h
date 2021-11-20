//
// Created by Yifei Yang on 3/26/21.
//

#ifndef NORMAL_NORMAL_EXECUTOR_INCLUDE_NORMAL_EXECUTOR_PHYSICAL_SHUFFLE_RECORDBATCHSHUFFLER2_H
#define NORMAL_NORMAL_EXECUTOR_INCLUDE_NORMAL_EXECUTOR_PHYSICAL_SHUFFLE_RECORDBATCHSHUFFLER2_H

#include <normal/tuple/TupleSet.h>
#include <normal/tuple/ArrayAppender.h>
#include <arrow/api.h>
#include <tl/expected.hpp>

using namespace normal::tuple;

namespace normal::executor::physical::shuffle {

/**
* Class to shuffle a record batch into N tuple sets.
*/
class RecordBatchShuffler2 {

public:
  RecordBatchShuffler2(int shuffleColumnIndex, size_t numSlots, std::shared_ptr<::arrow::Schema> schema, size_t numRows);

  static tl::expected<std::shared_ptr<RecordBatchShuffler2>, std::string>
  make(const std::string &columnName, size_t numSlots, const std::shared_ptr<::arrow::Schema> &schema, size_t numRows);

  [[nodiscard]] tl::expected<void, std::string> shuffle(const std::shared_ptr<::arrow::RecordBatch> &recordBatch);

  tl::expected<std::vector<std::shared_ptr<TupleSet>>, std::string> toTupleSets();

protected:
  int shuffleColumnIndex_;
  size_t numSlots_;
  std::shared_ptr<::arrow::Schema> schema_;
  std::vector<std::vector<std::shared_ptr<ArrayAppender>>> shuffledAppendersVector_;
};

}


#endif //NORMAL_NORMAL_EXECUTOR_INCLUDE_NORMAL_EXECUTOR_PHYSICAL_SHUFFLE_RECORDBATCHSHUFFLER2_H
