//
// Created by Yifei Yang on 12/13/21.
//

#ifndef NORMAL_NORMAL_EXECUTOR_INCLUDE_NORMAL_EXECUTOR_PHYSICAL_NESTEDLOOPJOIN_RECORDBATCHNESTEDLOOPJOINER_H
#define NORMAL_NORMAL_EXECUTOR_INCLUDE_NORMAL_EXECUTOR_PHYSICAL_NESTEDLOOPJOIN_RECORDBATCHNESTEDLOOPJOINER_H

#include <normal/expression/gandiva/Expression.h>
#include <normal/expression/Filter.h>
#include <normal/tuple/ArrayAppender.h>
#include <normal/tuple/TupleSet.h>

using namespace normal::tuple;
using namespace normal::expression::gandiva;
using namespace std;

namespace normal::executor::physical::join {

class RecordBatchNestedLoopJoiner {

public:
  RecordBatchNestedLoopJoiner(const optional<shared_ptr<expression::gandiva::Expression>> &predicate,
                              const shared_ptr<::arrow::Schema> &outputSchema,
                              const vector<shared_ptr<pair<bool, int>>> &neededColumnIndice);
  
  static shared_ptr<RecordBatchNestedLoopJoiner> make(const optional<shared_ptr<expression::gandiva::Expression>> &predicate,
                                                      const shared_ptr<::arrow::Schema> &outputSchema,
                                                      const vector<shared_ptr<pair<bool, int>>> &neededColumnIndice);

  tl::expected<void, string> join(const shared_ptr<::arrow::RecordBatch> &leftRecordBatch,
                                  const shared_ptr<::arrow::RecordBatch> &rightRecordBatch);

  tl::expected<shared_ptr<TupleSet>, string> toTupleSet();

private:
  tl::expected<shared_ptr<arrow::RecordBatch>, string>
  cartesian(const shared_ptr<::arrow::RecordBatch> &leftRecordBatch,
            const shared_ptr<::arrow::RecordBatch> &rightRecordBatch);
  arrow::ArrayVector filter(const shared_ptr<arrow::RecordBatch> &recordBatch);

  optional<shared_ptr<expression::gandiva::Expression>> predicate_;
  optional<std::shared_ptr<normal::expression::Filter>> filter_;

  shared_ptr<::arrow::Schema> outputSchema_;
  shared_ptr<::arrow::Schema> leftOutputSchema_;
  shared_ptr<::arrow::Schema> rightOutputSchema_;
  vector<int> neededLeftColumnIndexes_;
  vector<int> neededRightColumnIndexes_;
  vector<::arrow::ArrayVector> joinedArrayVectors_;
  
};

}


#endif //NORMAL_NORMAL_EXECUTOR_INCLUDE_NORMAL_EXECUTOR_PHYSICAL_NESTEDLOOPJOIN_RECORDBATCHNESTEDLOOPJOINER_H
