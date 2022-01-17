//
// Created by Yifei Yang on 12/17/21.
//

#ifndef NORMAL_NORMAL_EXECUTOR_INCLUDE_NORMAL_EXECUTOR_PHYSICAL_JOIN_OUTERJOINHELPER_H
#define NORMAL_NORMAL_EXECUTOR_INCLUDE_NORMAL_EXECUTOR_PHYSICAL_JOIN_OUTERJOINHELPER_H

#include <normal/tuple/TupleSet.h>
#include <normal/tuple/Column.h>
#include <memory>
#include <unordered_set>

using namespace normal::tuple;
using namespace std;

namespace normal::executor::physical::join {

class OuterJoinHelper {

public:
  OuterJoinHelper(bool isLeft,
                  const shared_ptr<arrow::Schema> &outputSchema,
                  const vector<shared_ptr<pair<bool, int>>> &neededColumnIndice);

  static shared_ptr<OuterJoinHelper> make(bool isLeft,
                                          const shared_ptr<arrow::Schema> &outputSchema,
                                          const vector<shared_ptr<pair<bool, int>>> &neededColumnIndice);

  void putRowMatchIndexes(const unordered_set<int64_t> &newRowMatchIndexes);

  /**
   * Compute outer join result
   * @param tupleSet Left/right tupleSet for left/right join
   * @return
   */
  tl::expected<shared_ptr<TupleSet>, string> compute(const shared_ptr<TupleSet> &tupleSet);

private:
  tl::expected<arrow::ArrayVector, string> computeKeepSide(const shared_ptr<TupleSet> &tupleSet);
  tl::expected<arrow::ArrayVector, string> computeDiscardSide(const shared_ptr<TupleSet> &keepTupleSet);

  bool isLeft_;
  shared_ptr<arrow::Schema> outputSchema_;
  vector<shared_ptr<pair<bool, int>>> neededColumnIndice_;
  std::optional<unordered_set<int64_t>> rowMatchIndexes_;

};

}


#endif //NORMAL_NORMAL_EXECUTOR_INCLUDE_NORMAL_EXECUTOR_PHYSICAL_JOIN_OUTERJOINHELPER_H
