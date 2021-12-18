//
// Created by Yifei Yang on 12/17/21.
//

#include <normal/executor/physical/join/OuterJoinHelper.h>

namespace normal::executor::physical::join {

OuterJoinHelper::OuterJoinHelper(bool isLeft,
                                 const shared_ptr<arrow::Schema> &outputSchema,
                                 const vector<shared_ptr<pair<bool, int>>> &neededColumnIndice):
  isLeft_(isLeft),
  outputSchema_(outputSchema),
  neededColumnIndice_(neededColumnIndice) {}

void OuterJoinHelper::putRowMatchIndexes(const unordered_set<int64_t> &newRowMatchIndexes) {
  if (!rowMatchIndexes_.has_value()) {
    rowMatchIndexes_ = newRowMatchIndexes;
  } else {
    rowMatchIndexes_->insert(newRowMatchIndexes.begin(), newRowMatchIndexes.end());
  }
}

tl::expected<shared_ptr<TupleSet>, string> OuterJoinHelper::compute(const shared_ptr<TupleSet> &tupleSet) {
  // keep side
  const auto &expKeepArrayVector = computeKeepSide(tupleSet);
  if (!expKeepArrayVector) {
    return tl::make_unexpected(expKeepArrayVector.error());
  }
  const auto &keepArrayVector = expKeepArrayVector.value();

  // discard side
  const auto &expDiscardArrayVector = computeDiscardSide(tupleSet);
  if (!expDiscardArrayVector) {
    return tl::make_unexpected(expDiscardArrayVector.error());
  }
  const auto &discardArrayVector = expDiscardArrayVector.value();

  // make output
  arrow::ArrayVector outputArrayVector;
  if (isLeft_) {
    outputArrayVector.insert(outputArrayVector.end(), keepArrayVector.begin(), keepArrayVector.end());
    outputArrayVector.insert(outputArrayVector.end(), discardArrayVector.begin(), discardArrayVector.end());
  } else {
    outputArrayVector.insert(outputArrayVector.end(), discardArrayVector.begin(), discardArrayVector.end());
    outputArrayVector.insert(outputArrayVector.end(), keepArrayVector.begin(), keepArrayVector.end());
  }
  return TupleSet::make(outputSchema_, outputArrayVector);
}

tl::expected<arrow::ArrayVector, string> computeKeepSide(const shared_ptr<TupleSet> &tupleSet) {

}

tl::expected<arrow::ArrayVector, string> computeDiscardSide(const shared_ptr<TupleSet> &keepTupleSet) {

}

}
