//
// Created by matt on 7/3/20.
//

#include <normal/executor/physical/aggregate/AggregateResult.h>

using namespace normal::executor::physical::aggregate;

AggregateResult::AggregateResult() = default;

void AggregateResult::put(const string &key, const shared_ptr<arrow::Scalar> &value) {
  this->resultMap_.insert_or_assign(key, value);
}

optional<shared_ptr<arrow::Scalar>> AggregateResult::get(const string &key) {
  auto resIt = this->resultMap_.find(key);
  if (resIt == this->resultMap_.end()) {
    return nullopt;
  } else {
    return resIt->second;
  }
}
