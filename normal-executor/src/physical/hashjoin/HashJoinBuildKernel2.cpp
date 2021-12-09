//
// Created by matt on 1/8/20.
//

#include <normal/executor/physical/hashjoin/HashJoinBuildKernel2.h>
#include <normal/tuple/ColumnName.h>
#include <utility>

using namespace normal::executor::physical::hashjoin;

HashJoinBuildKernel2::HashJoinBuildKernel2(vector<string> columnNames) :
	columnNames_(move(columnNames)) {}

HashJoinBuildKernel2 HashJoinBuildKernel2::make(const vector<string> &columnNames) {

  assert(!columnNames.empty());

  auto canonicalColumnNames = ColumnName::canonicalize(columnNames);
  return HashJoinBuildKernel2(canonicalColumnNames);
}

tl::expected<void, string> HashJoinBuildKernel2::put(const shared_ptr<TupleSet> &tupleSet) {

  assert(tupleSet);

  if(!tupleSetIndex_.has_value()){
    auto expectedTupleSetIndex = TupleSetIndex::make(columnNames_, tupleSet->table());
    if(!expectedTupleSetIndex.has_value()){
      return tl::make_unexpected(expectedTupleSetIndex.error());
    }
    tupleSetIndex_ = expectedTupleSetIndex.value();
    return {};
  }

  auto result = tupleSetIndex_.value()->put(tupleSet->table());
  return result;
}

size_t HashJoinBuildKernel2::size() {
  if(!tupleSetIndex_.has_value()){
    return 0;
  }
  return tupleSetIndex_.value()->size();
}

void HashJoinBuildKernel2::clear() {
  tupleSetIndex_ = nullopt;
}

optional<shared_ptr<TupleSetIndex>> HashJoinBuildKernel2::getTupleSetIndex() {
  return tupleSetIndex_;
}
