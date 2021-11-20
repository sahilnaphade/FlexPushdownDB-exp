//
// Created by matt on 1/8/20.
//

#include <normal/executor/physical/hashjoin/HashJoinBuildKernel2.h>
#include <normal/tuple/TupleSetIndexWrapper.h>
#include <normal/tuple/ColumnName.h>
#include <utility>

using namespace normal::executor::physical::hashjoin;

HashJoinBuildKernel2::HashJoinBuildKernel2(std::string columnName) :
	columnName_(std::move(columnName)) {}

HashJoinBuildKernel2 HashJoinBuildKernel2::make(const std::string &columnName) {

  assert(!columnName.empty());

  auto canonicalColumnName = ColumnName::canonicalize(columnName);
  return HashJoinBuildKernel2(canonicalColumnName);
}

tl::expected<void, std::string> HashJoinBuildKernel2::put(const std::shared_ptr<TupleSet> &tupleSet) {

  assert(tupleSet);

  if(!tupleSetIndex_.has_value()){
	auto expectedTupleSetIndex = TupleSetIndexBuilder::make(tupleSet->table(), columnName_);
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
  tupleSetIndex_ = std::nullopt;
}

std::optional<std::shared_ptr<TupleSetIndex>> HashJoinBuildKernel2::getTupleSetIndex() {
  return tupleSetIndex_;
}
