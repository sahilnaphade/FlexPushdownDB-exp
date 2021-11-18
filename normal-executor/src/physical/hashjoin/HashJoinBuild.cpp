//
// Created by matt on 29/4/20.
//

#include <normal/executor/physical/hashjoin/HashJoinBuild.h>
#include <normal/executor/physical/Globals.h>
#include <normal/executor/message/TupleSetIndexMessage.h>
#include <normal/tuple/TupleSet2.h>
#include <utility>

using namespace normal::executor::physical::hashjoin;
using namespace normal::tuple;

HashJoinBuild::HashJoinBuild(const std::string &name, std::string columnName, long queryId) :
	PhysicalOp(name, "HashJoinBuild", queryId),
	columnName_(std::move(columnName)),
	kernel_(HashJoinBuildKernel2::make(columnName_)){
}

std::shared_ptr<HashJoinBuild> HashJoinBuild::create(const std::string &name, const std::string &columnName) {
  auto canonicalColumnName = ColumnName::canonicalize(columnName);
  return std::make_shared<HashJoinBuild>(name, canonicalColumnName);
}

void HashJoinBuild::onReceive(const Envelope &msg) {
  if (msg.message().type() == "StartMessage") {
	this->onStart();
  } else if (msg.message().type() == "TupleMessage") {
	auto tupleMessage = dynamic_cast<const TupleMessage &>(msg.message());
	this->onTuple(tupleMessage);
  } else if (msg.message().type() == "CompleteMessage") {
	auto completeMessage = dynamic_cast<const CompleteMessage &>(msg.message());
	this->onComplete(completeMessage);
  } else {
	// FIXME: Propagate error properly
	throw std::runtime_error(fmt::format("Unrecognized message type: {}, {}", msg.message().type(), name()));
  }
}

void HashJoinBuild::onStart() {
  SPDLOG_DEBUG("Starting operator  |  name: '{}'", this->name());
}

void HashJoinBuild::onTuple(const TupleMessage &msg) {
  auto tupleSet = TupleSet2::create(msg.tuples());

  SPDLOG_DEBUG("Adding tuple set to hash table  |  operator: '{}', tupleSet:\n{}", this->name(), tupleSet->showString(TupleSetShowOptions(TupleSetShowOrientation::RowOriented, 1000)));

  auto result = buffer(tupleSet);
  if(!result)
    throw std::runtime_error(fmt::format("{}, {}", result.error(), name()));
  send(false);
}

void HashJoinBuild::onComplete(const CompleteMessage &) {
  if(!ctx()->isComplete() && ctx()->operatorMap().allComplete(POpRelationshipType::Producer)) {
    send(true);
    ctx()->notifyComplete();
  }
}

tl::expected<void, std::string> HashJoinBuild::buffer(const std::shared_ptr<TupleSet2> &tupleSet) {
  return kernel_.put(tupleSet);
}

void HashJoinBuild::send(bool force) {
  if (kernel_.getTupleSetIndex().has_value() && (force || kernel_.getTupleSetIndex().value()->size() >= DefaultBufferSize)) {
    std::shared_ptr<Message> message =
            std::make_shared<TupleSetIndexMessage>(kernel_.getTupleSetIndex().value(), name());
    ctx()->tell(message);
    kernel_.clear();
  }
}
