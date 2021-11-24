//
// Created by Yifei Yang on 11/20/21.
//

#include <normal/executor/physical/sort/SortPOp.h>

namespace normal::executor::physical::sort {

SortPOp::SortPOp(const string &name,
                 const vector<string> &projectColumnNames,
                 long queryId) :
  PhysicalOp(name, "SortPOp", projectColumnNames, queryId) {}

void SortPOp::onReceive(const Envelope &message) {
  if (message.message().type() == "StartMessage") {
    this->onStart();
  } else if (message.message().type() == "TupleMessage") {
    auto tupleMessage = dynamic_cast<const TupleMessage &>(message.message());
    this->onTuple(tupleMessage);
  } else if (message.message().type() == "CompleteMessage") {
    auto completeMessage = dynamic_cast<const CompleteMessage &>(message.message());
    this->onComplete(completeMessage);
  } else {
    // FIXME: Propagate error properly
    throw runtime_error("Unrecognized message type " + message.message().type());
  }
}

void SortPOp::onStart() {
  SPDLOG_DEBUG("Starting operator  |  name: '{}'", this->name());
}

void SortPOp::onComplete(const CompleteMessage &) {
  if(!ctx()->isComplete() && ctx()->operatorMap().allComplete(POpRelationshipType::Producer)){
    ctx()->notifyComplete();
  }
}

void SortPOp::onTuple(const TupleMessage &message) {
  // TODO: sort
  shared_ptr<Message> tupleMessage = make_shared<TupleMessage>(message.tuples(), name());
  ctx()->tell(tupleMessage);
}

}
