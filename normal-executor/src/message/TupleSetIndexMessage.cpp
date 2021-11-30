//
// Created by matt on 4/8/20.
//

#include <normal/executor/message/TupleSetIndexMessage.h>
#include <utility>

using namespace normal::executor::message;

TupleSetIndexMessage::TupleSetIndexMessage(std::shared_ptr<TupleSetIndex> tupleSetIndex, const std::string &sender) :
	Message("TupleSetIndexMessage", sender),
	tupleSetIndex_(std::move(tupleSetIndex)) {
}

const std::shared_ptr<TupleSetIndex> &TupleSetIndexMessage::getTupleSetIndex() const {
  return tupleSetIndex_;
}