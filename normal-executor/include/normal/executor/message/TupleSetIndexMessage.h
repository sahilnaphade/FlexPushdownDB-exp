//
// Created by matt on 4/8/20.
//

#ifndef NORMAL_NORMAL_EXECUTOR_INCLUDE_NORMAL_EXECUTOR_MESSAGE_TUPLESETINDEXMESSAGE_H
#define NORMAL_NORMAL_EXECUTOR_INCLUDE_NORMAL_EXECUTOR_MESSAGE_TUPLESETINDEXMESSAGE_H

#include <normal/executor/message/Message.h>
#include <normal/tuple/TupleSetIndex.h>
#include <memory>

using namespace normal::tuple;

namespace normal::executor::message {

class TupleSetIndexMessage : public Message {

public:
  TupleSetIndexMessage(std::shared_ptr<TupleSetIndex> tupleSetIndex, const std::string &sender);

  [[nodiscard]] const std::shared_ptr<TupleSetIndex> &getTupleSetIndex() const;

private:
  std::shared_ptr<TupleSetIndex> tupleSetIndex_;

};

}

#endif //NORMAL_NORMAL_EXECUTOR_INCLUDE_NORMAL_EXECUTOR_MESSAGE_TUPLESETINDEXMESSAGE_H
