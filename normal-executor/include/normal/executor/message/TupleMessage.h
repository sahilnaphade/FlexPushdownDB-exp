//
// Created by matt on 11/12/19.
//

#ifndef NORMAL_NORMAL_EXECUTOR_INCLUDE_NORMAL_EXECUTOR_MESSAGE_TUPLEMESSAGE_H
#define NORMAL_NORMAL_EXECUTOR_INCLUDE_NORMAL_EXECUTOR_MESSAGE_TUPLEMESSAGE_H

#include <normal/executor/message/Message.h>
#include <normal/tuple/TupleSet.h>
#include <memory>

using namespace normal::tuple;

namespace normal::executor::message {

/**
 * Message containing a list of tuples
 */
class TupleMessage : public Message {

private:
  std::shared_ptr<TupleSet> tuples_;

public:
  explicit TupleMessage(std::shared_ptr<TupleSet> tuples, std::string sender);
  ~TupleMessage() override = default;

  const std::shared_ptr<TupleSet>& tuples() const;

};

}

#endif //NORMAL_NORMAL_EXECUTOR_INCLUDE_NORMAL_EXECUTOR_MESSAGE_TUPLEMESSAGE_H
