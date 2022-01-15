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

public:
  explicit TupleMessage(std::shared_ptr<TupleSet> tuples, std::string sender);
  TupleMessage() = default;
  TupleMessage(const TupleMessage&) = default;
  TupleMessage& operator=(const TupleMessage&) = default;
  ~TupleMessage() override = default;

  const std::shared_ptr<TupleSet>& tuples() const;

private:
  std::shared_ptr<TupleSet> tuples_;

// caf inspect
public:
  template <class Inspector>
  friend bool inspect(Inspector& f, TupleMessage& msg) {
    return f.object(msg).fields(f.field("type", msg.type_),
                                f.field("sender", msg.sender_),
                                f.field("tuples", msg.tuples_));
  }
};

}

#endif //NORMAL_NORMAL_EXECUTOR_INCLUDE_NORMAL_EXECUTOR_MESSAGE_TUPLEMESSAGE_H
