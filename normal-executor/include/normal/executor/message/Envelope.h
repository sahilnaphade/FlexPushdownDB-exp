//
// Created by matt on 4/3/20.
//

#ifndef NORMAL_NORMAL_EXECUTOR_INCLUDE_NORMAL_EXECUTOR_MESSAGE_ENVELOPE_H
#define NORMAL_NORMAL_EXECUTOR_INCLUDE_NORMAL_EXECUTOR_MESSAGE_ENVELOPE_H

#include <normal/executor/message/Message.h>
#include <normal/caf/CAFUtil.h>
#include <memory>

namespace normal::executor::message {
/**
 * Class encapsulating a message sent between actors
 *
 * CAF seems to want its messages to be declared as temporaries which it then copies. To make sure CAF doesn't copy
 * any big messages the actual message is kept only as a pointer in this Envelope class.
 *
 * FIXME: Is Envelope the best name? Does it imply this class does more than it actually does? This will need to be
 * reworked when moving to tuped actors so probably not super important
 */
class Envelope {

public:
  explicit Envelope(std::shared_ptr<Message> message);
  Envelope() = default;
  Envelope(const Envelope&) = default;
  Envelope& operator=(const Envelope&) = default;

  const Message &message() const;

private:
  std::shared_ptr<Message> message_;

};

}

CAF_BEGIN_TYPE_ID_BLOCK(Envelope, normal::caf::CAFUtil::Envelope_first_custom_type_id)
CAF_ADD_TYPE_ID(Envelope, (normal::executor::message::Envelope))
CAF_END_TYPE_ID_BLOCK(Envelope)

CAF_ALLOW_UNSAFE_MESSAGE_TYPE(normal::executor::message::Envelope)

#endif //NORMAL_NORMAL_EXECUTOR_INCLUDE_NORMAL_EXECUTOR_MESSAGE_ENVELOPE_H
