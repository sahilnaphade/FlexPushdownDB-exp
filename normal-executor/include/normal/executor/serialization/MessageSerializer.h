//
// Created by Yifei Yang on 1/12/22.
//

#ifndef NORMAL_NORMAL_EXECUTOR_INCLUDE_NORMAL_EXECUTOR_SERIALIZATION_MESSAGESERIALIZER_H
#define NORMAL_NORMAL_EXECUTOR_INCLUDE_NORMAL_EXECUTOR_SERIALIZATION_MESSAGESERIALIZER_H

#include <normal/executor/message/Message.h>
#include <normal/executor/message/StartMessage.h>
#include <normal/executor/message/ConnectMessage.h>
#include <normal/executor/message/CompleteMessage.h>
#include <normal/executor/message/ScanMessage.h>
#include <normal/executor/message/TupleMessage.h>
#include <normal/executor/message/TupleSetIndexMessage.h>
#include <normal/executor/message/cache/LoadRequestMessage.h>
#include <normal/executor/message/cache/LoadResponseMessage.h>
#include <normal/executor/message/cache/StoreRequestMessage.h>
#include <normal/executor/message/cache/WeightRequestMessage.h>
#include <normal/executor/message/cache/CacheMetricsMessage.h>
#include <normal/caf/CAFUtil.h>
#include <normal/tuple/serialization/TupleKeyElementSerializer.h>

using namespace normal::executor::message;

using MessagePtr = std::shared_ptr<Message>;

CAF_BEGIN_TYPE_ID_BLOCK(Message, normal::caf::CAFUtil::Message_first_custom_type_id)
CAF_ADD_TYPE_ID(Message, (MessagePtr))
CAF_ADD_TYPE_ID(Message, (StartMessage))
CAF_ADD_TYPE_ID(Message, (ConnectMessage))
CAF_ADD_TYPE_ID(Message, (CompleteMessage))
CAF_ADD_TYPE_ID(Message, (ScanMessage))
CAF_ADD_TYPE_ID(Message, (TupleMessage))
CAF_ADD_TYPE_ID(Message, (TupleSetIndexMessage))
// For the following cache messages, we have to implement `inspect` for concrete derived shared_ptr type one by one,
// because SegmentCacheActor directly uses the concrete derived types rather than base type Message used by other actors
CAF_ADD_TYPE_ID(Message, (LoadRequestMessage))
CAF_ADD_TYPE_ID(Message, (LoadResponseMessage))
CAF_ADD_TYPE_ID(Message, (StoreRequestMessage))
CAF_ADD_TYPE_ID(Message, (WeightRequestMessage))
CAF_ADD_TYPE_ID(Message, (CacheMetricsMessage))
CAF_ADD_TYPE_ID(Message, (std::shared_ptr<LoadResponseMessage>))
CAF_ADD_TYPE_ID(Message, (std::shared_ptr<LoadRequestMessage>))
CAF_ADD_TYPE_ID(Message, (std::shared_ptr<StoreRequestMessage>))
CAF_ADD_TYPE_ID(Message, (std::shared_ptr<WeightRequestMessage>))
CAF_ADD_TYPE_ID(Message, (std::shared_ptr<CacheMetricsMessage>))
CAF_END_TYPE_ID_BLOCK(Message)

// Variant-based approach on MessagePtr
namespace caf {

template<>
struct variant_inspector_traits<MessagePtr> {
  using value_type = MessagePtr;

  // Lists all allowed types and gives them a 0-based index.
  static constexpr type_id_t allowed_types[] = {
          type_id_v<none_t>,
          type_id_v<StartMessage>,
          type_id_v<ConnectMessage>,
          type_id_v<CompleteMessage>,
          type_id_v<ScanMessage>,
          type_id_v<TupleMessage>,
          type_id_v<TupleSetIndexMessage>
  };

  // Returns which type in allowed_types corresponds to x.
  static auto type_index(const value_type &x) {
    if (!x)
      return 0;
    else if (x->type() == "StartMessage")
      return 1;
    else if (x->type() == "ConnectMessage")
      return 2;
    else if (x->type() == "CompleteMessage")
      return 3;
    else if (x->type() == "ScanMessage")
      return 4;
    else if (x->type() == "TupleMessage")
      return 5;
    else if (x->type() == "TupleSetIndexMessage")
      return 6;
    else
      return -1;
  }

  // Applies f to the value of x.
  template<class F>
  static auto visit(F &&f, const value_type &x) {
    switch (type_index(x)) {
      case 1:
        return f(dynamic_cast<StartMessage &>(*x));
      case 2:
        return f(dynamic_cast<ConnectMessage &>(*x));
      case 3:
        return f(dynamic_cast<CompleteMessage &>(*x));
      case 4:
        return f(dynamic_cast<ScanMessage &>(*x));
      case 5:
        return f(dynamic_cast<TupleMessage &>(*x));
      case 6:
        return f(dynamic_cast<TupleSetIndexMessage &>(*x));
      default: {
        none_t dummy;
        return f(dummy);
      }
    }
  }

  // Assigns a value to x.
  template<class U>
  static void assign(value_type &x, U value) {
    if constexpr (std::is_same<U, none_t>::value)
      x.reset();
    else
      x = std::make_shared<U>(std::move(value));
  }

  // Create a default-constructed object for `type` and then call the
  // continuation with the temporary object to perform remaining load steps.
  template<class F>
  static bool load(type_id_t type, F continuation) {
    switch (type) {
      default:
        return false;
      case type_id_v<none_t>: {
        none_t dummy;
        continuation(dummy);
        return true;
      }
      case type_id_v<StartMessage>: {
        auto tmp = StartMessage{};
        continuation(tmp);
        return true;
      }
      case type_id_v<ConnectMessage>: {
        auto tmp = ConnectMessage{};
        continuation(tmp);
        return true;
      }
      case type_id_v<CompleteMessage>: {
        auto tmp = CompleteMessage{};
        continuation(tmp);
        return true;
      }
      case type_id_v<ScanMessage>: {
        auto tmp = ScanMessage{};
        continuation(tmp);
        return true;
      }
      case type_id_v<TupleMessage>: {
        auto tmp = TupleMessage{};
        continuation(tmp);
        return true;
      }
      case type_id_v<TupleSetIndexMessage>: {
        auto tmp = TupleSetIndexMessage{};
        continuation(tmp);
        return true;
      }
    }
  }
};

template <>
struct inspector_access<MessagePtr> : variant_inspector_access<MessagePtr> {
  // nop
};

} // namespace caf

#endif //NORMAL_NORMAL_EXECUTOR_INCLUDE_NORMAL_EXECUTOR_SERIALIZATION_MESSAGESERIALIZER_H
