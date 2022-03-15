//
// Created by Yifei Yang on 3/14/22.
//

#ifndef FPDB_FPDB_EXECUTOR_INCLUDE_FPDB_EXECUTOR_MESSAGE_DEBUGMETRICSMESSAGE_H
#define FPDB_FPDB_EXECUTOR_INCLUDE_FPDB_EXECUTOR_MESSAGE_DEBUGMETRICSMESSAGE_H

#include <fpdb/executor/message/Message.h>
#include <memory>

namespace fpdb::executor::message {

class DebugMetricsMessage : public Message {

public:
  DebugMetricsMessage(int64_t bytesFromStore,
                     std::string sender);
  DebugMetricsMessage() = default;
  DebugMetricsMessage(const DebugMetricsMessage&) = default;
  DebugMetricsMessage& operator=(const DebugMetricsMessage&) = default;
  ~DebugMetricsMessage() override = default;

  std::string getTypeString() const override;

  int64_t getBytesFromStore() const;

private:
  int64_t bytesFromStore_;

// caf inspect
public:
  template <class Inspector>
  friend bool inspect(Inspector& f, DebugMetricsMessage& msg) {
    return f.object(msg).fields(f.field("type", msg.type_),
                                f.field("sender", msg.sender_),
                                f.field("bytesFromStore", msg.bytesFromStore_));
  }
};

}


#endif //FPDB_FPDB_EXECUTOR_INCLUDE_FPDB_EXECUTOR_MESSAGE_DEBUGMETRICSMESSAGE_H
