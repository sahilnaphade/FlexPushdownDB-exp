//
// Created by Yifei Yang on 3/14/22.
//

#ifndef FPDB_FPDB_EXECUTOR_INCLUDE_FPDB_EXECUTOR_MESSAGE_DEBUGMETRICSMESSAGE_H
#define FPDB_FPDB_EXECUTOR_INCLUDE_FPDB_EXECUTOR_MESSAGE_DEBUGMETRICSMESSAGE_H

#include <fpdb/executor/message/Message.h>
#include <fpdb/executor/metrics/DebugMetrics.h>
#include <memory>

namespace fpdb::executor::message {

class DebugMetricsMessage : public Message {

public:
  DebugMetricsMessage(const executor::metrics::DebugMetrics &debugMetrics,
                      const std::string &sender);
  DebugMetricsMessage() = default;
  DebugMetricsMessage(const DebugMetricsMessage&) = default;
  DebugMetricsMessage& operator=(const DebugMetricsMessage&) = default;
  ~DebugMetricsMessage() override = default;

  std::string getTypeString() const override;

  const executor::metrics::DebugMetrics &getDebugMetrics() const;

private:
  executor::metrics::DebugMetrics debugMetrics_;

// caf inspect
public:
  template <class Inspector>
  friend bool inspect(Inspector& f, DebugMetricsMessage& msg) {
    return f.object(msg).fields(f.field("type", msg.type_),
                                f.field("sender", msg.sender_),
                                f.field("debugMetrics", msg.debugMetrics_));
  }
};

}


#endif //FPDB_FPDB_EXECUTOR_INCLUDE_FPDB_EXECUTOR_MESSAGE_DEBUGMETRICSMESSAGE_H
