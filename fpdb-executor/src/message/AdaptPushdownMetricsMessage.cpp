//
// Created by Yifei Yang on 10/31/22.
//

#include <fpdb/executor/message/AdaptPushdownMetricsMessage.h>
#include <fmt/format.h>

namespace fpdb::executor::message {

AdaptPushdownMetricsMessage::AdaptPushdownMetricsMessage(const std::string &key,
                                                         int64_t execTime,
                                                         const std::string &sender):
  Message(ADAPT_PUSHDOWN_METRICS, sender),
  key_(key),
  execTime_(execTime) {}

tl::expected<std::string, std::string>
AdaptPushdownMetricsMessage::generateAdaptPushdownMetricsKey(long queryId, const std::string &op) {
  if (op.substr(0, PullupOpNamePrefix.size()) == PullupOpNamePrefix ||
      op.substr(0, PushdownOpNamePrefix.size()) == PushdownOpNamePrefix) {
    auto pos = op.find("]");
    return fmt::format("{}-{}", queryId, op.substr(0, pos + 1));
  } else {
    return tl::make_unexpected(fmt::format("Invalid op name for adaptive pushdown metrics: {}", op));
  }
}

std::string AdaptPushdownMetricsMessage::getTypeString() const {
  return "AdaptPushdownMetricsMessage";
}

const std::string &AdaptPushdownMetricsMessage::getKey() const {
  return key_;
}

int64_t AdaptPushdownMetricsMessage::getExecTime() const {
  return execTime_;
}

}
