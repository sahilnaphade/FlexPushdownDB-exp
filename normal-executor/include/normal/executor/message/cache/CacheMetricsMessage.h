//
// Created by Yifei Yang on 11/2/20.
//

#ifndef NORMAL_NORMAL_EXECUTOR_INCLUDE_NORMAL_EXECUTOR_MESSAGE_CACHE_CACHEMETRICSMESSAGE_H
#define NORMAL_NORMAL_EXECUTOR_INCLUDE_NORMAL_EXECUTOR_MESSAGE_CACHE_CACHEMETRICSMESSAGE_H

#include <normal/executor/message/Message.h>
#include <normal/caf/CAFUtil.h>
#include <memory>

namespace normal::executor::message {

class CacheMetricsMessage : public Message {

public:
  CacheMetricsMessage(size_t hitNum, size_t missNum, size_t shardHitNum, size_t shardMissNum, const std::string &sender);
  CacheMetricsMessage() = default;
  CacheMetricsMessage(const CacheMetricsMessage&) = default;
  CacheMetricsMessage& operator=(const CacheMetricsMessage&) = default;

  static std::shared_ptr<CacheMetricsMessage> make(size_t hitNum, size_t missNum, size_t shardHitNum, size_t shardMissNum, const std::string &sender);

  size_t getHitNum() const;
  size_t getMissNum() const;

  size_t getShardHitNum() const;
  size_t getShardMissNum() const;

private:
  size_t hitNum_;
  size_t missNum_;
  size_t shardHitNum_;
  size_t shardMissNum_;

// caf inspect
public:
  template <class Inspector>
  friend bool inspect(Inspector& f, CacheMetricsMessage& msg) {
    return f.object(msg).fields(f.field("type", msg.typeNoConst()),
                                f.field("sender", msg.senderNoConst()),
                                f.field("hitNum", msg.hitNum_),
                                f.field("missNum", msg.missNum_),
                                f.field("shardHitNum", msg.shardHitNum_),
                                f.field("shardMissNum", msg.shardMissNum_));
  };
};

}

using CacheMetricsMessagePtr = std::shared_ptr<normal::executor::message::CacheMetricsMessage>;

namespace caf {
template <>
struct inspector_access<CacheMetricsMessagePtr> : variant_inspector_access<CacheMetricsMessagePtr> {
  // nop
};
} // namespace caf

#endif //NORMAL_NORMAL_EXECUTOR_INCLUDE_NORMAL_EXECUTOR_MESSAGE_CACHE_CACHEMETRICSMESSAGE_H
