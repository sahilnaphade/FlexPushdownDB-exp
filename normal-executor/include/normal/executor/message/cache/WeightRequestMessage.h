//
// Created by Yifei Yang on 9/9/20.
//

#ifndef NORMAL_NORMAL_EXECUTOR_INCLUDE_NORMAL_EXECUTOR_MESSAGE_CACHE_WEIGHTREQUESTMESSAGE_H
#define NORMAL_NORMAL_EXECUTOR_INCLUDE_NORMAL_EXECUTOR_MESSAGE_CACHE_WEIGHTREQUESTMESSAGE_H

#include <normal/executor/message/Message.h>
#include <normal/cache/SegmentKey.h>
#include <normal/caf/CAFUtil.h>
#include <unordered_map>

using namespace normal::cache;

namespace normal::executor::message {

/**
 * A message to update segment weights
 */
class WeightRequestMessage : public Message {

public:
  WeightRequestMessage(const std::unordered_map<std::shared_ptr<SegmentKey>, double> &weightMap,
                       const std::string &sender);
  WeightRequestMessage() = default;
  WeightRequestMessage(const WeightRequestMessage&) = default;
  WeightRequestMessage& operator=(const WeightRequestMessage&) = default;

  static std::shared_ptr<WeightRequestMessage>
  make(const std::unordered_map<std::shared_ptr<SegmentKey>, double> &weightMap, const std::string &sender);

  std::string getTypeString() const override;

  const std::unordered_map<std::shared_ptr<SegmentKey>, double> &getWeightMap() const;

private:
  std::unordered_map<std::shared_ptr<SegmentKey>, double> weightMap_;

// caf inspect
public:
  template <class Inspector>
  friend bool inspect(Inspector& f, WeightRequestMessage& msg) {
    return f.object(msg).fields(f.field("type", msg.type_),
                                f.field("sender", msg.sender_),
                                f.field("weightMap", msg.weightMap_));
  };
};

}

using WeightRequestMessagePtr = std::shared_ptr<normal::executor::message::WeightRequestMessage>;

namespace caf {
template <>
struct inspector_access<WeightRequestMessagePtr> : variant_inspector_access<WeightRequestMessagePtr> {
  // nop
};
} // namespace caf

#endif //NORMAL_NORMAL_EXECUTOR_INCLUDE_NORMAL_EXECUTOR_MESSAGE_CACHE_WEIGHTREQUESTMESSAGE_H
