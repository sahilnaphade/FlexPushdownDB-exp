//
// Created by matt on 20/5/20.
//

#ifndef NORMAL_NORMAL_EXECUTOR_INCLUDE_NORMAL_EXECUTOR_MESSAGE_CACHE_LOADREQUESTMESSAGE_H
#define NORMAL_NORMAL_EXECUTOR_INCLUDE_NORMAL_EXECUTOR_MESSAGE_CACHE_LOADREQUESTMESSAGE_H

#include <normal/executor/message/Message.h>
#include <normal/cache/SegmentKey.h>
#include <normal/cache/SegmentData.h>
#include <normal/caf/CAFUtil.h>

using namespace normal::cache;

namespace normal::executor::message {

/**
 * Request for the segment cache actor to load a segment given a segment key. On a hit, the segment is sent to the
 * requesting actor, or nullopt on a cache miss.
 */
class LoadRequestMessage : public Message {

public:
  LoadRequestMessage(std::vector<std::shared_ptr<SegmentKey>> segmentKeys,
							  const std::string &sender);
  LoadRequestMessage() = default;
  LoadRequestMessage(const LoadRequestMessage&) = default;
  LoadRequestMessage& operator=(const LoadRequestMessage&) = default;

  static std::shared_ptr<LoadRequestMessage> make(const std::vector<std::shared_ptr<SegmentKey>>& segmentKeys, const std::string &sender);

  [[nodiscard]] const std::vector<std::shared_ptr<SegmentKey>> &getSegmentKeys() const;

  [[nodiscard]] std::string toString() const;

private:
  std::vector<std::shared_ptr<SegmentKey>> segmentKeys_;

// caf inspect
public:
  template <class Inspector>
  friend bool inspect(Inspector& f, LoadRequestMessage& msg) {
    return f.object(msg).fields(f.field("type", msg.typeNoConst()),
                                f.field("sender", msg.senderNoConst()),
                                f.field("segmentKeys", msg.segmentKeys_));
  }
};

}

using LoadRequestMessagePtr = std::shared_ptr<normal::executor::message::LoadRequestMessage>;

namespace caf {
template <>
struct inspector_access<LoadRequestMessagePtr> : variant_inspector_access<LoadRequestMessagePtr> {
  // nop
};
} // namespace caf

#endif //NORMAL_NORMAL_EXECUTOR_INCLUDE_NORMAL_EXECUTOR_MESSAGE_CACHE_LOADREQUESTMESSAGE_H
