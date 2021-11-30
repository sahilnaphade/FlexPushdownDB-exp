//
// Created by matt on 20/5/20.
//

#ifndef NORMAL_NORMAL_EXECUTOR_INCLUDE_NORMAL_EXECUTOR_MESSAGE_CACHE_STOREREQUESTMESSAGE_H
#define NORMAL_NORMAL_EXECUTOR_INCLUDE_NORMAL_EXECUTOR_MESSAGE_CACHE_STOREREQUESTMESSAGE_H

#include <normal/executor/message/Message.h>
#include <normal/cache/SegmentKey.h>
#include <normal/cache/SegmentData.h>

using namespace normal::cache;

namespace normal::executor::message {

/**
 * Reuest for the segment cache actor to store the given segment data given a segment key.
 */
class StoreRequestMessage : public Message {

public:
  StoreRequestMessage(std::unordered_map<std::shared_ptr<SegmentKey>, std::shared_ptr<SegmentData>> segments,
					  const std::string &sender);

  static std::shared_ptr<StoreRequestMessage>
  make(const std::unordered_map<std::shared_ptr<SegmentKey>, std::shared_ptr<SegmentData>>& segments,
	   const std::string &sender);

  [[nodiscard]] const std::unordered_map<std::shared_ptr<SegmentKey>, std::shared_ptr<SegmentData>> &
  getSegments() const;

  [[nodiscard]] std::string toString() const;

private:
  std::unordered_map<std::shared_ptr<SegmentKey>, std::shared_ptr<SegmentData>> segments_;
};

}

#endif //NORMAL_NORMAL_EXECUTOR_INCLUDE_NORMAL_EXECUTOR_MESSAGE_CACHE_STOREREQUESTMESSAGE_H
