//
// Created by matt on 20/5/20.
//

#ifndef NORMAL_NORMAL_EXECUTOR_INCLUDE_NORMAL_EXECUTOR_MESSAGE_CACHE_LOADRESPONSEMESSAGE_H
#define NORMAL_NORMAL_EXECUTOR_INCLUDE_NORMAL_EXECUTOR_MESSAGE_CACHE_LOADRESPONSEMESSAGE_H

#include <normal/executor/message/Message.h>
#include <normal/cache/SegmentKey.h>
#include <normal/cache/SegmentData.h>
#include <unordered_map>

using namespace normal::cache;

namespace normal::executor::message {

/**
 * Response sent from segment cache actor containing a segment loaded from cache, segment data is nullopt if segment was not found
 * in cache.
 */
class LoadResponseMessage : public Message {

public:
  LoadResponseMessage(std::unordered_map<std::shared_ptr<SegmentKey>, std::shared_ptr<SegmentData>, SegmentKeyPointerHash, SegmentKeyPointerPredicate> segments,
					            const std::string &sender,
                      std::vector<std::shared_ptr<SegmentKey>> segmentKeysToCache);

  static std::shared_ptr<LoadResponseMessage> make(std::unordered_map<std::shared_ptr<SegmentKey>, std::shared_ptr<SegmentData>, SegmentKeyPointerHash, SegmentKeyPointerPredicate> segments,
												                           const std::string &sender,
                                                   std::vector<std::shared_ptr<SegmentKey>> segmentKeysToCache);

  [[maybe_unused]] [[nodiscard]] const std::unordered_map<std::shared_ptr<SegmentKey>, std::shared_ptr<SegmentData>, SegmentKeyPointerHash, SegmentKeyPointerPredicate> &getSegments() const;
  [[nodiscard]] const std::vector<std::shared_ptr<SegmentKey>> &getSegmentKeysToCache() const;

  [[nodiscard]] std::string toString() const;

private:
  std::unordered_map<std::shared_ptr<SegmentKey>, std::shared_ptr<SegmentData>, SegmentKeyPointerHash, SegmentKeyPointerPredicate> segments_;
  std::vector<std::shared_ptr<SegmentKey>> segmentKeysToCache_;

};

}

#endif //NORMAL_NORMAL_EXECUTOR_INCLUDE_NORMAL_EXECUTOR_MESSAGE_CACHE_LOADRESPONSEMESSAGE_H