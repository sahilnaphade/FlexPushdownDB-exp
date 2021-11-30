//
// Created by Yifei Yang on 9/9/20.
//

#ifndef NORMAL_NORMAL_EXECUTOR_INCLUDE_NORMAL_EXECUTOR_MESSAGE_CACHE_WEIGHTREQUESTMESSAGE_H
#define NORMAL_NORMAL_EXECUTOR_INCLUDE_NORMAL_EXECUTOR_MESSAGE_CACHE_WEIGHTREQUESTMESSAGE_H

#include <normal/executor/message/Message.h>
#include <normal/cache/SegmentKey.h>
#include <unordered_map>

using namespace normal::cache;

namespace normal::executor::message {

/**
 * A message to update segment weights
 */
class WeightRequestMessage : public Message {

public:
  WeightRequestMessage(const std::shared_ptr<std::unordered_map<std::shared_ptr<SegmentKey>, double>> &weightMap,
                       const std::string &sender);

  static std::shared_ptr<WeightRequestMessage> make(
          const std::shared_ptr<std::unordered_map<std::shared_ptr<SegmentKey>, double>> &weightMap,
          const std::string &sender);

  const std::shared_ptr<std::unordered_map<std::shared_ptr<SegmentKey>, double>> &getWeightMap() const;

private:
  std::shared_ptr<std::unordered_map<std::shared_ptr<SegmentKey>, double>> weightMap_;
};

}


#endif //NORMAL_NORMAL_EXECUTOR_INCLUDE_NORMAL_EXECUTOR_MESSAGE_CACHE_WEIGHTREQUESTMESSAGE_H
