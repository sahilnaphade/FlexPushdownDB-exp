//
// Created by Yifei Yang on 9/9/20.
//

#include <normal/executor/message/cache/WeightRequestMessage.h>

using namespace normal::executor::message;

WeightRequestMessage::WeightRequestMessage(const std::shared_ptr<std::unordered_map<std::shared_ptr<SegmentKey>, double>> &weightMap,
                                           const std::string &sender) :
  Message("WeightRequestMessage", sender),
  weightMap_(weightMap) {}

std::shared_ptr<WeightRequestMessage> WeightRequestMessage::make(
        const std::shared_ptr<std::unordered_map<std::shared_ptr<SegmentKey>, double>> &weightMap,
        const std::string &sender) {
  return std::make_shared<WeightRequestMessage>(weightMap, sender);
}

const std::shared_ptr<std::unordered_map<std::shared_ptr<SegmentKey>, double>> &
WeightRequestMessage::getWeightMap() const {
  return weightMap_;
}
