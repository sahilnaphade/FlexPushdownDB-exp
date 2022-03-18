//
// Created by Yifei Yang on 3/17/22.
//

#include <fpdb/executor/message/BloomFilterMessage.h>

namespace fpdb::executor::message {

BloomFilterMessage::BloomFilterMessage(const std::shared_ptr<BloomFilter> &bloomFilter,
                                       const std::string &sender):
  Message(BLOOM_FILTER, sender),
  bloomFilter_(bloomFilter) {}

std::string BloomFilterMessage::getTypeString() const {
  return "BloomFilterMessage";
}

const std::shared_ptr<BloomFilter> &BloomFilterMessage::getBloomFilter() const {
  return bloomFilter_;
}

}
