//
// Created by Yifei Yang on 3/17/22.
//

#include <fpdb/executor/physical/bloomfilter/BloomFilter.h>
#include <fmt/format.h>
#include <cmath>
#include <cassert>

namespace fpdb::executor::physical::bloomfilter {

BloomFilter::BloomFilter(int64_t capacity, double falsePositiveRate) :
  capacity_(capacity),
  falsePositiveRate_(falsePositiveRate) {

  assert(falsePositiveRate >= 0.0 && falsePositiveRate <= 1.0);
}

std::shared_ptr<BloomFilter> BloomFilter::make(int64_t capacity, double falsePositiveRate) {
  return std::make_shared<BloomFilter>(capacity, falsePositiveRate);
}

void BloomFilter::init() {
  numHashFunctions_ = calculateNumHashFunctions();
  numBits_ = calculateNumBits();
  hashFunctions_ = makeHashFunctions();
  bitArray_ = makeBitArray();
}

void BloomFilter::add(int64_t key) {
  assert(capacity_ > 0);

  auto hs = hashes(key);
  bool foundAllBits = true;

  for (auto h: hs) {
    if (foundAllBits && not bitArray_[h]) {
      foundAllBits = false;
    }
    bitArray_[h] = true;
  }
}

bool BloomFilter::contains(int64_t key) {
  if (capacity_ == 0)
    return false;

  auto hs = hashes(key);

  for (auto h: hs) {
    if (!bitArray_[h]) {
      return false;
    }
  }

  return true;
}

tl::expected<void, std::string> BloomFilter::merge(const std::shared_ptr<BloomFilter> &other) {
  // check
  if (capacity_ != other->capacity_) {
    return tl::make_unexpected(fmt::format("Capacity mismatch, {} vs {}",
                                           capacity_, other->capacity_));
  }
  if (falsePositiveRate_ != other->falsePositiveRate_) {
    return tl::make_unexpected(fmt::format("FalsePositiveRate mismatch, {} vs {}",
                                           falsePositiveRate_, other->falsePositiveRate_));
  }

  // update bit arrays
  std::vector<bool> mergedBitArray;
  std::transform(bitArray_.begin(), bitArray_.end(),
                 other->bitArray_.begin(), mergedBitArray.begin(), std::logical_or<bool>());
  bitArray_ = mergedBitArray;

  return {};
}

int64_t BloomFilter::calculateNumHashFunctions() const {
  return k_from_p(falsePositiveRate_);
}

int64_t BloomFilter::calculateNumBits() const {
  return m_from_np(capacity_, falsePositiveRate_);
}

std::vector<std::shared_ptr<UniversalHashFunction>> BloomFilter::makeHashFunctions() const {
  std::vector<std::shared_ptr<UniversalHashFunction>> hashFunctions;
  hashFunctions.reserve(numHashFunctions_);
  for (int64_t s = 0; s < numHashFunctions_; ++s) {
    hashFunctions.emplace_back(UniversalHashFunction::make(numBits_));
  }
  return hashFunctions;
}

std::vector<bool> BloomFilter::makeBitArray() const {
  return std::vector<bool>(numBits_, false);
}

std::vector<int64_t> BloomFilter::hashes(int64_t key) {
  std::vector<int64_t> hashes(hashFunctions_.size());

  for (size_t i = 0; i < hashFunctions_.size(); ++i) {
    hashes[i] = hashFunctions_[i]->hash(key);
  }

  return hashes;
}

int64_t BloomFilter::k_from_p(double p) {
  return std::ceil(
          std::log(1 / p) / std::log(2));
}

int64_t BloomFilter::m_from_np(int64_t n, double p) {
  return std::ceil(
          ((double) n) * std::abs(std::log(p)) / std::pow(std::log(2), 2));
}

}
