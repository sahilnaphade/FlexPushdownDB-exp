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

BloomFilter::BloomFilter(int64_t capacity,
                         double falsePositiveRate,
                         int64_t numHashFunctions,
                         int64_t numBits,
                         const std::vector<std::shared_ptr<UniversalHashFunction>> &hashFunctions,
                         const std::vector<int64_t> &bitArray) :
  capacity_(capacity),
  falsePositiveRate_(falsePositiveRate),
  numHashFunctions_(numHashFunctions),
  numBits_(numBits),
  hashFunctions_(hashFunctions),
  bitArray_(bitArray) {

  assert(falsePositiveRate >= 0.0 && falsePositiveRate <= 1.0);
}

std::shared_ptr<BloomFilter> BloomFilter::make(int64_t capacity, double falsePositiveRate) {
  return std::make_shared<BloomFilter>(capacity, falsePositiveRate);
}

std::shared_ptr<BloomFilter> BloomFilter::make(int64_t capacity,
                                               double falsePositiveRate,
                                               int64_t numHashFunctions,
                                               int64_t numBits,
                                               const std::vector<std::shared_ptr<UniversalHashFunction>> &hashFunctions,
                                               const std::vector<int64_t> &bitArray) {
  return std::make_shared<BloomFilter>(capacity, falsePositiveRate, numHashFunctions, numBits, hashFunctions, bitArray);
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

  for (auto h: hs) {
    // set h-th bit
    int64_t valueId = h / 64;
    int valueOffset = h % 64;
    bitArray_[valueId] ^= (-1 ^ bitArray_[valueId]) & (1UL << valueOffset);
  }
}

bool BloomFilter::contains(int64_t key) {
  if (capacity_ == 0)
    return false;

  auto hs = hashes(key);

  for (auto h: hs) {
    // check h-th bit
    int64_t valueId = h / 64;
    int valueOffset = h % 64;
    if (!((bitArray_[valueId] >> valueOffset) & 1UL)) {
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
  std::vector<int64_t> mergedBitArray;
  mergedBitArray.reserve(bitArray_.size());
  for (uint64_t i = 0; i < bitArray_.size(); ++i) {
    mergedBitArray.emplace_back(bitArray_[i] | other->bitArray_[i]);
  }
  bitArray_ = mergedBitArray;

  return {};
}

::nlohmann::json BloomFilter::toJson() const {
  ::nlohmann::json jObj;
  jObj.emplace("capacity", capacity_);
  jObj.emplace("falsePositiveRate", falsePositiveRate_);
  jObj.emplace("numHashFunctions", numHashFunctions_);
  jObj.emplace("numBits", numBits_);

  std::vector<nlohmann::json> hashFunctionsJArr;
  for (const auto &function: hashFunctions_) {
    hashFunctionsJArr.emplace_back(function->toJson());
  }
  jObj.emplace("hashFunctions", hashFunctionsJArr);

  jObj.emplace("bitArray", bitArray_);

  return jObj;
}

tl::expected<std::shared_ptr<BloomFilter>, std::string> BloomFilter::fromJson(const nlohmann::json &jObj) {
  if (!jObj.contains("capacity")) {
    return tl::make_unexpected(fmt::format("Capacity not specified in bloom filter JSON '{}'", jObj));
  }
  int64_t capacity = jObj["capacity"].get<int64_t>();

  if (!jObj.contains("falsePositiveRate")) {
    return tl::make_unexpected(fmt::format("FalsePositiveRate not specified in bloom filter JSON '{}'", jObj));
  }
  double falsePositiveRate = jObj["falsePositiveRate"].get<double>();

  if (!jObj.contains("numHashFunctions")) {
    return tl::make_unexpected(fmt::format("NumHashFunctions not specified in bloom filter JSON '{}'", jObj));
  }
  int64_t numHashFunctions = jObj["numHashFunctions"].get<int64_t>();

  if (!jObj.contains("numBits")) {
    return tl::make_unexpected(fmt::format("NumBits not specified in bloom filter JSON '{}'", jObj));
  }
  int64_t numBits = jObj["numBits"].get<int64_t>();

  if (!jObj.contains("hashFunctions")) {
    return tl::make_unexpected(fmt::format("HashFunctions not specified in bloom filter JSON '{}'", jObj));
  }
  std::vector<std::shared_ptr<UniversalHashFunction>> hashFunctions;
  auto hashFunctionsJArr = jObj["hashFunctions"].get<std::vector<nlohmann::json>>();
  for (const auto &hashFunctionJObj: hashFunctionsJArr) {
    auto expHashFunction = UniversalHashFunction::fromJson(hashFunctionJObj);
    if (!expHashFunction.has_value()) {
      return tl::make_unexpected(expHashFunction.error());
    }
    hashFunctions.emplace_back(*expHashFunction);
  }

  if (!jObj.contains("bitArray")) {
    return tl::make_unexpected(fmt::format("BitArray not specified in bloom filter JSON '{}'", jObj));
  }
  std::vector<int64_t> bitArray = jObj["bitArray"].get<std::vector<int64_t>>();

  return make(capacity, falsePositiveRate, numHashFunctions, numBits, hashFunctions, bitArray);
}

int64_t BloomFilter::calculateNumHashFunctions() const {
  return k_from_p(falsePositiveRate_);
}

int64_t BloomFilter::calculateNumBits() const {
  return m_from_np(capacity_, falsePositiveRate_);
}

std::vector<std::shared_ptr<UniversalHashFunction>> BloomFilter::makeHashFunctions() {
  std::vector<std::shared_ptr<UniversalHashFunction>> hashFunctions;

  // check capacity
  if (capacity_ == 0) {
    numHashFunctions_ = 0;
    return hashFunctions;
  }

  hashFunctions.reserve(numHashFunctions_);
  for (int64_t s = 0; s < numHashFunctions_; ++s) {
    hashFunctions.emplace_back(UniversalHashFunction::make(numBits_));
  }
  return hashFunctions;
}

std::vector<int64_t> BloomFilter::makeBitArray() const {
  int64_t len = numBits_ / 64 + 1;
  return std::vector<int64_t>(len, 0);
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