//
// Created by Yifei Yang on 3/17/22.
//

#include <fpdb/executor/physical/bloomfilter/UniversalHashFunction.h>
#include <primesieve.hpp>
#include <random>
#include <cassert>

namespace fpdb::executor::physical::bloomfilter {

UniversalHashFunction::UniversalHashFunction(int64_t m) {

  assert(m > 0);

  std::random_device rd;
  std::mt19937 gen(rd());

  primesieve::set_num_threads(1);

  m_ = m;

  // Pick a random integer greater than or equal to m (upper bound of 2m)
  int64_t i = std::uniform_int_distribution<int64_t>(m_, 2 * m_)(gen);

  // Get the 1st prime greater than or equal to i
  p_ = primesieve::nth_prime(0, i);

  // Set a = random int (less than p) (where a != 0)
  a_ = std::uniform_int_distribution<int64_t>(1, p_ - 1)(gen);

  // Set b = random int (less than p)
  b_ = std::uniform_int_distribution<int64_t>(0, p_ - 1)(gen);
}

std::shared_ptr<UniversalHashFunction> UniversalHashFunction::make(int64_t m) {
  return std::make_shared<UniversalHashFunction>(m);
}

int64_t UniversalHashFunction::hash(int64_t x) const {
  // to prevent overflow
  __int128 mul = ((__int128) a_) * ((__int128) x);
  int64_t h = ((mul + b_) % p_) % m_;;

  assert(h >= 0 && h <= m_);
  return h;
}

}
