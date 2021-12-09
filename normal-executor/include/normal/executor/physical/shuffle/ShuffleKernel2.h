//
// Created by matt on 29/7/20.
//

#ifndef NORMAL_NORMAL_EXECUTOR_INCLUDE_NORMAL_EXECUTOR_PHYSICAL_SHUFFLE_SHUFFLEKERNEL2_H
#define NORMAL_NORMAL_EXECUTOR_INCLUDE_NORMAL_EXECUTOR_PHYSICAL_SHUFFLE_SHUFFLEKERNEL2_H

#include <normal/tuple/TupleSet.h>
#include <tl/expected.hpp>
#include <vector>
#include <memory>
#include <string>

using namespace normal::tuple;
using namespace std;

namespace normal::executor::physical::shuffle {

/**
 * Shuffles a given tuple set into numSlots tuplesets based on the values in the column with the given name.
 */
class ShuffleKernel2 {

public:
  static tl::expected<vector<shared_ptr<TupleSet>>, string>
  shuffle(const vector<string> &columnName, size_t numSlots, const TupleSet &tupleSet);

};

}

#endif //NORMAL_NORMAL_EXECUTOR_INCLUDE_NORMAL_EXECUTOR_PHYSICAL_SHUFFLE_SHUFFLEKERNEL2_H
