//
// Created by matt on 20/7/20.
//

#ifndef NORMAL_NORMAL_EXECUTOR_INCLUDE_NORMAL_EXECUTOR_PHYSICAL_MERGE_MERGEKERNEL_H
#define NORMAL_NORMAL_EXECUTOR_INCLUDE_NORMAL_EXECUTOR_PHYSICAL_MERGE_MERGEKERNEL_H

#include <normal/tuple/TupleSet.h>
#include <tl/expected.hpp>
#include <vector>
#include <memory>
#include <string>

using namespace normal::tuple;

namespace normal::executor::physical::merge {

class MergeKernel {

public:

  static tl::expected<void, std::string>
  validateTupleSets(const std::shared_ptr<TupleSet> &tupleSet1, const std::shared_ptr<TupleSet> &tupleSet2);

  static std::shared_ptr<Schema>
  mergeSchema(const std::shared_ptr<TupleSet> &tupleSet1, const std::shared_ptr<TupleSet> &tupleSet2);

  static std::vector<std::shared_ptr<::arrow::ChunkedArray>>
  mergeArrays(const std::shared_ptr<TupleSet> &tupleSet1, const std::shared_ptr<TupleSet> &tupleSet2);

  static tl::expected<std::shared_ptr<TupleSet>, std::string>
  merge(const std::shared_ptr<TupleSet> &tupleSet1, const std::shared_ptr<TupleSet> &tupleSet2);
};

}

#endif //NORMAL_NORMAL_EXECUTOR_INCLUDE_NORMAL_EXECUTOR_PHYSICAL_MERGE_MERGEKERNEL_H
