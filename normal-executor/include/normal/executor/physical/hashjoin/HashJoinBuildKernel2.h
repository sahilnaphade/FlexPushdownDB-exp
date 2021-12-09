//
// Created by matt on 1/8/20.
//

#ifndef NORMAL_NORMAL_EXECUTOR_INCLUDE_NORMAL_EXECUTOR_PHYSICAL_HASHJOIN_HASHJOINBUILDKERNEL2_H
#define NORMAL_NORMAL_EXECUTOR_INCLUDE_NORMAL_EXECUTOR_PHYSICAL_HASHJOIN_HASHJOINBUILDKERNEL2_H

#include <normal/tuple/TupleSet.h>
#include <normal/tuple/TupleSetIndex.h>
#include <string>
#include <memory>

using namespace normal::tuple;
using namespace std;

namespace normal::executor::physical::hashjoin {

/**
 * Kernel for creating the hash table on the build relation in a hash join
 */
class HashJoinBuildKernel2 {

public:
  explicit HashJoinBuildKernel2(vector<string> columnNames);
  static HashJoinBuildKernel2 make(const vector<string> &columnNames);

  tl::expected<void, string> put(const shared_ptr<TupleSet> &tupleSet);
  size_t size();
  void clear();
  optional<shared_ptr<TupleSetIndex>> getTupleSetIndex();

private:

  /**
   * The columns to hash on
   */
  vector<string> columnNames_;

  /**
   * The hashtable as an indexed tupleset
   */
  optional<shared_ptr<TupleSetIndex>> tupleSetIndex_;

};

}

#endif //NORMAL_NORMAL_EXECUTOR_INCLUDE_NORMAL_EXECUTOR_PHYSICAL_HASHJOIN_HASHJOINBUILDKERNEL2_H
