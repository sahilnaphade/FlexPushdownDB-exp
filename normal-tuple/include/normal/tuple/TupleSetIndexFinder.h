//
// Created by matt on 3/8/20.
//

#ifndef NORMAL_NORMAL_TUPLE_INCLUDE_NORMAL_TUPLE_TUPLESETINDEXFINDER_H
#define NORMAL_NORMAL_TUPLE_INCLUDE_NORMAL_TUPLE_TUPLESETINDEXFINDER_H

#include <normal/tuple/TupleSetIndex.h>
#include <normal/tuple/TupleKey.h>
#include <vector>
#include <cstdint>

using namespace std;

/**
 * Provides a find method allowing the caller to look up tupleKeys in the tupleSetIndex.
 */
namespace normal::tuple {

class TupleSetIndexFinder {

public:
  TupleSetIndexFinder(shared_ptr<TupleSetIndex> tupleSetIndex,
                      vector<int> probeColumnIndexes,
                      shared_ptr<arrow::RecordBatch> recordBatch);
  virtual ~TupleSetIndexFinder() = default;

  static tl::expected<shared_ptr<TupleSetIndexFinder>, string> make(const shared_ptr<TupleSetIndex> &tupleSetIndex,
                                                                    const vector<string> &probeColumnNames,
                                                                    const shared_ptr<arrow::RecordBatch> &recordBatch);

  tl::expected<vector<int64_t>, string> find(int64_t rowIndex);

private:
  shared_ptr<TupleSetIndex> tupleSetIndex_;
  vector<int> probeColumnIndexes_;
  shared_ptr<arrow::RecordBatch> recordBatch_;

};

}

#endif //NORMAL_NORMAL_TUPLE_INCLUDE_NORMAL_TUPLE_TUPLESETINDEXFINDER_H
