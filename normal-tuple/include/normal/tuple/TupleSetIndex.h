//
// Created by matt on 1/8/20.
//

#ifndef NORMAL_NORMAL_TUPLE_INCLUDE_NORMAL_TUPLE_TUPLESETINDEX_H
#define NORMAL_NORMAL_TUPLE_INCLUDE_NORMAL_TUPLE_TUPLESETINDEX_H

#include <normal/tuple/TupleKey.h>
#include <arrow/api.h>
#include <tl/expected.hpp>
#include <utility>
#include <vector>
#include <unordered_map>

using namespace std;

namespace normal::tuple {

/**
 * An index into a tuple set
 *
 * Contains a map of values for particular columns to row numbers in the tuple set
 *
 * TODO: Should really operate on tuple sets and not an arrow table
 */
class TupleSetIndex {
public:
  TupleSetIndex(vector<string> columnNames,
                vector<int> columnIndexes,
                shared_ptr<::arrow::Table> table,
                unordered_multimap<shared_ptr<TupleKey>, int64_t, TupleKeyPointerHash, TupleKeyPointerPredicate> valueRowMap);
  virtual ~TupleSetIndex() = default;

  static tl::expected<shared_ptr<TupleSetIndex>, string> make(const vector<string> &columnNames,
                                                              const shared_ptr<::arrow::Table> &table);

  int64_t size() const;
  const shared_ptr<::arrow::Table> &getTable() const;
  string toString() const;

  /**
   * Invokes combineChunks on the underlying value->row map
   *
   * @return
   */
  tl::expected<void, string> combine();

  /**
   * Build TupleSetIndex given columnIndexes, row offset and table
   *
   * @param columnIndexes
   * @param rowIndexOffset
   * @param table
   * @return
   */
  static tl::expected<unordered_multimap<shared_ptr<TupleKey>, int64_t, TupleKeyPointerHash, TupleKeyPointerPredicate>, string>
  build(const vector<int>& columnIndexes, int64_t rowIndexOffset, const std::shared_ptr<::arrow::Table> &table);

  /**
   * Adds the given table to the index
   *
   * @param table
   * @return
   */
  tl::expected<void, string> put(const shared_ptr<::arrow::Table> &table);

  /**
   * Adds another index to this index
   *
   * @param other
   * @return
   */
  tl::expected<void, string> merge(const shared_ptr<TupleSetIndex> &other);

  /**
   * Find rows of given tupleKey
   *
   * @param tupleKey
   * @return
   */
  vector<int64_t> find(const shared_ptr<TupleKey> &tupleKey);
  
  /**
   * Validate the tupleSetIndex
   * @return 
   */
  tl::expected<void, string> validate();

private:
  vector<string> columnNames_;
  vector<int> columnIndexes_;
  shared_ptr<::arrow::Table> table_;
  unordered_multimap<shared_ptr<TupleKey>, int64_t, TupleKeyPointerHash, TupleKeyPointerPredicate> valueRowMap_;
};

}

#endif //NORMAL_NORMAL_TUPLE_INCLUDE_NORMAL_TUPLE_TUPLESETINDEX_H
