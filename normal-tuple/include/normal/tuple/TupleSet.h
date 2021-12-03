//
// Created by matt on 12/12/19.
//

#ifndef NORMAL_NORMAL_TUPLE_INCLUDE_NORMAL_TUPLE_TUPLESET_H
#define NORMAL_NORMAL_TUPLE_INCLUDE_NORMAL_TUPLE_TUPLESET_H

#include <normal/tuple/arrow/Arrays.h>
#include <normal/tuple/arrow/TableHelper.h>
#include <normal/tuple/Globals.h>
#include <normal/tuple/Schema.h>
#include <normal/tuple/Column.h>
#include <normal/tuple/TupleSetShowOptions.h>
#include <arrow/api.h>
#include <arrow/csv/api.h>
#include <tl/expected.hpp>
#include <memory>
#include <vector>

namespace arrow { class Table; }
namespace arrow::csv { class TableReader; }

namespace normal::tuple {

/**
 * A list of tuples/rows/records. Really just encapsulates Arrow tables and record batches. Hiding
 * some of the rough edges.
 */
class TupleSet : public std::enable_shared_from_this<TupleSet> {

public:
  explicit TupleSet();
  explicit TupleSet(std::shared_ptr<arrow::Table> table);

  static std::shared_ptr<TupleSet> make(const std::shared_ptr<arrow::Table> &table);
  static std::shared_ptr<TupleSet> make(const std::shared_ptr<arrow::Schema>& schema);
  static std::shared_ptr<TupleSet> make(const std::shared_ptr<arrow::Schema>& schema,
                                        const std::vector<std::shared_ptr<arrow::Array>>& values);
  static std::shared_ptr<TupleSet> make(const std::shared_ptr<arrow::Schema>& schema,
                                        const std::vector<std::shared_ptr<arrow::ChunkedArray>>& values);
  static std::shared_ptr<TupleSet> make(const std::shared_ptr<arrow::Schema>& schema,
                                        const std::vector<std::shared_ptr<Column>>& columns);
  static std::shared_ptr<TupleSet> make(const std::vector<std::shared_ptr<Column>>& columns);
  static std::shared_ptr<TupleSet> make(const std::shared_ptr<arrow::csv::TableReader> &tableReader);
  static std::shared_ptr<TupleSet> makeWithNullTable();
  static std::shared_ptr<TupleSet> makeWithEmptyTable();

  bool valid() const;
  bool validate() const;
  void clear();
  int64_t numRows() const;
  int64_t numColumns() const;
  size_t size() const;
  std::shared_ptr<arrow::Schema> schema() const;
  std::shared_ptr<arrow::Table> table() const;
  void table(const std::shared_ptr<arrow::Table> &table);

  /**
   * Concatenate tupleSets.
   */
  static tl::expected<std::shared_ptr<TupleSet>, std::string> concatenate(const std::vector<std::shared_ptr<TupleSet>>& tupleSets);

  /**
   * Append tupleSets.
   */
  tl::expected<void, std::string> append(const std::vector<std::shared_ptr<TupleSet>>& tupleSet);
  tl::expected<void, std::string> append(const std::shared_ptr<TupleSet>& tupleSet);

  /**
   * Get column.
   */
  tl::expected<std::shared_ptr<Column>, std::string> getColumnByName(const std::string &name) const;
  tl::expected<std::shared_ptr<Column>, std::string> getColumnByIndex(const int &columnIndex) const;

  /**
   * Project specified columns, ignore non-existing ones.
   * @param columnNames
   * @return
   */
  tl::expected<std::shared_ptr<TupleSet>, std::string> projectExist(const std::vector<std::string> &columnNames) const;

  /**
   * Rename columns.
   */
  tl::expected<void, std::string> renameColumns(const std::vector<std::string>& columnNames);
  tl::expected<void, std::string> renameColumns(const std::unordered_map<std::string, std::string> &columnRenames);

  /**
   * Returns the tuple set pretty printed as a string
   *
   * @return
   */
  std::string showString();
  std::string showString(TupleSetShowOptions options);

  /**
   * Returns a short string representing the tuple set
   * @return
   */
  std::string toString() const;

  /**
   * Perform a custom function which returns an scalar value on the table.
   * @param fn
   * @return
   */
  std::shared_ptr<arrow::Scalar> visit(const std::function<std::shared_ptr<arrow::Scalar>(
          std::shared_ptr<arrow::Scalar>, arrow::RecordBatch &)>& fn);

  /**
   * Returns an element from the tupleset given and column name and row number.
   *
   * @tparam ARROW_TYPE
   * @tparam C_TYPE
   * @param columnName
   * @param row
   * @return
   */
  template<typename ARROW_TYPE, typename C_TYPE = typename ARROW_TYPE::c_type>
  tl::expected<C_TYPE, std::string> value(const std::string &columnName, int row){
    return TableHelper::value<ARROW_TYPE, C_TYPE>(columnName, row, *table_);
  }

  /**
   * Returns an string element from the tupleset given and column name and row number.
   * @param columnName
   * @param row
   * @return
   */
  tl::expected<std::string, std::string> stringValue(const std::string &columnName, int row){
	  return TableHelper::value<::arrow::StringType, std::string>(columnName, row, *table_);
  }


  /**
   * Returns an element from the tupleset given and column number and row number.
   *
   * @tparam ARROW_TYPE
   * @tparam C_TYPE
   * @param columnName
   * @param row
   * @return
   */
  template<typename ARROW_TYPE, typename C_TYPE = typename ARROW_TYPE::c_type>
  tl::expected<C_TYPE, std::string> value(int column, int row){
	return TableHelper::value<ARROW_TYPE, C_TYPE>(column, row, *table_);
  }

  /**
   * Returns a column from the tupleset as a vector given a column name
   *
   * @tparam ARROW_TYPE
   * @tparam C_TYPE
   * @param columnName
   * @param row
   * @return
   */
  template<typename ARROW_TYPE, typename C_TYPE = typename ARROW_TYPE::c_type>
  tl::expected<std::shared_ptr<std::vector<C_TYPE>>, std::string> vector(const std::string &columnName){
	return TableHelper::vector<ARROW_TYPE, C_TYPE>(*table_->GetColumnByName(columnName));
  }

private:
  static std::vector<std::shared_ptr<arrow::Table>>
  tupleSetVectorToArrowTableVector(const std::vector<std::shared_ptr<TupleSet>> &tupleSets);

  std::shared_ptr<arrow::Table> table_;
};

}

#endif //NORMAL_NORMAL_TUPLE_INCLUDE_NORMAL_TUPLE_TUPLESET_H
