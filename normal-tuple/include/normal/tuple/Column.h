//
// Created by matt on 1/5/20.
//

#ifndef NORMAL_NORMAL_TUPLE_INCLUDE_NORMAL_TUPLE_COLUMN_H
#define NORMAL_NORMAL_TUPLE_INCLUDE_NORMAL_TUPLE_COLUMN_H

#include <normal/tuple/Scalar.h>
#include <normal/tuple/ColumnIterator.h>
#include <normal/tuple/serialization/ArrowSerializer.h>
#include <normal/caf/CAFUtil.h>
#include <arrow/api.h>
#include <arrow/table.h>
#include <arrow/array.h>
#include <tl/expected.hpp>
#include <utility>

namespace normal::tuple {

/**
 * A named array of data
 */
class Column {

public:
  explicit Column(std::string name, std::shared_ptr<::arrow::ChunkedArray> array);
  Column() = default;
  Column(const Column&) = default;
  Column& operator=(const Column&) = default;

  static std::shared_ptr<Column> make(const std::string &name, const std::shared_ptr<::arrow::Array> &array);

  static std::shared_ptr<Column> make(const std::string &name, const std::shared_ptr<::arrow::ChunkedArray> &array);

  static std::shared_ptr<Column> make(const std::string &name, const ::arrow::ArrayVector &arrays);

  /**
   * Makes an empty column of the given type
   */
  static std::shared_ptr<Column> make(const std::string &name, const std::shared_ptr<::arrow::DataType> &type);

  static std::vector<std::shared_ptr<::arrow::ChunkedArray>>
  columnVectorToArrowChunkedArrayVector(const std::vector<std::shared_ptr<Column>> &columns);

  [[nodiscard]] const std::string &getName() const;
  void setName(const std::string &Name);
  std::shared_ptr<::arrow::DataType> type();

  long numRows();
  size_t size();

  std::string showString();
  [[nodiscard]] std::string toString() const;

  /**
   * Returns the element in the column at the given row index
   *
   * @param row
   * @return
   */
  tl::expected<std::shared_ptr<Scalar>, std::string> element(long index);

  ColumnIterator begin();

  ColumnIterator end();

  [[nodiscard]] const std::shared_ptr<::arrow::ChunkedArray> &getArrowArray() const;

private:
  std::string name_;
  std::shared_ptr<::arrow::ChunkedArray> array_;

// caf inspect
public:
  template <class Inspector>
  friend bool inspect(Inspector& f, Column& column) {
    auto toBytes = [&column]() -> decltype(auto) {
      return ArrowSerializer::chunkedArray_to_bytes(column.array_);
    };
    auto fromBytes = [&column](const std::vector<std::uint8_t> &bytes) {
      column.array_ = ArrowSerializer::bytes_to_chunkedArray(bytes);
      return true;
    };
    return f.object(column).fields(f.field("name", column.name_),
                                   f.field("table", toBytes, fromBytes));
  }
};

}

using ColumnPtr = std::shared_ptr<normal::tuple::Column>;

CAF_BEGIN_TYPE_ID_BLOCK(Column, normal::caf::CAFUtil::Column_first_custom_type_id)
CAF_ADD_TYPE_ID(Column, (normal::tuple::Column))
CAF_END_TYPE_ID_BLOCK(Column)

namespace caf {
template <>
struct inspector_access<ColumnPtr> : variant_inspector_access<ColumnPtr> {
  // nop
};
} // namespace caf

#endif //NORMAL_NORMAL_TUPLE_INCLUDE_NORMAL_TUPLE_COLUMN_H
