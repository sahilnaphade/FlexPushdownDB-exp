//
// Created by matt on 8/5/20.
//

#ifndef NORMAL_NORMAL_TUPLE_INCLUDE_NORMAL_TUPLE_COLUMNBUILDER_H
#define NORMAL_NORMAL_TUPLE_INCLUDE_NORMAL_TUPLE_COLUMNBUILDER_H

#include "Column.h"
#include <arrow/array/builder_base.h>
#include <arrow/builder.h>
#include <tl/expected.hpp>
#include <memory>
#include <utility>

namespace normal::tuple {

class ColumnBuilder {

public:
  ColumnBuilder(std::string  name, const std::shared_ptr<::arrow::DataType>& type);

  static std::shared_ptr<ColumnBuilder> make(const std::string& name, const std::shared_ptr<::arrow::DataType>& type);

  void append(const std::shared_ptr<Scalar>& scalar);
  tl::expected<void, std::string> appendNulls(int64_t length);

  std::shared_ptr<Column> finalize();

private:
  std::string name_;
  std::unique_ptr<::arrow::ArrayBuilder> arrowBuilder_;
  std::shared_ptr<::arrow::Array> array_;

};

}

#endif //NORMAL_NORMAL_TUPLE_INCLUDE_NORMAL_TUPLE_COLUMNBUILDER_H
