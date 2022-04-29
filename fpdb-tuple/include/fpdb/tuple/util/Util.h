//
// Created by Yifei Yang on 1/18/22.
//

#ifndef FPDB_FPDB_TUPLE_INCLUDE_FPDB_TUPLE_UTIL_UTIL_H
#define FPDB_FPDB_TUPLE_INCLUDE_FPDB_TUPLE_UTIL_UTIL_H

#include <arrow/api.h>
#include <tl/expected.hpp>

namespace fpdb::tuple::util {

class Util {

public:
  static tl::expected<std::shared_ptr<arrow::Array>, std::string>
  makeEmptyArray(const std::shared_ptr<arrow::DataType> &type);

  static tl::expected<std::shared_ptr<arrow::RecordBatch>, std::string>
  makeEmptyRecordBatch(const std::shared_ptr<arrow::Schema> &schema);

};

}


#endif //FPDB_FPDB_TUPLE_INCLUDE_FPDB_TUPLE_UTIL_UTIL_H
