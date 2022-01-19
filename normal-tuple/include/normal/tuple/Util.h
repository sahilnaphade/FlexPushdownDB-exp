//
// Created by Yifei Yang on 1/18/22.
//

#ifndef NORMAL_NORMAL_TUPLE_INCLUDE_NORMAL_TUPLE_UTIL_H
#define NORMAL_NORMAL_TUPLE_INCLUDE_NORMAL_TUPLE_UTIL_H

#include <arrow/api.h>
#include <tl/expected.hpp>

namespace normal::tuple {

class Util {

public:
  static tl::expected<std::shared_ptr<arrow::Array>, std::string>
  makeEmptyArray(const std::shared_ptr<arrow::DataType> &type);

};

}


#endif //NORMAL_NORMAL_TUPLE_INCLUDE_NORMAL_TUPLE_UTIL_H
