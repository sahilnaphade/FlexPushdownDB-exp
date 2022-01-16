//
// Created by Yifei Yang on 1/15/22.
//

#include <normal/tuple/DataType.h>

namespace normal::tuple {

DataType::DataType(const std::shared_ptr<arrow::DataType> &dataType):
  dataType_(dataType) {}

const std::shared_ptr<arrow::DataType> DataType::getDataType() const {
  return dataType_;
}

}
