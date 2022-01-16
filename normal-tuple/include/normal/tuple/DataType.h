//
// Created by Yifei Yang on 1/15/22.
//

#ifndef NORMAL_NORMAL_TUPLE_INCLUDE_NORMAL_TUPLE_DATATYPE_H
#define NORMAL_NORMAL_TUPLE_INCLUDE_NORMAL_TUPLE_DATATYPE_H

#include <normal/tuple/serialization/ArrowSerializer.h>
#include <normal/caf/CAFUtil.h>
#include <arrow/type.h>

namespace normal::tuple {

/**
 * A wrapper of arrow::DataType
 */
class DataType {

public:
  DataType(const std::shared_ptr<arrow::DataType> &dataType);
  DataType() = default;
  DataType(const DataType&) = default;
  DataType& operator=(const DataType&) = default;
  
  const std::shared_ptr<arrow::DataType> getDataType() const;

private:
  std::shared_ptr<arrow::DataType> dataType_;

// caf inspect
public:
  template <class Inspector>
  friend bool inspect(Inspector& f, DataType& dataType) {
    auto toBytes = [&dataType]() -> decltype(auto) {
      return ArrowSerializer::dataType_to_bytes(dataType.dataType_);
    };
    auto fromBytes = [&dataType](const std::vector<std::uint8_t> &bytes) {
      dataType.dataType_ = ArrowSerializer::bytes_to_dataType(bytes);
      return true;
    };
    return f.object(dataType).fields(f.field("dataType", toBytes, fromBytes));
  }
};

}

using DataTypePtr = std::shared_ptr<normal::tuple::DataType>;

CAF_BEGIN_TYPE_ID_BLOCK(DataType, normal::caf::CAFUtil::DataType_first_custom_type_id)
CAF_ADD_TYPE_ID(DataType, (normal::tuple::DataType))
CAF_END_TYPE_ID_BLOCK(DataType)

namespace caf {
template <>
struct inspector_access<DataTypePtr> : variant_inspector_access<DataTypePtr> {
  // nop
};
} // namespace caf


#endif //NORMAL_NORMAL_TUPLE_INCLUDE_NORMAL_TUPLE_DATATYPE_H
