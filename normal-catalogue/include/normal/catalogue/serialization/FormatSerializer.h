//
// Created by Yifei Yang on 1/16/22.
//

#ifndef NORMAL_NORMAL_CATALOGUE_INCLUDE_NORMAL_CATALOGUE_SERIALIZATION_FORMATSERIALIZER_H
#define NORMAL_NORMAL_CATALOGUE_INCLUDE_NORMAL_CATALOGUE_SERIALIZATION_FORMATSERIALIZER_H

#include <normal/catalogue/format/CSVFormat.h>
#include <normal/catalogue/format/ParquetFormat.h>
#include <normal/caf/CAFUtil.h>

using FormatPtr = std::shared_ptr<normal::catalogue::format::Format>;

CAF_BEGIN_TYPE_ID_BLOCK(Format, normal::caf::CAFUtil::Format_first_custom_type_id)
CAF_ADD_TYPE_ID(Format, (FormatPtr))
CAF_ADD_TYPE_ID(Format, (normal::catalogue::format::CSVFormat))
CAF_ADD_TYPE_ID(Format, (normal::catalogue::format::ParquetFormat))
CAF_END_TYPE_ID_BLOCK(Format)

namespace caf {

template<>
struct variant_inspector_traits<FormatPtr> {
  using value_type = FormatPtr;

  // Lists all allowed types and gives them a 0-based index.
  static constexpr type_id_t allowed_types[] = {
          type_id_v<none_t>,
          type_id_v<normal::catalogue::format::CSVFormat>,
          type_id_v<normal::catalogue::format::ParquetFormat>
  };

  // Returns which type in allowed_types corresponds to x.
  static auto type_index(const value_type &x) {
    if (!x)
      return 0;
    else if (x->getType() == normal::catalogue::format::CSV)
      return 1;
    else if (x->getType() == normal::catalogue::format::PARQUET)
      return 2;
    else
      return -1;
  }

  // Applies f to the value of x.
  template<class F>
  static auto visit(F &&f, const value_type &x) {
    switch (type_index(x)) {
      case 1:
        return f(static_cast<normal::catalogue::format::CSVFormat &>(*x));
      case 2:
        return f(static_cast<normal::catalogue::format::ParquetFormat &>(*x));
      default: {
        none_t dummy;
        return f(dummy);
      }
    }
  }

  // Assigns a value to x.
  template<class U>
  static void assign(value_type &x, U value) {
    if constexpr (std::is_same<U, none_t>::value)
      x.reset();
    else
      x = std::make_shared<U>(std::move(value));
  }

  // Create a default-constructed object for `type` and then call the
  // continuation with the temporary object to perform remaining load steps.
  template<class F>
  static bool load(type_id_t type, F continuation) {
    switch (type) {
      default:
        return false;
      case type_id_v<none_t>: {
        none_t dummy;
        continuation(dummy);
        return true;
      }
      case type_id_v<normal::catalogue::format::CSVFormat>: {
        auto tmp = normal::catalogue::format::CSVFormat{};
        continuation(tmp);
        return true;
      }
      case type_id_v<normal::catalogue::format::ParquetFormat>: {
        auto tmp = normal::catalogue::format::ParquetFormat{};
        continuation(tmp);
        return true;
      }
    }
  }
};

template <>
struct inspector_access<FormatPtr> : variant_inspector_access<FormatPtr> {
  // nop
};

} // namespace caf

#endif //NORMAL_NORMAL_CATALOGUE_INCLUDE_NORMAL_CATALOGUE_SERIALIZATION_FORMATSERIALIZER_H
