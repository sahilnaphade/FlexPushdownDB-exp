//
// Created by Yifei Yang on 1/16/22.
//

#ifndef NORMAL_NORMAL_CATALOGUE_INCLUDE_NORMAL_CATALOGUE_SERIALIZATION_TABLESERIALIZER_H
#define NORMAL_NORMAL_CATALOGUE_INCLUDE_NORMAL_CATALOGUE_SERIALIZATION_TABLESERIALIZER_H

#include <normal/catalogue/s3/S3Table.h>
#include <normal/catalogue/local-fs/LocalFSTable.h>
#include <normal/caf/CAFUtil.h>

using TablePtr = std::shared_ptr<normal::catalogue::Table>;

CAF_BEGIN_TYPE_ID_BLOCK(Table, normal::caf::CAFUtil::Table_first_custom_type_id)
CAF_ADD_TYPE_ID(Table, (TablePtr))
CAF_ADD_TYPE_ID(Table, (normal::catalogue::s3::S3Table))
CAF_ADD_TYPE_ID(Table, (normal::catalogue::local_fs::LocalFSTable))
CAF_END_TYPE_ID_BLOCK(Table)

namespace caf {

template<>
struct variant_inspector_traits<TablePtr> {
  using value_type = TablePtr;

  // Lists all allowed types and gives them a 0-based index.
  static constexpr type_id_t allowed_types[] = {
          type_id_v<none_t>,
          type_id_v<normal::catalogue::s3::S3Table>,
          type_id_v<normal::catalogue::local_fs::LocalFSTable>
  };

  // Returns which type in allowed_types corresponds to x.
  static auto type_index(const value_type &x) {
    if (!x)
      return 0;
    else if (x->getCatalogueEntryType() == normal::catalogue::S3)
      return 1;
    else if (x->getCatalogueEntryType() == normal::catalogue::LOCAL_FS)
      return 2;
    else
      return -1;
  }

  // Applies f to the value of x.
  template<class F>
  static auto visit(F &&f, const value_type &x) {
    switch (type_index(x)) {
      case 1:
        return f(dynamic_cast<normal::catalogue::s3::S3Table &>(*x));
      case 2:
        return f(dynamic_cast<normal::catalogue::local_fs::LocalFSTable &>(*x));
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
      case type_id_v<normal::catalogue::s3::S3Table>: {
        auto tmp = normal::catalogue::s3::S3Table{};
        continuation(tmp);
        return true;
      }
      case type_id_v<normal::catalogue::local_fs::LocalFSTable>: {
        auto tmp = normal::catalogue::local_fs::LocalFSTable{};
        continuation(tmp);
        return true;
      }
    }
  }
};

template <>
struct inspector_access<TablePtr> : variant_inspector_access<TablePtr> {
  // nop
};

} // namespace caf

#endif //NORMAL_NORMAL_CATALOGUE_INCLUDE_NORMAL_CATALOGUE_SERIALIZATION_TABLESERIALIZER_H
