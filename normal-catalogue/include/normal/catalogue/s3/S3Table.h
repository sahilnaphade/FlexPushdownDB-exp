//
// Created by Yifei Yang on 11/8/21.
//

#ifndef NORMAL_NORMAL_CATALOGUE_INCLUDE_NORMAL_CATALOGUE_S3_S3TABLE_H
#define NORMAL_NORMAL_CATALOGUE_INCLUDE_NORMAL_CATALOGUE_S3_S3TABLE_H

#include <normal/catalogue/Table.h>
#include <normal/catalogue/s3/S3Partition.h>
#include <normal/tuple/serialization/ArrowSerializer.h>
#include <vector>

using namespace std;

namespace normal::catalogue::s3 {

class S3Table: public Table {
public:
  S3Table(const string &name,
          const shared_ptr<arrow::Schema> &schema,
          const shared_ptr<format::Format> &format,
          const unordered_map<string, int> &apxColumnLengthMap,
          int apxRowLength,
          const unordered_set<string> &zonemapColumnNames,
          const vector<shared_ptr<S3Partition>> &s3Partitions);
  S3Table() = default;
  S3Table(const S3Table&) = default;
  S3Table& operator=(const S3Table&) = default;

  const vector<shared_ptr<S3Partition>> &getS3Partitions() const;

  CatalogueEntryType getCatalogueEntryType() override;

private:
  vector<shared_ptr<S3Partition>> s3Partitions_;

// caf inspect
public:
  template <class Inspector>
  friend bool inspect(Inspector& f, S3Table& table) {
    auto schemaToBytes = [&table]() -> decltype(auto) {
      return normal::tuple::ArrowSerializer::schema_to_bytes(table.schema_);
    };
    auto schemaFromBytes = [&table](const std::vector<std::uint8_t> &bytes) {
      table.schema_ = ArrowSerializer::bytes_to_schema(bytes);
      return true;
    };
    return f.object(table).fields(f.field("name", table.name_),
                                  f.field("schema", schemaToBytes, schemaFromBytes),
                                  f.field("format", table.format_),
                                  f.field("apxColumnLengthMap", table.apxColumnLengthMap_),
                                  f.field("apxRowLength", table.apxRowLength_),
                                  f.field("zonemapColumnNames", table.zonemapColumnNames_),
                                  f.field("s3Partitions", table.s3Partitions_));
  }
};

}


#endif //NORMAL_NORMAL_CATALOGUE_INCLUDE_NORMAL_CATALOGUE_S3_S3TABLE_H
