//
// Created by Yifei Yang on 1/12/22.
//

#ifndef NORMAL_NORMAL_TUPLE_INCLUDE_NORMAL_TUPLE_SERIALIZATION_ARROWSERIALIZER_H
#define NORMAL_NORMAL_TUPLE_INCLUDE_NORMAL_TUPLE_SERIALIZATION_ARROWSERIALIZER_H

#include <arrow/api.h>
#include <arrow/compute/api.h>

namespace normal::tuple {

class ArrowSerializer {

public:
  /**
   * Serialization and deserialization methods of Table.
   */
  static std::shared_ptr<arrow::Table> bytes_to_table(const std::vector<std::uint8_t>& bytes_vec);
  static std::vector<std::uint8_t> table_to_bytes(const std::shared_ptr<arrow::Table>& table);

  /**
   * Serialization and deserialization methods of RecordBatch.
   */
  static std::shared_ptr<arrow::RecordBatch> bytes_to_recordBatch(const std::vector<std::uint8_t>& bytes_vec);
  static std::vector<std::uint8_t> recordBatch_to_bytes(const std::shared_ptr<arrow::RecordBatch>& recordBatch);

  /**
   * Serialization and deserialization methods of Schema.
   */
  static std::shared_ptr<arrow::Schema> bytes_to_schema(const std::vector<std::uint8_t>& bytes_vec);
  static std::vector<std::uint8_t> schema_to_bytes(const std::shared_ptr<arrow::Schema>& schema);

  /**
   * Serialization and deserialization methods of ChunkedArray.
   * As the primitive serialization unit of Arrow is RecordBatch, here we do indirectly by making a Table first
   */
  static std::shared_ptr<arrow::ChunkedArray> bytes_to_chunkedArray(const std::vector<std::uint8_t>& bytes_vec);
  static std::vector<std::uint8_t> chunkedArray_to_bytes(const std::shared_ptr<arrow::ChunkedArray>& chunkedArray);

  /**
   * Serialization and deserialization methods of Scalar
   * Do this by converting the scalar to a record batch
   */
  static std::shared_ptr<arrow::Scalar> bytes_to_scalar(const std::vector<std::uint8_t>& bytes_vec);
  static std::vector<std::uint8_t> scalar_to_bytes(const std::shared_ptr<arrow::Scalar>& scalar);

  /**
   * Serialization and deserialization methods of DataType
   * Do this in a straightforward way (using name()) instead of using schema serialization
   */
  static std::shared_ptr<arrow::DataType> bytes_to_dataType(const std::vector<std::uint8_t>& bytes_vec);
  static std::vector<std::uint8_t> dataType_to_bytes(const std::shared_ptr<arrow::DataType>& dataType);

  template <class Inspector>
  friend bool inspect(Inspector& f, arrow::compute::SelectKOptions& option) {
    return f.object(option).fields(f.field("k", option.k),
                                   f.field("sort_keys", option.sort_keys));
  }

  template <class Inspector>
  friend bool inspect(Inspector& f, arrow::compute::SortOptions& option) {
    return f.object(option).fields(f.field("sort_keys", option.sort_keys),
                                   f.field("null_placement", option.null_placement));
  }

  template <class Inspector>
  friend bool inspect(Inspector& f, arrow::compute::SortKey& sortKey) {
    return f.object(sortKey).fields(f.field("name", sortKey.name),
                                    f.field("order", sortKey.order));
  }
};

}


#endif //NORMAL_NORMAL_TUPLE_INCLUDE_NORMAL_TUPLE_SERIALIZATION_ARROWSERIALIZER_H
