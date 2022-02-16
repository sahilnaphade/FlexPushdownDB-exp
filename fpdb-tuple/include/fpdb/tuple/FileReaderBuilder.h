//
// Created by matt on 12/8/20.
//

#ifndef FPDB_FPDB_TUPLE_INCLUDE_FPDB_TUPLE_FILEREADERBUILDER_H
#define FPDB_FPDB_TUPLE_INCLUDE_FPDB_TUPLE_FILEREADERBUILDER_H

#include <fpdb/tuple/FileReader.h>
#include <fpdb/tuple/FileFormat.h>
#include <string>

namespace fpdb::tuple {

class FileReaderBuilder {

public:
  static std::shared_ptr<FileReader> make(const std::string &path,
                                          const std::shared_ptr<FileFormat> &format,
                                          const std::shared_ptr<::arrow::Schema> &schema);

};

}

#endif //FPDB_FPDB_TUPLE_INCLUDE_FPDB_TUPLE_FILEREADERBUILDER_H
