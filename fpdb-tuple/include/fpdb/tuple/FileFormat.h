//
// Created by Yifei Yang on 2/13/22.
//

#ifndef FPDB_FPDB_TUPLE_INCLUDE_FPDB_TUPLE_FILEFORMAT_H
#define FPDB_FPDB_TUPLE_INCLUDE_FPDB_TUPLE_FILEFORMAT_H

#include <fpdb/tuple/FileFormatType.h>

namespace fpdb::tuple {

class FileFormat {

public:
  FileFormat(FileFormatType type);
  FileFormat() = default;
  FileFormat(const FileFormat&) = default;
  FileFormat& operator=(const FileFormat&) = default;

  FileFormatType getType() const;

protected:
  FileFormatType type_;

};

}

#endif // FPDB_FPDB_TUPLE_INCLUDE_FPDB_TUPLE_FILEFORMAT_H
