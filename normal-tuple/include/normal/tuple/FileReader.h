//
// Created by matt on 12/8/20.
//

#ifndef NORMAL_NORMAL_TUPLE_INCLUDE_NORMAL_TUPLE_FILEREADER_H
#define NORMAL_NORMAL_TUPLE_INCLUDE_NORMAL_TUPLE_FILEREADER_H

#include <normal/tuple/FileType.h>
#include <tl/expected.hpp>
#include <memory>

namespace normal::tuple {

class FileReader {
  
public:
  FileReader(FileType type);
  FileReader() = default;
  FileReader(const FileReader&) = default;
  FileReader& operator=(const FileReader&) = default;
  virtual ~FileReader() = default;

  FileType getType() const;

  virtual tl::expected<std::shared_ptr<TupleSet>, std::string>
  read(const std::vector<std::string> &columnNames, unsigned long startPos, unsigned long finishPos) = 0;

protected:
  FileType type_;

};

}

#endif //NORMAL_NORMAL_TUPLE_INCLUDE_NORMAL_TUPLE_FILEREADER_H
