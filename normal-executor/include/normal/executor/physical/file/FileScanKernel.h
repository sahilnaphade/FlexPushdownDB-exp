//
// Created by matt on 12/8/20.
//

#ifndef NORMAL_NORMAL_EXECUTOR_INCLUDE_NORMAL_EXECUTOR_PHYSICAL_FILE_FILESCANKERNEL_H
#define NORMAL_NORMAL_EXECUTOR_INCLUDE_NORMAL_EXECUTOR_PHYSICAL_FILE_FILESCANKERNEL_H

#include <normal/tuple/FileReader.h>
#include <normal/tuple/FileType.h>
#include <normal/tuple/TupleSet.h>
#include <normal/tuple/FileReaderBuilder.h>
#include <tl/expected.hpp>
#include <string>
#include <vector>
#include <memory>
#include <optional>

using namespace normal::tuple;

namespace normal::executor::physical::file {

class FileScanKernel {

public:
  FileScanKernel(std::string path,
				 std::shared_ptr<FileReader> reader,
				 unsigned long startPos,
				 unsigned long finishPos);
  FileScanKernel() = default;
  FileScanKernel(const FileScanKernel&) = default;
  FileScanKernel& operator=(const FileScanKernel&) = default;

  static FileScanKernel make(const std::string &path,
                             FileType fileType,
                             unsigned long startPos,
                             unsigned long finishPos);

  tl::expected<std::shared_ptr<TupleSet>, std::string> scan(const std::vector<std::string> &columnNames);

  [[nodiscard]] const std::string &getPath() const;
  [[nodiscard]] FileType getFileType() const;
  [[nodiscard]] unsigned long getStartPos() const;
  [[nodiscard]] unsigned long getFinishPos() const;

private:
  std::string path_;
  std::shared_ptr<FileReader> reader_;
  unsigned long startPos_;
  unsigned long finishPos_;

// caf inspect
public:
  template <class Inspector>
  friend bool inspect(Inspector& f, FileScanKernel& kernel) {
    return f.object(kernel).fields(f.field("path", kernel.path_),
                                   f.field("reader", kernel.reader_),
                                   f.field("startPos", kernel.startPos_),
                                   f.field("finishPos", kernel.finishPos_));
  }
};

}

#endif //NORMAL_NORMAL_EXECUTOR_INCLUDE_NORMAL_EXECUTOR_PHYSICAL_FILE_FILESCANKERNEL_H
