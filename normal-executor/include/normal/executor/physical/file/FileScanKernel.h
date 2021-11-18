//
// Created by matt on 12/8/20.
//

#ifndef NORMAL_NORMAL_EXECUTOR_INCLUDE_NORMAL_EXECUTOR_PHYSICAL_FILE_FILESCANKERNEL_H
#define NORMAL_NORMAL_EXECUTOR_INCLUDE_NORMAL_EXECUTOR_PHYSICAL_FILE_FILESCANKERNEL_H

#include <normal/tuple/FileReader.h>
#include <normal/tuple/FileType.h>
#include <normal/tuple/TupleSet2.h>
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
				 FileType fileType,
				 std::shared_ptr<FileReader> reader,
				 unsigned long startPos,
				 unsigned long finishPos);

  static std::unique_ptr<FileScanKernel> make(const std::string &path,
											  FileType fileType,
											  unsigned long startPos,
											  unsigned long finishPos);

  tl::expected<std::shared_ptr<TupleSet2>, std::string> scan(const std::vector<std::string> &columnNames);

  [[nodiscard]] const std::string &getPath() const;
  [[nodiscard]] const std::optional<FileType> &getFileType() const;
  [[nodiscard]] unsigned long getStartPos() const;
  [[nodiscard]] unsigned long getFinishPos() const;

private:
  std::string path_;
  std::optional<FileType> fileType_;
  std::shared_ptr<FileReader> reader_;
  unsigned long startPos_;
  unsigned long finishPos_;

};

}

#endif //NORMAL_NORMAL_EXECUTOR_INCLUDE_NORMAL_EXECUTOR_PHYSICAL_FILE_FILESCANKERNEL_H
