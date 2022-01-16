//
// Created by matt on 12/8/20.
//

#include <normal/executor/physical/file/FileScanKernel.h>

using namespace normal::executor::physical::file;

FileScanKernel::FileScanKernel(std::string path,
							   std::shared_ptr<FileReader> reader,
							   unsigned long startPos,
							   unsigned long finishPos) :
	path_(std::move(path)),
	reader_(std::move(reader)),
	startPos_(startPos),
	finishPos_(finishPos) {}

FileScanKernel FileScanKernel::make(const std::string &path,
                                    FileType fileType,
                                    unsigned long startPos,
                                    unsigned long finishPos) {

  auto reader = FileReaderBuilder::make(path, fileType);

  return {path, std::move(reader), startPos, finishPos};
}

tl::expected<std::shared_ptr<TupleSet>, std::string>
FileScanKernel::scan(const std::vector<std::string> &columnNames) {
  return reader_->read(columnNames, startPos_, finishPos_);
}

const std::string &FileScanKernel::getPath() const {
  return path_;
}

FileType FileScanKernel::getFileType() const {
  return reader_->getType();
}

unsigned long FileScanKernel::getStartPos() const {
  return startPos_;
}

unsigned long FileScanKernel::getFinishPos() const {
  return finishPos_;
}
