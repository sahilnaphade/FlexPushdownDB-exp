//
// Created by matt on 15/4/20.
//

#include <normal/catalogue/local-fs/LocalFSPartition.h>
#include <utility>

namespace normal::catalogue::local_fs {

LocalFSPartition::LocalFSPartition(std::string path) :
        path_(std::move(path)) {}

const std::string &LocalFSPartition::getPath() const {
  return path_;
}

std::string LocalFSPartition::toString() {
  return "file://" + path_;
}

size_t LocalFSPartition::hash() {
  return std::hash<std::string>()("file://" + path_);
}

bool LocalFSPartition::equalTo(std::shared_ptr<Partition> other) {
  auto typedOther = std::static_pointer_cast<const LocalFSPartition>(other);
  if(!typedOther){
	return false;
  }
  else{
	return this->operator==(*typedOther);
  }
}

bool LocalFSPartition::operator==(const LocalFSPartition &other) {
  return path_ == other.path_;
}

}
