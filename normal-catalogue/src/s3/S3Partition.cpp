//
// Created by matt on 15/4/20.
//

#include <normal/catalogue/s3/S3Partition.h>
#include <utility>

namespace normal::catalogue::s3 {

S3Partition::S3Partition(std::string s3Bucket, std::string s3Object) :
        s3Bucket_(std::move(s3Bucket)),
        s3Object_(std::move(s3Object)) {}

S3Partition::S3Partition(std::string s3Bucket, std::string s3Object, long numBytes) :
	s3Bucket_(std::move(s3Bucket)),
	s3Object_(std::move(s3Object)){
  setNumBytes(numBytes);
}

const std::string &S3Partition::getBucket() const {
  return s3Bucket_;
}

const std::string &S3Partition::getObject() const {
  return s3Object_;
}

std::string S3Partition::toString() {
  return "s3://" + s3Bucket_ + "/" + s3Object_;
}

size_t S3Partition::hash() {
  return std::hash<std::string>()("s3://" + s3Bucket_ + "/" + s3Object_);
}

bool S3Partition::equalTo(std::shared_ptr<Partition> other) {
  auto typedOther = std::static_pointer_cast<const S3Partition>(other);
  if(!typedOther){
	return false;
  }
  else{
	return this->operator==(*typedOther);
  }
}

bool S3Partition::operator==(const S3Partition &other) {
  return s3Bucket_ == other.s3Bucket_ && s3Object_ == other.s3Object_;
}

}
