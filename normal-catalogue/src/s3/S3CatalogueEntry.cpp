//
// Created by matt on 27/3/20.
//

#include <normal/catalogue/s3/S3CatalogueEntry.h>
#include <normal/catalogue/CatalogueEntryType.h>

#include <utility>

using namespace normal::catalogue;

namespace normal::catalogue::s3 {

S3CatalogueEntry::S3CatalogueEntry(const std::string &name,
                                   std::shared_ptr<Catalogue> catalogue,
                                   std::string s3Bucket) :
  CatalogueEntry(S3, name, std::move(catalogue)),
  s3Bucket_(std::move(s3Bucket)) {}

const std::string &S3CatalogueEntry::getS3Bucket() const {
  return s3Bucket_;
}

}
