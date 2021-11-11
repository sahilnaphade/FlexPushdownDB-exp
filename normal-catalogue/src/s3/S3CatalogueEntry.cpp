//
// Created by matt on 27/3/20.
//

#include <normal/catalogue/s3/S3CatalogueEntry.h>
#include <normal/catalogue/CatalogueEntryType.h>

#include <utility>

using namespace normal::catalogue;

namespace normal::catalogue::s3 {

S3CatalogueEntry::S3CatalogueEntry(const string &name,
                                   string s3Bucket,
                                   shared_ptr<Catalogue> catalogue) :
  CatalogueEntry(S3, name, move(catalogue)),
  s3Bucket_(move(s3Bucket)) {}

const string &S3CatalogueEntry::getS3Bucket() const {
  return s3Bucket_;
}

void S3CatalogueEntry::addS3Table(const shared_ptr<S3Table> &s3Table) {
  s3Tables_.emplace(s3Table->getName(), s3Table);
}

}
