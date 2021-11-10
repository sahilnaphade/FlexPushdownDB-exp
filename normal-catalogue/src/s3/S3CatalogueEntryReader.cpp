//
// Created by Yifei Yang on 11/9/21.
//

#include <normal/catalogue/s3/S3CatalogueEntryReader.h>

namespace normal::catalogue::s3 {

shared_ptr<S3CatalogueEntry>
S3CatalogueEntryReader::readS3CatalogueEntry(const string &s3Bucket,
                                             const string &schemaName,
                                             const shared_ptr<Catalogue> &catalogue) {
  return shared_ptr<S3CatalogueEntry>();
}
}
