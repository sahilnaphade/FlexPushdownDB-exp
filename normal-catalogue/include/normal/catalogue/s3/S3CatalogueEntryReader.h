//
// Created by Yifei Yang on 11/9/21.
//

#ifndef NORMAL_NORMAL_CATALOGUE_INCLUDE_NORMAL_CATALOGUE_S3_S3CATALOGUEENTRYREADER_H
#define NORMAL_NORMAL_CATALOGUE_INCLUDE_NORMAL_CATALOGUE_S3_S3CATALOGUEENTRYREADER_H

#include <normal/catalogue/s3/S3CatalogueEntry.h>
#include <normal/catalogue/Catalogue.h>

using namespace std;

namespace normal::catalogue::s3 {

class S3CatalogueEntryReader {
public:
  static shared_ptr<S3CatalogueEntry> readS3CatalogueEntry(const string &s3Bucket,
                                                           const string &schemaName,
                                                           const shared_ptr<Catalogue> &catalogue);
};

}


#endif //NORMAL_NORMAL_CATALOGUE_INCLUDE_NORMAL_CATALOGUE_S3_S3CATALOGUEENTRYREADER_H
