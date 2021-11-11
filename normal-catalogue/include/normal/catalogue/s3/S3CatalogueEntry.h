//
// Created by matt on 27/3/20.
//

#ifndef NORMAL_NORMAL_CATALOGUE_INCLUDE_NORMAL_CATALOGUE_S3_S3CATALOGUEENTRY_H
#define NORMAL_NORMAL_CATALOGUE_INCLUDE_NORMAL_CATALOGUE_S3_S3CATALOGUEENTRY_H

#include <normal/catalogue/CatalogueEntry.h>
#include <normal/catalogue/Catalogue.h>
#include <normal/catalogue/s3/S3Table.h>

#include <string>

using namespace normal::catalogue;
using namespace std;

namespace normal::catalogue::s3 {

class S3CatalogueEntry : public CatalogueEntry {

public:
  S3CatalogueEntry(const string &name,
                   string s3Bucket,
                   shared_ptr<Catalogue> catalogue,
                   shared_ptr<format::Format> format);
  ~S3CatalogueEntry() override = default;

  const string &getS3Bucket() const;

  void addS3Table(const shared_ptr<S3Table> &s3Table);

private:
  string s3Bucket_;
  unordered_map<string, shared_ptr<S3Table>> s3Tables_;
};

}

#endif //NORMAL_NORMAL_CATALOGUE_INCLUDE_NORMAL_CATALOGUE_S3_S3CATALOGUEENTRY_H
