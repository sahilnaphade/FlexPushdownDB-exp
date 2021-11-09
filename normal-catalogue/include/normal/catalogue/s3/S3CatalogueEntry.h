//
// Created by matt on 27/3/20.
//

#ifndef NORMAL_NORMAL_SQL_INCLUDE_NORMAL_SQL_CONNECTOR_S3_S3SELECTCATALOGUEENTRY_H
#define NORMAL_NORMAL_SQL_INCLUDE_NORMAL_SQL_CONNECTOR_S3_S3SELECTCATALOGUEENTRY_H

#include <normal/catalogue/CatalogueEntry.h>
#include <normal/catalogue/Catalogue.h>

#include <string>

using namespace normal::catalogue;

namespace normal::catalogue::s3 {

class S3CatalogueEntry : public CatalogueEntry {

public:
  S3CatalogueEntry(const std::string &name,
                   std::shared_ptr<Catalogue>,
                   std::string s3Bucket);
  ~S3CatalogueEntry() override = default;

  const std::string &getS3Bucket() const;

private:
  std::string s3Bucket_;
};

}

#endif //NORMAL_NORMAL_SQL_INCLUDE_NORMAL_SQL_CONNECTOR_S3_S3SELECTCATALOGUEENTRY_H
