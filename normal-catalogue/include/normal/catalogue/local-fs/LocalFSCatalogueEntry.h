//
// Created by matt on 27/3/20.
//

#ifndef NORMAL_NORMAL_SQL_INCLUDE_NORMAL_SQL_CONNECTOR_S3_S3SELECTCATALOGUEENTRY_H
#define NORMAL_NORMAL_SQL_INCLUDE_NORMAL_SQL_CONNECTOR_S3_S3SELECTCATALOGUEENTRY_H

#include <normal/catalogue/CatalogueEntry.h>
#include <normal/catalogue/Catalogue.h>

#include <string>

using namespace normal::catalogue;

namespace normal::catalogue::local_fs {

class LocalFSCatalogueEntry : public CatalogueEntry {

public:
  LocalFSCatalogueEntry(const std::string &name,
                   std::shared_ptr<Catalogue>);
  ~LocalFSCatalogueEntry() override = default;

};

}

#endif //NORMAL_NORMAL_SQL_INCLUDE_NORMAL_SQL_CONNECTOR_S3_S3SELECTCATALOGUEENTRY_H
