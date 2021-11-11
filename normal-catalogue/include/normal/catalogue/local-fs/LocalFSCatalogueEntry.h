//
// Created by matt on 27/3/20.
//

#ifndef NORMAL_NORMAL_CATALOGUE_INCLUDE_NORMAL_CATALOGUE_LOCAL_FS_LOCALFSCATALOGUEENTRY_H
#define NORMAL_NORMAL_CATALOGUE_INCLUDE_NORMAL_CATALOGUE_LOCAL_FS_LOCALFSCATALOGUEENTRY_H

#include <normal/catalogue/CatalogueEntry.h>
#include <normal/catalogue/Catalogue.h>

#include <string>

using namespace normal::catalogue;
using namespace std;

namespace normal::catalogue::local_fs {

class LocalFSCatalogueEntry : public CatalogueEntry {

public:
  LocalFSCatalogueEntry(const string &name,
                        shared_ptr<Catalogue> catalogue,
                        shared_ptr<format::Format> format);
  ~LocalFSCatalogueEntry() override = default;

};

}

#endif //NORMAL_NORMAL_CATALOGUE_INCLUDE_NORMAL_CATALOGUE_LOCAL_FS_LOCALFSCATALOGUEENTRY_H
