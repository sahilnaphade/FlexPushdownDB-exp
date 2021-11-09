//
// Created by matt on 27/3/20.
//

#include <normal/catalogue/local-fs/LocalFSCatalogueEntry.h>
#include <normal/catalogue/CatalogueEntryType.h>
#include <utility>

using namespace normal::catalogue;

namespace normal::catalogue::local_fs {

LocalFSCatalogueEntry::LocalFSCatalogueEntry(const std::string &name,
                                   std::shared_ptr<Catalogue> catalogue) :
  CatalogueEntry(LocalFS, name, std::move(catalogue)) {}

}
