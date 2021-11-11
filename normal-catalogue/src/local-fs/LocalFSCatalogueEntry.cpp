//
// Created by matt on 27/3/20.
//

#include <normal/catalogue/local-fs/LocalFSCatalogueEntry.h>
#include <normal/catalogue/CatalogueEntryType.h>
#include <utility>

namespace normal::catalogue::local_fs {

LocalFSCatalogueEntry::LocalFSCatalogueEntry(const string &name,
                                             shared_ptr<Catalogue> catalogue,
                                             shared_ptr<format::Format> format) :
  CatalogueEntry(LocalFS, name, move(catalogue), move(format)) {}

}
