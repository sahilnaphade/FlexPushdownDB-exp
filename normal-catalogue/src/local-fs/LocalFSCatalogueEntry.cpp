//
// Created by matt on 27/3/20.
//

#include <normal/catalogue/local-fs/LocalFSCatalogueEntry.h>
#include <normal/catalogue/CatalogueEntryType.h>
#include <fmt/format.h>
#include <utility>

namespace normal::catalogue::local_fs {

LocalFSCatalogueEntry::LocalFSCatalogueEntry(string schemaName,
                                             shared_ptr<Catalogue> catalogue) :
  CatalogueEntry(LocalFS,
                 move(schemaName),
                 move(catalogue)) {}

string LocalFSCatalogueEntry::getTypeName() const {
  return "LocalFSCatalogueEntry";
}

string LocalFSCatalogueEntry::getName() const {
  return fmt::format("local-fs://{}", getSchemaName());
}

}
