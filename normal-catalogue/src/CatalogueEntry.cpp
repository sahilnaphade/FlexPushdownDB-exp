//
// Created by matt on 27/3/20.
//

#include <normal/catalogue/CatalogueEntry.h>

using namespace normal::catalogue;

CatalogueEntry::CatalogueEntry(CatalogueEntryType type,
                               string schemaName,
                               shared_ptr<Catalogue> Catalogue) :
  type_(type),
  schemaName_(move(schemaName)),
  catalogue_(move(Catalogue)) {}

const string &CatalogueEntry::getSchemaName() const {
  return schemaName_;
}

const shared_ptr<Catalogue> &CatalogueEntry::getCatalogue() const {
  return catalogue_;
}

CatalogueEntryType CatalogueEntry::getType() const {
  return type_;
}
