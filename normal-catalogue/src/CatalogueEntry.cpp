//
// Created by matt on 27/3/20.
//

#include <normal/catalogue/CatalogueEntry.h>

using namespace normal::catalogue;

CatalogueEntry::CatalogueEntry(CatalogueEntryType type,
                               string name,
                               shared_ptr<Catalogue> Catalogue) :
  type_(type),
  name_(move(name)),
  catalogue_(move(Catalogue)) {}

const string &CatalogueEntry::getName() const {
  return name_;
}

const shared_ptr<Catalogue> &CatalogueEntry::getCatalogue() const {
  return catalogue_;
}

CatalogueEntryType CatalogueEntry::getType() const {
  return type_;
}
