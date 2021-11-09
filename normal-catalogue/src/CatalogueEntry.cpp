//
// Created by matt on 27/3/20.
//

#include <normal/catalogue/CatalogueEntry.h>

using namespace normal::catalogue;

CatalogueEntry::CatalogueEntry(CatalogueEntryType type,
                               std::string name,
                               std::shared_ptr<Catalogue> Catalogue) :
  type_(type),
  name_(std::move(name)),
  catalogue_(std::move(Catalogue)) {}

const std::string &CatalogueEntry::getName() const {
  return name_;
}

const std::shared_ptr<Catalogue> &CatalogueEntry::getCatalogue() const {
  return catalogue_;
}

CatalogueEntryType CatalogueEntry::getType() const {
  return type_;
}
