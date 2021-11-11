//
// Created by matt on 27/3/20.
//

#ifndef NORMAL_NORMAL_CATALOGUE_INCLUDE_NORMAL_CATALOGUE_CATALOGUEENTRY_H
#define NORMAL_NORMAL_CATALOGUE_INCLUDE_NORMAL_CATALOGUE_CATALOGUEENTRY_H

#include <normal/catalogue/Catalogue.h>
#include <normal/catalogue/CatalogueEntryType.h>
#include <normal/catalogue/format/Format.h>
#include <memory>
#include <string>

using namespace::std;

namespace normal::catalogue {

class Catalogue;

class CatalogueEntry {

public:
  explicit CatalogueEntry(CatalogueEntryType type,
                          string name,
                          shared_ptr<Catalogue> Catalogue,
                          shared_ptr<format::Format> format);
  virtual ~CatalogueEntry() = default;

  [[nodiscard]] const string &getName() const;
  [[nodiscard]] const shared_ptr<Catalogue> &getCatalogue() const;
  [[nodiscard]] CatalogueEntryType getType() const;

private:
  CatalogueEntryType type_;
  string name_;
  shared_ptr<Catalogue> catalogue_;
  shared_ptr<format::Format> format_;

};

}

#endif //NORMAL_NORMAL_CATALOGUE_INCLUDE_NORMAL_CATALOGUE_CATALOGUEENTRY_H
