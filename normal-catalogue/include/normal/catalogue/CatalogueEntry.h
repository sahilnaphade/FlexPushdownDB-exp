//
// Created by matt on 27/3/20.
//

#ifndef NORMAL_NORMAL_CATALOGUE_INCLUDE_NORMAL_CATALOGUE_CATALOGUEENTRY_H
#define NORMAL_NORMAL_CATALOGUE_INCLUDE_NORMAL_CATALOGUE_CATALOGUEENTRY_H

#include <normal/catalogue/Catalogue.h>
#include <normal/catalogue/CatalogueEntryType.h>
#include <memory>
#include <string>

using namespace::std;

namespace normal::catalogue {

class Catalogue;

class CatalogueEntry {

public:
  explicit CatalogueEntry(CatalogueEntryType type,
                          string name,
                          shared_ptr<Catalogue> Catalogue);
  virtual ~CatalogueEntry() = default;

  const string &getName() const;
  const shared_ptr<Catalogue> &getCatalogue() const;
  CatalogueEntryType getType() const;
  virtual string getTypeName() const = 0;

private:
  CatalogueEntryType type_;
  string name_;
  shared_ptr<Catalogue> catalogue_;

};

}

#endif //NORMAL_NORMAL_CATALOGUE_INCLUDE_NORMAL_CATALOGUE_CATALOGUEENTRY_H
