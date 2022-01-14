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
                          string schemaName,
                          const shared_ptr<Catalogue>& Catalogue);
  virtual ~CatalogueEntry() = default;

  const string &getSchemaName() const;
  const weak_ptr<Catalogue> &getCatalogue() const;
  CatalogueEntryType getType() const;
  virtual string getTypeName() const = 0;
  virtual string getName() const = 0;

private:
  CatalogueEntryType type_;
  string schemaName_;
  weak_ptr<Catalogue> catalogue_;

};

}

#endif //NORMAL_NORMAL_CATALOGUE_INCLUDE_NORMAL_CATALOGUE_CATALOGUEENTRY_H
