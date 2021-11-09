//
// Created by matt on 27/3/20.
//

#ifndef NORMAL_NORMAL_SQL_INCLUDE_NORMAL_SQL_CONNECTOR_CATALOGUEENTRY_H
#define NORMAL_NORMAL_SQL_INCLUDE_NORMAL_SQL_CONNECTOR_CATALOGUEENTRY_H

#include <normal/catalogue/Catalogue.h>
#include <normal/catalogue/CatalogueEntryType.h>
#include <memory>
#include <string>

namespace normal::catalogue {

class Catalogue;

class CatalogueEntry {

public:
  explicit CatalogueEntry(CatalogueEntryType type,
                          std::string name,
                          std::shared_ptr<Catalogue> Catalogue);
  virtual ~CatalogueEntry() = default;

  [[nodiscard]] const std::string &getName() const;
  [[nodiscard]] const std::shared_ptr<Catalogue> &getCatalogue() const;
  [[nodiscard]] CatalogueEntryType getType() const;

private:
  CatalogueEntryType type_;
  std::string name_;
  std::shared_ptr<Catalogue> catalogue_;

};

}

#endif //NORMAL_NORMAL_SQL_INCLUDE_NORMAL_SQL_CONNECTOR_CATALOGUEENTRY_H
