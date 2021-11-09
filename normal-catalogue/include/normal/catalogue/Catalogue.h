//
// Created by matt on 27/3/20.
//

#ifndef NORMAL_NORMAL_CATALOGUE_INCLUDE_NORMAL_CATALOGUE_CATALOGUE_H
#define NORMAL_NORMAL_CATALOGUE_INCLUDE_NORMAL_CATALOGUE_CATALOGUE_H

#include <normal/catalogue/CatalogueEntry.h>

#include <string>
#include <unordered_map>
#include <memory>
#include <tl/expected.hpp>

namespace normal::catalogue {

class CatalogueEntry;

class Catalogue {

public:
  explicit Catalogue(std::string Name);
  virtual ~Catalogue() = default;

  [[nodiscard]] const std::string &getName() const;
  tl::expected<std::shared_ptr<CatalogueEntry>, std::string> getEntry(const std::string &name);

  void putEntry(const std::shared_ptr<CatalogueEntry> &entry);

private:
  std::string name_;
  std::unordered_map<std::string, std::shared_ptr<CatalogueEntry>> entries_;

};

}

#endif //NORMAL_NORMAL_CATALOGUE_INCLUDE_NORMAL_CATALOGUE_CATALOGUE_H
