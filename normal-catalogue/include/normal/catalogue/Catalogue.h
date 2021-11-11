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
#include <filesystem>

using namespace std;

namespace normal::catalogue {

class CatalogueEntry;

class Catalogue {

public:
  explicit Catalogue(string Name, filesystem::path metadataPath);
  virtual ~Catalogue() = default;

  [[nodiscard]] const string &getName() const;
  tl::expected<shared_ptr<CatalogueEntry>, string> getEntry(const string &name);
  [[nodiscard]] filesystem::path getMetadataPath() const;

  void putEntry(const shared_ptr<CatalogueEntry> &entry);

private:
  string name_;
  unordered_map<string, shared_ptr<CatalogueEntry>> entries_;
  filesystem::path metadataPath_;

};

}

#endif //NORMAL_NORMAL_CATALOGUE_INCLUDE_NORMAL_CATALOGUE_CATALOGUE_H
