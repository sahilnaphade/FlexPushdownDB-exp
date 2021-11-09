//
// Created by matt on 27/3/20.
//

#include <normal/catalogue/Catalogue.h>

using namespace normal::catalogue;

Catalogue::Catalogue(std::string Name)
    : name_(std::move(Name)) {}

const std::string &Catalogue::getName() const {
  return name_;
}

void Catalogue::putEntry(const std::shared_ptr<CatalogueEntry>& entry) {
  this->entries_.emplace(entry->getName(), entry);
}

tl::expected<std::shared_ptr<CatalogueEntry>, std::string> Catalogue::getEntry(const std::string& name) {
  auto entryIterator = this->entries_.find(name);
  if(entryIterator == this->entries_.end()){
    return tl::unexpected("Catalogue entry '" + name + "' not found");
  }
  else{
    return entryIterator->second;
  }
}
