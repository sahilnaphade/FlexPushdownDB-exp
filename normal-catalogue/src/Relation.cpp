//
// Created by Yifei Yang on 11/8/21.
//

#include <normal/catalogue/Relation.h>
#include <utility>

namespace normal::catalogue {

Relation::Relation(string name, const shared_ptr<CatalogueEntry> &catalogueEntry) :
  name_(std::move(name)),
  catalogueEntry_(catalogueEntry) {}
}
