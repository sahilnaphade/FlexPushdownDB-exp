//
// Created by Yifei Yang on 11/8/21.
//

#ifndef NORMAL_NORMAL_CATALOGUE_INCLUDE_NORMAL_CATALOGUE_RELATION_H
#define NORMAL_NORMAL_CATALOGUE_INCLUDE_NORMAL_CATALOGUE_RELATION_H

#include <normal/catalogue/CatalogueEntry.h>

using namespace std;

namespace normal::catalogue {

class CatalogueEntry;

class Relation {
public:
  Relation(string name, const shared_ptr<CatalogueEntry> &catalogueEntry);

private:
  string name_;
  shared_ptr<CatalogueEntry> catalogueEntry_;
};

}


#endif //NORMAL_NORMAL_CATALOGUE_INCLUDE_NORMAL_CATALOGUE_RELATION_H
