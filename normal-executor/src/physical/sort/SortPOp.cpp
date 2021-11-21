//
// Created by Yifei Yang on 11/20/21.
//

#include <normal/executor/physical/sort/SortPOp.h>

namespace normal::executor::physical::sort {

SortPOp::SortPOp(const std::string &name,
                 const std::vector<std::string> &projectColumnNames,
                 long queryId) :
  PhysicalOp(name, "Sort", projectColumnNames, queryId) {}

void SortPOp::onReceive(const Envelope &msg) {

}

}
