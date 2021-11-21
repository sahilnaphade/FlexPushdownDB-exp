//
// Created by Yifei Yang on 11/20/21.
//

#ifndef NORMAL_NORMAL_EXECUTOR_INCLUDE_NORMAL_EXECUTOR_PHYSICAL_SORT_SORTPOP_H
#define NORMAL_NORMAL_EXECUTOR_INCLUDE_NORMAL_EXECUTOR_PHYSICAL_SORT_SORTPOP_H

#include <normal/executor/physical/PhysicalOp.h>
#include <normal/executor/message/CompleteMessage.h>
#include <normal/executor/message/TupleMessage.h>

using namespace normal::executor::message;

namespace normal::executor::physical::sort {

class SortPOp : public PhysicalOp {

public:
  SortPOp(const std::string &name,
          const std::vector<std::string> &projectColumnNames,
          long queryId = 0);

  void onReceive(const Envelope &msg) override;

};

}


#endif //NORMAL_NORMAL_EXECUTOR_INCLUDE_NORMAL_EXECUTOR_PHYSICAL_SORT_SORTPOP_H
