//
// Created by Yifei Yang on 11/7/21.
//

#include <normal/plan/prephysical/ProjectPrePOp.h>

namespace normal::plan::prephysical {
ProjectPrePOp::ProjectPrePOp(const vector<shared_ptr<Expression>> &exprs) :
  PrePhysicalOp(Project),
  exprs_(exprs) {}

string ProjectPrePOp::getName() {
  return "ProjectPrePOp";
}
}
