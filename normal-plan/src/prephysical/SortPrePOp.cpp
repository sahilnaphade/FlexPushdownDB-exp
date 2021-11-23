//
// Created by Yifei Yang on 10/31/21.
//

#include <normal/plan/prephysical/SortPrePOp.h>
#include <normal/plan/prephysical/PrePOpType.h>

using namespace std;

namespace normal::plan::prephysical {

SortPrePOp::SortPrePOp(const vector<pair<string, FieldDirection>> &sortColumns) :
  PrePhysicalOp(SORT),
  sortColumns_(sortColumns) {}

string SortPrePOp::getTypeString() {
  return "SortPrePOp";
}

unordered_set<string> SortPrePOp::getUsedColumnNames() {
  return getProjectColumnNames();
}

}
