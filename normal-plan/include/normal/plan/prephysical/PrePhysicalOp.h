//
// Created by Yifei Yang on 10/31/21.
//

#ifndef NORMAL_NORMAL_PLAN_INCLUDE_NORMAL_PLAN_PREPHYSICALOP_PREPHYSICALOP_H
#define NORMAL_NORMAL_PLAN_INCLUDE_NORMAL_PLAN_PREPHYSICALOP_PREPHYSICALOP_H

#include <normal/plan/prephysical/PrePOpType.h>
#include <vector>
#include <unordered_set>
#include <string>

using namespace std;

namespace normal::plan::prephysical {

class PrePhysicalOp {
public:
  PrePhysicalOp(PrePOpType type);
  virtual ~PrePhysicalOp() = default;

  PrePOpType getType() const;
  virtual string getTypeString() = 0;
  const vector<shared_ptr<PrePhysicalOp>> &getProducers() const;
  const unordered_set<string> &getProjectColumnNames() const;
  virtual unordered_set<string> getUsedColumnNames() = 0;

  void setProducers(const vector<shared_ptr<PrePhysicalOp>> &producers);
  void setProjectColumnNames(const unordered_set<string> &projectColumnNames);

private:
  PrePOpType type_;
  vector<shared_ptr<PrePhysicalOp>> producers_;
  unordered_set<string> projectColumnNames_;
};

}


#endif //NORMAL_NORMAL_PLAN_INCLUDE_NORMAL_PLAN_PREPHYSICALOP_PREPHYSICALOP_H
