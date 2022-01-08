//
// Created by Yifei Yang on 11/7/21.
//

#ifndef NORMAL_NORMAL_PLAN_INCLUDE_NORMAL_PLAN_PREPHYSICAL_PROJECTPREPOP_H
#define NORMAL_NORMAL_PLAN_INCLUDE_NORMAL_PLAN_PREPHYSICAL_PROJECTPREPOP_H

#include <normal/plan/prephysical/PrePhysicalOp.h>
#include <normal/expression/gandiva/Expression.h>

using namespace normal::expression::gandiva;

namespace normal::plan::prephysical {

class ProjectPrePOp: public PrePhysicalOp {
public:
  ProjectPrePOp(uint id,
                const vector<shared_ptr<Expression>> &exprs,
                const vector<std::string> &exprNames,
                const vector<pair<string, string>> &projectColumnNamePairs);

  string getTypeString() override;

  set<string> getUsedColumnNames() override;

  const vector<shared_ptr<Expression>> &getExprs() const;
  const vector<std::string> &getExprNames() const;
  const vector<pair<string, string>> &getProjectColumnNamePairs() const;

  // override base implementation
  void setProjectColumnNames(const set<string> &projectColumnNames);

private:
  vector<shared_ptr<Expression>> exprs_;
  vector<string> exprNames_;
  vector<pair<string, string>> projectColumnNamePairs_;
};

}


#endif //NORMAL_NORMAL_PLAN_INCLUDE_NORMAL_PLAN_PREPHYSICAL_PROJECTPREPOP_H
