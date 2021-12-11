//
// Created by Yifei Yang on 12/10/21.
//

#ifndef NORMAL_NORMAL_EXPRESSION_GANDIVA_INCLUDE_NORMAL_EXPRESSION_GANDIVA_INSTRING_H
#define NORMAL_NORMAL_EXPRESSION_GANDIVA_INCLUDE_NORMAL_EXPRESSION_GANDIVA_INSTRING_H

#include "In.h"
#include <gandiva/tree_expr_builder.h>
#include <memory>
#include <string>
#include <unordered_set>

namespace normal::expression::gandiva {

class InString : public In {

public:
  InString(const shared_ptr<Expression> &left, const unordered_set<string> &values);

  void compile(const shared_ptr<arrow::Schema> &schema) override;

  string alias() override;

  string getTypeString() override;

private:
  unordered_set<string> values_;

};

shared_ptr<Expression> inString_(const shared_ptr<Expression> &left, const unordered_set<string> &values);

}


#endif //NORMAL_NORMAL_EXPRESSION_GANDIVA_INCLUDE_NORMAL_EXPRESSION_GANDIVA_INSTRING_H
