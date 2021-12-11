//
// Created by Yifei Yang on 12/10/21.
//

#ifndef NORMAL_NORMAL_EXPRESSION_GANDIVA_INCLUDE_NORMAL_EXPRESSION_GANDIVA_INDOUBLE_H
#define NORMAL_NORMAL_EXPRESSION_GANDIVA_INCLUDE_NORMAL_EXPRESSION_GANDIVA_INDOUBLE_H

#include "In.h"
#include <gandiva/tree_expr_builder.h>
#include <memory>
#include <string>
#include <unordered_set>

namespace normal::expression::gandiva {

class InDouble : public In {

public:
  InDouble(const shared_ptr<Expression> &left, const unordered_set<double> &values);

  void compile(const shared_ptr<arrow::Schema> &schema) override;

  string alias() override;

  string getTypeString() override;

private:
  unordered_set<double> values_;

};

shared_ptr<Expression> inDouble_(const shared_ptr<Expression> &left, const unordered_set<double> &values);

}


#endif //NORMAL_NORMAL_EXPRESSION_GANDIVA_INCLUDE_NORMAL_EXPRESSION_GANDIVA_INDOUBLE_H
