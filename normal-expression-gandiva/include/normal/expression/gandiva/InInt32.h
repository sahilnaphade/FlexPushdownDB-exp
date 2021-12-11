//
// Created by Yifei Yang on 12/10/21.
//

#ifndef NORMAL_NORMAL_EXPRESSION_GANDIVA_INCLUDE_NORMAL_EXPRESSION_GANDIVA_ININT32_H
#define NORMAL_NORMAL_EXPRESSION_GANDIVA_INCLUDE_NORMAL_EXPRESSION_GANDIVA_ININT32_H

#include "In.h"
#include <gandiva/tree_expr_builder.h>
#include <memory>
#include <string>
#include <unordered_set>

namespace normal::expression::gandiva {

class InInt32 : public In {
  
public:
  InInt32(const shared_ptr<Expression> &left, const unordered_set<int32_t> &values);

  void compile(const shared_ptr<arrow::Schema> &schema) override;

  string alias() override;

  string getTypeString() override;

private:
  unordered_set<int32_t> values_;
  
};

shared_ptr<Expression> inInt32_(const shared_ptr<Expression> &left, const unordered_set<int32_t> &values);

}


#endif //NORMAL_NORMAL_EXPRESSION_GANDIVA_INCLUDE_NORMAL_EXPRESSION_GANDIVA_ININT32_H
