//
// Created by matt on 6/5/20.
//

#ifndef NORMAL_NORMAL_EXPRESSION_INCLUDE_NORMAL_EXPRESSION_FILTER_H
#define NORMAL_NORMAL_EXPRESSION_INCLUDE_NORMAL_EXPRESSION_FILTER_H

#include <normal/tuple/Schema.h>
#include <normal/tuple/TupleSet.h>
#include <tl/expected.hpp>
#include <memory>

namespace normal::expression {

class Filter {

public:
  virtual ~Filter() = default;

  virtual tl::expected<std::shared_ptr<normal::tuple::TupleSet>, std::string>
  evaluate(const normal::tuple::TupleSet &TupleSet) = 0;

  virtual tl::expected<arrow::ArrayVector, std::string> evaluate(const arrow::RecordBatch &recordBatch) = 0;

  virtual tl::expected<void, std::string> compile(const std::shared_ptr<normal::tuple::Schema> &Schema) = 0;

};

}

#endif //NORMAL_NORMAL_EXPRESSION_INCLUDE_NORMAL_EXPRESSION_FILTER_H
