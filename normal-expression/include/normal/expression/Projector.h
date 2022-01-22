//
// Created by matt on 27/4/20.
//

#ifndef NORMAL_NORMAL_EXPRESSION_INCLUDE_NORMAL_EXPRESSION_PROJECTOR_H
#define NORMAL_NORMAL_EXPRESSION_INCLUDE_NORMAL_EXPRESSION_PROJECTOR_H

#include <arrow/type.h>
#include <arrow/array.h>
#include <normal/tuple/TupleSet.h>

using namespace normal::tuple;

namespace normal::expression {

class Projector {

public:
  virtual ~Projector() = default;

  virtual std::shared_ptr<arrow::Schema> getResultSchema() = 0;

  virtual tl::expected<std::shared_ptr<TupleSet>, std::string> evaluate(const TupleSet &tupleSet) = 0;
  virtual tl::expected<arrow::ArrayVector, std::string> evaluate(const arrow::RecordBatch &recordBatch) = 0;

  virtual tl::expected<void, std::string> compile(const std::shared_ptr<arrow::Schema> &schema) = 0;

};

}

#endif //NORMAL_NORMAL_EXPRESSION_INCLUDE_NORMAL_EXPRESSION_PROJECTOR_H
