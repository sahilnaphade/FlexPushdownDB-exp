//
// Created by matt on 23/7/20.
//

#include <memory>
#include <vector>

#include <doctest/doctest.h>

#include <normal/expression/gandiva/Column.h>
#include <normal/expression/gandiva/Cast.h>
#include <normal/expression/gandiva/Multiply.h>
#include <normal/expression/gandiva/Projector.h>
#include <normal/tuple/TupleSet2.h>

#include "Globals.h"
#include "TestUtil.h"

using namespace normal::tuple;
using namespace normal::expression::gandiva;
using namespace normal::expression::gandiva::test;

#define SKIP_SUITE true

TEST_SUITE ("multiply" * doctest::skip(SKIP_SUITE)) {

TEST_CASE ("simple" * doctest::skip(false || SKIP_SUITE)) {

  auto tuples = TestUtil::prepareTupleSet();
  auto inputTupleSet = normal::tuple::TupleSet2::create(tuples);

  SPDLOG_DEBUG("Input:\n{}", inputTupleSet->showString(TupleSetShowOptions(TupleSetShowOrientation::RowOriented)));

  auto expressions = std::vector<std::shared_ptr<normal::expression::gandiva::Expression>>{
	  times(cast(col("a"), arrow::float64()),
			cast(col("b"), arrow::float64()))
  };

  auto projector = std::make_shared<Projector>(expressions);
  projector->compile(tuples->table()->schema());

  SPDLOG_DEBUG("Projector:\n{}", projector->showString());

  auto evaluated = projector->evaluate(*tuples);
  auto evaluatedTupleSet = normal::tuple::TupleSet2::create(evaluated);
  SPDLOG_DEBUG("Output:\n{}", evaluatedTupleSet->showString(TupleSetShowOptions(TupleSetShowOrientation::RowOriented)));

	  CHECK_EQ(evaluatedTupleSet->numColumns(), 1);
	  CHECK_EQ(evaluatedTupleSet->numRows(), 3);

  auto column = evaluatedTupleSet->getColumnByIndex(0).value();
	  CHECK_EQ(column->element(0).value()->value<double>(), 4);
	  CHECK_EQ(column->element(1).value()->value<double>(), 10);
	  CHECK_EQ(column->element(2).value()->value<double>(), 18);
}

TEST_CASE ("empty" * doctest::skip(false || SKIP_SUITE)) {

  auto tuples = TestUtil::prepareEmptyTupleSet();
  auto inputTupleSet = normal::tuple::TupleSet2::create(tuples);

  SPDLOG_DEBUG("Input:\n{}", inputTupleSet->showString(TupleSetShowOptions(TupleSetShowOrientation::RowOriented)));

  auto expressions = std::vector<std::shared_ptr<normal::expression::gandiva::Expression>>{
	  times(cast(col("a"), arrow::float64()),
			cast(col("b"), arrow::float64()))
  };

  auto projector = std::make_shared<Projector>(expressions);
  CHECK_THROWS(projector->compile(tuples->table()->schema()));
}

TEST_CASE ("0-rows" * doctest::skip(false || SKIP_SUITE)) {

  auto tuples = TestUtil::prepare3x0TupleSet();
  auto inputTupleSet = normal::tuple::TupleSet2::create(tuples);

  SPDLOG_DEBUG("Input:\n{}", inputTupleSet->showString(TupleSetShowOptions(TupleSetShowOrientation::RowOriented)));

  auto expressions = std::vector<std::shared_ptr<normal::expression::gandiva::Expression>>{
	  times(cast(col("a"), arrow::float64()),
			cast(col("b"), arrow::float64()))
  };

  auto projector = std::make_shared<Projector>(expressions);
  projector->compile(tuples->table()->schema());

  SPDLOG_DEBUG("Projector:\n{}", projector->showString());

  auto evaluated = projector->evaluate(*tuples);
  auto evaluatedTupleSet = normal::tuple::TupleSet2::create(evaluated);
  SPDLOG_DEBUG("Output:\n{}", evaluatedTupleSet->showString(TupleSetShowOptions(TupleSetShowOrientation::RowOriented)));

	  CHECK_EQ(evaluatedTupleSet->numColumns(), 1);
	  CHECK_EQ(evaluatedTupleSet->numRows(), 0);
}

}
