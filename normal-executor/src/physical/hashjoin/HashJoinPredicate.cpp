//
// Created by matt on 29/4/20.
//

#include <normal/executor/physical/hashjoin/HashJoinPredicate.h>
#include <algorithm>
#include <utility>

using namespace normal::executor::physical::hashjoin;

HashJoinPredicate::HashJoinPredicate(std::string leftColumnName,
							 std::string rightColumnName) :
	leftColumnName_(std::move(leftColumnName)),
	rightColumnName_(std::move(rightColumnName)) {}

HashJoinPredicate HashJoinPredicate::create(const std::string &leftColumnName, const std::string &rightColumnName) {

  std::string l(leftColumnName);
  std::string r(rightColumnName);

  std::transform(l.begin(), l.end(), l.begin(), ::tolower);
  std::transform(r.begin(), r.end(), r.begin(), ::tolower);
  return HashJoinPredicate(l, r);
}

const std::string &HashJoinPredicate::getLeftColumnName() const {
  return leftColumnName_;
}

const std::string &HashJoinPredicate::getRightColumnName() const {
  return rightColumnName_;
}
