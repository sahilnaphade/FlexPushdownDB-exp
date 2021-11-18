//
// Created by matt on 29/4/20.
//

#ifndef NORMAL_NORMAL_EXECUTOR_INCLUDE_NORMAL_EXECUTOR_PHYSICAL_HASHJOIN_JOINPREDICATE_H
#define NORMAL_NORMAL_EXECUTOR_INCLUDE_NORMAL_EXECUTOR_PHYSICAL_HASHJOIN_JOINPREDICATE_H

#include <string>

namespace normal::executor::physical::hashjoin {

/**
 * A join predicate for straight column to column joins
 *
 * TODO: Support expressions
 */
class HashJoinPredicate {
  
public:
  HashJoinPredicate(std::string leftColumnName, std::string rightColumnName);
  const std::string &getLeftColumnName() const;
  const std::string &getRightColumnName() const;
  static HashJoinPredicate create(const std::string &leftColumnName, const std::string &rightColumnName);

private:
  std::string leftColumnName_;
  std::string rightColumnName_;

};

}

#endif //NORMAL_NORMAL_EXECUTOR_INCLUDE_NORMAL_EXECUTOR_PHYSICAL_HASHJOIN_JOINPREDICATE_H
