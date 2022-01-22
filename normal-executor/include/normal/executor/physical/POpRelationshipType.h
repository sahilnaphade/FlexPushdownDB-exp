//
// Created by matt on 25/3/20.
//

#ifndef NORMAL_NORMAL_EXECUTOR_INCLUDE_NORMAL_EXECUTOR_PHYSICAL_POPRELATIONSHIPTYPE_H
#define NORMAL_NORMAL_EXECUTOR_INCLUDE_NORMAL_EXECUTOR_PHYSICAL_POPRELATIONSHIPTYPE_H

namespace normal::executor::physical {

/**
 * Represents the relationships physical operators can have with each other, that is either producing or consuming
 */
enum class POpRelationshipType {
  Producer,
  Consumer
};

}

#endif //NORMAL_NORMAL_EXECUTOR_INCLUDE_NORMAL_EXECUTOR_PHYSICAL_POPRELATIONSHIPTYPE_H
