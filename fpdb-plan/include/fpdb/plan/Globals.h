//
// Created by Yifei Yang on 4/12/23.
//

#ifndef FPDB_FPDB_PLAN_INCLUDE_FPDB_PLAN_GLOBALS_H
#define FPDB_FPDB_PLAN_INCLUDE_FPDB_PLAN_GLOBALS_H

namespace fpdb::plan {

/**
 * Pushdown parameters used by FPDB store (others are set in fpdb-executor)
 * Set by "pushdown.conf"
 */
inline bool ENABLE_CO_LOCATED_JOIN_PUSHDOWN;

/**
 * For predicate transfer.
 */
inline bool ENABLE_PRED_TRANS = false;

}

#endif //FPDB_FPDB_PLAN_INCLUDE_FPDB_PLAN_GLOBALS_H