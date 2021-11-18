//
// Created by Yifei Yang on 11/17/21.
//

#ifndef NORMAL_NORMAL_EXECUTOR_INCLUDE_NORMAL_EXECUTOR_PHYSICAL_GLOBALS_H
#define NORMAL_NORMAL_EXECUTOR_INCLUDE_NORMAL_EXECUTOR_PHYSICAL_GLOBALS_H

namespace normal::executor::physical {

/**
 * Default number of tuples operators should buffer before sending to consumers
 */
inline constexpr int DefaultBufferSize = 10000;

/**
 * Parameters used in WLFU, with csv_150MB/ and 200 parallel reqs
 */
// c5a.8x
inline constexpr double vNetwork = 1.16320;     // unit: GB/s
inline constexpr double vS3Scan = 18.00891;     // unit: GB/s
inline constexpr double vS3Filter = 0.32719;    // unit: GPred/s

}

#endif //NORMAL_NORMAL_EXECUTOR_INCLUDE_NORMAL_EXECUTOR_PHYSICAL_GLOBALS_H
