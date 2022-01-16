//
// Created by Yifei Yang on 1/15/22.
//

#ifndef NORMAL_NORMAL_EXECUTOR_INCLUDE_NORMAL_EXECUTOR_SERIALIZATION_AGGREGATEFUNCTIONSERIALIZER_H
#define NORMAL_NORMAL_EXECUTOR_INCLUDE_NORMAL_EXECUTOR_SERIALIZATION_AGGREGATEFUNCTIONSERIALIZER_H

#include <normal/executor/physical/aggregate/function/AggregateFunction.h>
#include <normal/executor/physical/aggregate/function/Count.h>
#include <normal/executor/physical/aggregate/function/MinMax.h>
#include <normal/executor/physical/aggregate/function/Sum.h>
#include <normal/caf/CAFUtil.h>

using namespace normal::executor::physical::aggregate;
using AggregationFunctionPtr = std::shared_ptr<AggregateFunction>;

CAF_BEGIN_TYPE_ID_BLOCK(AggregateFunction, normal::caf::CAFUtil::AggregateFunction_first_custom_type_id)
CAF_ADD_TYPE_ID(AggregateFunction, (AggregationFunctionPtr))
CAF_ADD_TYPE_ID(AggregateFunction, (Count))
CAF_ADD_TYPE_ID(AggregateFunction, (MinMax))
CAF_ADD_TYPE_ID(AggregateFunction, (Sum))
CAF_END_TYPE_ID_BLOCK(AggregateFunction)

// Variant-based approach on AggregationFunctionPtr
namespace caf {

template<>
struct variant_inspector_traits<AggregationFunctionPtr> {
  using value_type = AggregationFunctionPtr;

  // Lists all allowed types and gives them a 0-based index.
  static constexpr type_id_t allowed_types[] = {
          type_id_v<none_t>,
          type_id_v<Count>,
          type_id_v<MinMax>,
          type_id_v<Sum>
  };

  // Returns which type in allowed_types corresponds to x.
  static auto type_index(const value_type &x) {
    if (!x)
      return 0;
    else if (x->getType() == COUNT)
      return 1;
    else if (x->getType() == MIN_MAX)
      return 2;
    else if (x->getType() == SUM)
      return 3;
    else return -1;
  }

  // Applies f to the value of x.
  template<class F>
  static auto visit(F &&f, const value_type &x) {
    switch (type_index(x)) {
      case 1:
        return f(dynamic_cast<Count &>(*x));
      case 2:
        return f(dynamic_cast<MinMax &>(*x));
      case 3:
        return f(dynamic_cast<Sum &>(*x));
      default: {
        none_t dummy;
        return f(dummy);
      }
    }
  }

  // Assigns a value to x.
  template<class U>
  static void assign(value_type &x, U value) {
    if constexpr (std::is_same<U, none_t>::value)
      x.reset();
    else
      x = std::make_shared<U>(std::move(value));
  }

  // Create a default-constructed object for `type` and then call the
  // continuation with the temporary object to perform remaining load steps.
  template<class F>
  static bool load(type_id_t type, F continuation) {
    switch (type) {
      default:
        return false;
      case type_id_v<none_t>: {
        none_t dummy;
        continuation(dummy);
        return true;
      }
      case type_id_v<Count>: {
        auto tmp = Count{};
        continuation(tmp);
        return true;
      }
      case type_id_v<MinMax>: {
        auto tmp = MinMax{};
        continuation(tmp);
        return true;
      }
      case type_id_v<Sum>: {
        auto tmp = Sum{};
        continuation(tmp);
        return true;
      }
    }
  }
};

template <>
struct inspector_access<AggregationFunctionPtr> : variant_inspector_access<AggregationFunctionPtr> {
  // nop
};

} // namespace caf

#endif //NORMAL_NORMAL_EXECUTOR_INCLUDE_NORMAL_EXECUTOR_SERIALIZATION_AGGREGATEFUNCTIONSERIALIZER_H
