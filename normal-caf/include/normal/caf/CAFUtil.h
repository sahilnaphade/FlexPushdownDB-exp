//
// Created by Yifei Yang on 1/12/22.
//

#ifndef NORMAL_NORMAL_CAF_INCLUDE_NORMAL_CAF_CAFUTIL_H
#define NORMAL_NORMAL_CAF_INCLUDE_NORMAL_CAF_CAFUTIL_H

#include <caf/all.hpp>

namespace normal::caf {

class CAFUtil {

public:
  inline static constexpr ::caf::type_id_t SegmentCacheActor_first_custom_type_id = ::caf::first_custom_type_id;
  inline static constexpr ::caf::type_id_t Envelope_first_custom_type_id = ::caf::first_custom_type_id + 100;
  inline static constexpr ::caf::type_id_t POpActor_first_custom_type_id = ::caf::first_custom_type_id + 200;
  inline static constexpr ::caf::type_id_t POpActor2_first_custom_type_id = ::caf::first_custom_type_id + 300;
  inline static constexpr ::caf::type_id_t CollatePOp2_first_custom_type_id = ::caf::first_custom_type_id + 400;
  inline static constexpr ::caf::type_id_t FileScanPOp2_first_custom_type_id = ::caf::first_custom_type_id + 500;
  inline static constexpr ::caf::type_id_t TupleSet_first_custom_type_id = ::caf::first_custom_type_id + 600;
  inline static constexpr ::caf::type_id_t Message_first_custom_type_id = ::caf::first_custom_type_id + 700;

};

}

// A template to serialize any shared_ptr
namespace caf {
template <class T>
struct variant_inspector_traits<std::shared_ptr<T>> {
  using value_type = std::shared_ptr<T>;

  // Lists all allowed types and gives them a 0-based index.
  static constexpr type_id_t allowed_types[] = {
          type_id_v<none_t>,
          type_id_v<T>,
  };

  // Returns which type in allowed_types corresponds to x.
  static auto type_index(const value_type& x) {
    if (!x)
      return 0;
    else
      return 1;
  }

  // Applies f to the value of x.
  template <class F>
  static auto visit(F&& f, const value_type& x) {
    switch (type_index(x)) {
      case 0: {
        none_t dummy;
        return f(dummy);
      }
      default:{
        auto a = f(static_cast<T&>(*x));
        return a;
      }
    }
  }

  // Assigns a value to x.
  template <class U>
  static void assign(value_type& x, U value) {
    if constexpr (std::is_same<U, none_t>::value)
      x.reset();
    else
      x = std::make_shared<U>(std::move(value));
  }

  // Create a default-constructed object for `type` and then call the
  // continuation with the temporary object to perform remaining load steps.
  template <class F>
  static bool load(type_id_t type, F continuation) {
    switch (type) {
      default:
        return false;
      case type_id_v<none_t>: {
        none_t dummy;
        continuation(dummy);
        return true;
      }
      case type_id_v<T>: {
        auto tmp = T{};
        continuation(tmp);
        return true;
      }
    }
  }
};
} // namespace caf


#endif //NORMAL_NORMAL_CAF_INCLUDE_NORMAL_CAF_CAFUTIL_H
