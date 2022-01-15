//
// Created by Yifei Yang on 1/14/22.
//

#ifndef NORMAL_NORMAL_EXECUTOR_INCLUDE_NORMAL_EXECUTOR_SERIALIZATION_POPSERIALIZER_H
#define NORMAL_NORMAL_EXECUTOR_INCLUDE_NORMAL_EXECUTOR_SERIALIZATION_POPSERIALIZER_H

#include <normal/executor/physical/PhysicalOp.h>
#include <normal/executor/physical/aggregate/AggregatePOp.h>
#include <normal/executor/physical/cache/CacheLoadPOp.h>
#include <normal/executor/physical/collate/CollatePOp.h>
#include <normal/executor/physical/file/FileScanPOp.h>
#include <normal/executor/physical/filter/FilterPOp.h>
#include <normal/executor/physical/group/GroupPOp.h>
#include <normal/executor/physical/join/hashjoin/HashJoinBuildPOp.h>
#include <normal/executor/physical/join/hashjoin/HashJoinProbePOp.h>
#include <normal/executor/physical/join/nestedloopjoin/NestedLoopJoinPOp.h>
#include <normal/executor/physical/limitsort/LimitSortPOp.h>
#include <normal/executor/physical/merge/MergePOp.h>
#include <normal/executor/physical/project/ProjectPOp.h>
#include <normal/executor/physical/s3/S3GetPOp.h>
#include <normal/executor/physical/s3/S3SelectPOp.h>
#include <normal/executor/physical/shuffle/ShufflePOp.h>
#include <normal/executor/physical/sort/SortPOp.h>
#include <normal/executor/physical/split/SplitPOp.h>
#include <normal/caf/CAFUtil.h>

using namespace normal::executor::physical;

using POpPtr = std::shared_ptr<PhysicalOp>;

CAF_BEGIN_TYPE_ID_BLOCK(POp, normal::caf::CAFUtil::POp_first_custom_type_id)
CAF_ADD_TYPE_ID(POp, (POpPtr))
CAF_ADD_TYPE_ID(POp, (aggregate::AggregatePOp))
CAF_ADD_TYPE_ID(POp, (cache::CacheLoadPOp))
CAF_ADD_TYPE_ID(POp, (normal::executor::physical::collate::CollatePOp))
CAF_ADD_TYPE_ID(POp, (file::FileScanPOp))
CAF_ADD_TYPE_ID(POp, (filter::FilterPOp))
CAF_ADD_TYPE_ID(POp, (group::GroupPOp))
CAF_ADD_TYPE_ID(POp, (join::HashJoinBuildPOp))
CAF_ADD_TYPE_ID(POp, (join::HashJoinProbePOp))
CAF_ADD_TYPE_ID(POp, (join::NestedLoopJoinPOp))
CAF_ADD_TYPE_ID(POp, (limitsort::LimitSortPOp))
CAF_ADD_TYPE_ID(POp, (merge::MergePOp))
CAF_ADD_TYPE_ID(POp, (project::ProjectPOp))
CAF_ADD_TYPE_ID(POp, (normal::executor::physical::s3::S3GetPOp))
CAF_ADD_TYPE_ID(POp, (normal::executor::physical::s3::S3SelectPOp))
CAF_ADD_TYPE_ID(POp, (shuffle::ShufflePOp))
CAF_ADD_TYPE_ID(POp, (sort::SortPOp))
CAF_ADD_TYPE_ID(POp, (split::SplitPOp))
CAF_END_TYPE_ID_BLOCK(POp)

// Variant-based approach on POpPtr
namespace caf {

template<>
struct variant_inspector_traits<POpPtr> {
  using value_type = POpPtr;

  // Lists all allowed types and gives them a 0-based index.
  static constexpr type_id_t allowed_types[] = {
          type_id_v<none_t>,
          type_id_v<aggregate::AggregatePOp>,
          type_id_v<cache::CacheLoadPOp>,
          type_id_v<normal::executor::physical::collate::CollatePOp>,
          type_id_v<file::FileScanPOp>,
          type_id_v<filter::FilterPOp>,
          type_id_v<normal::executor::physical::group::GroupPOp>,
          type_id_v<join::HashJoinBuildPOp>,
          type_id_v<join::HashJoinProbePOp>,
          type_id_v<join::NestedLoopJoinPOp>,
          type_id_v<limitsort::LimitSortPOp>,
          type_id_v<merge::MergePOp>,
          type_id_v<project::ProjectPOp>,
          type_id_v<normal::executor::physical::s3::S3GetPOp>,
          type_id_v<normal::executor::physical::s3::S3SelectPOp>,
          type_id_v<shuffle::ShufflePOp>,
          type_id_v<sort::SortPOp>,
          type_id_v<split::SplitPOp>
  };

  // Returns which type in allowed_types corresponds to x.
  static auto type_index(const value_type &x) {
    if (!x)
      return 0;
    else if (x->getType() == "AggregatePOp")
      return 1;
    else if (x->getType() == "CacheLoadPOp")
      return 2;
    else if (x->getType() == "CollatePOp")
      return 3;
    else if (x->getType() == "FileScanPOp")
      return 4;
    else if (x->getType() == "FilterPOp")
      return 5;
    else if (x->getType() == "GroupPOp")
      return 6;
    else if (x->getType() == "HashJoinBuildPOp")
      return 7;
    else if (x->getType() == "HashJoinProbePOp")
      return 8;
    else if (x->getType() == "NestedLoopJoinPOp")
      return 9;
    else if (x->getType() == "LimitSortPOp")
      return 10;
    else if (x->getType() == "MergePOp")
      return 11;
    else if (x->getType() == "ProjectPOp")
      return 12;
    else if (x->getType() == "S3GetPOp")
      return 13;
    else if (x->getType() == "S3SelectPOp")
      return 14;
    else if (x->getType() == "ShufflePOp")
      return 15;
    else if (x->getType() == "SortPOp")
      return 16;
    else if (x->getType() == "SplitPOp")
      return 17;
    else
      return -1;
  }

  // Applies f to the value of x.
  template<class F>
  static auto visit(F &&f, const value_type &x) {
    switch (type_index(x)) {
      case 1:
        return f(dynamic_cast<aggregate::AggregatePOp &>(*x));
      case 2:
        return f(dynamic_cast<cache::CacheLoadPOp &>(*x));
      case 3:
        return f(dynamic_cast<normal::executor::physical::collate::CollatePOp &>(*x));
      case 4:
        return f(dynamic_cast<file::FileScanPOp &>(*x));
      case 5:
        return f(dynamic_cast<filter::FilterPOp &>(*x));
      case 6:
        return f(dynamic_cast<normal::executor::physical::group::GroupPOp &>(*x));
      case 7:
        return f(dynamic_cast<join::HashJoinBuildPOp &>(*x));
      case 8:
        return f(dynamic_cast<join::HashJoinProbePOp &>(*x));
      case 9:
        return f(dynamic_cast<join::NestedLoopJoinPOp &>(*x));
      case 10:
        return f(dynamic_cast<limitsort::LimitSortPOp &>(*x));
      case 11:
        return f(dynamic_cast<merge::MergePOp &>(*x));
      case 12:
        return f(dynamic_cast<project::ProjectPOp &>(*x));
      case 13:
        return f(dynamic_cast<normal::executor::physical::s3::S3GetPOp &>(*x));
      case 14:
        return f(dynamic_cast<normal::executor::physical::s3::S3SelectPOp &>(*x));
      case 15:
        return f(dynamic_cast<shuffle::ShufflePOp &>(*x));
      case 16:
        return f(dynamic_cast<sort::SortPOp &>(*x));
      case 17:
        return f(dynamic_cast<split::SplitPOp &>(*x));
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
      case type_id_v<aggregate::AggregatePOp>: {
        auto tmp = aggregate::AggregatePOp{};
        continuation(tmp);
        return true;
      }
      case type_id_v<cache::CacheLoadPOp>: {
        auto tmp = cache::CacheLoadPOp{};
        continuation(tmp);
        return true;
      }
      case type_id_v<normal::executor::physical::collate::CollatePOp>: {
        auto tmp = normal::executor::physical::collate::CollatePOp{};
        continuation(tmp);
        return true;
      }
      case type_id_v<file::FileScanPOp>: {
        auto tmp = file::FileScanPOp{};
        continuation(tmp);
        return true;
      }
      case type_id_v<filter::FilterPOp>: {
        auto tmp = filter::FilterPOp{};
        continuation(tmp);
        return true;
      }
      case type_id_v<normal::executor::physical::group::GroupPOp>: {
        auto tmp = normal::executor::physical::group::GroupPOp{};
        continuation(tmp);
        return true;
      }
      case type_id_v<join::HashJoinBuildPOp>: {
        auto tmp = join::HashJoinBuildPOp{};
        continuation(tmp);
        return true;
      }
      case type_id_v<join::HashJoinProbePOp>: {
        auto tmp = join::HashJoinProbePOp{};
        continuation(tmp);
        return true;
      }
      case type_id_v<join::NestedLoopJoinPOp>: {
        auto tmp = join::NestedLoopJoinPOp{};
        continuation(tmp);
        return true;
      }
      case type_id_v<limitsort::LimitSortPOp>: {
        auto tmp = limitsort::LimitSortPOp{};
        continuation(tmp);
        return true;
      }
      case type_id_v<merge::MergePOp>: {
        auto tmp = merge::MergePOp{};
        continuation(tmp);
        return true;
      }
      case type_id_v<project::ProjectPOp>: {
        auto tmp = project::ProjectPOp{};
        continuation(tmp);
        return true;
      }
      case type_id_v<normal::executor::physical::s3::S3GetPOp>: {
        auto tmp = normal::executor::physical::s3::S3GetPOp{};
        continuation(tmp);
        return true;
      }
      case type_id_v<normal::executor::physical::s3::S3SelectPOp>: {
        auto tmp = normal::executor::physical::s3::S3SelectPOp{};
        continuation(tmp);
        return true;
      }
      case type_id_v<shuffle::ShufflePOp>: {
        auto tmp = shuffle::ShufflePOp{};
        continuation(tmp);
        return true;
      }
      case type_id_v<sort::SortPOp>: {
        auto tmp = sort::SortPOp{};
        continuation(tmp);
        return true;
      }
      case type_id_v<split::SplitPOp>: {
        auto tmp = split::SplitPOp{};
        continuation(tmp);
        return true;
      }
    }
  }
};

template <>
struct inspector_access<POpPtr> : variant_inspector_access<POpPtr> {
  // nop
};

} // namespace caf

#endif //NORMAL_NORMAL_EXECUTOR_INCLUDE_NORMAL_EXECUTOR_SERIALIZATION_POPSERIALIZER_H
