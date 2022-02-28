//
// Created by Yifei Yang on 1/14/22.
//

#ifndef FPDB_FPDB_EXECUTOR_INCLUDE_FPDB_EXECUTOR_CAFSERIALIZATION_CAFPOPSERIALIZER_H
#define FPDB_FPDB_EXECUTOR_INCLUDE_FPDB_EXECUTOR_CAFSERIALIZATION_CAFPOPSERIALIZER_H

#include <fpdb/executor/physical/PhysicalOp.h>
#include <fpdb/executor/physical/aggregate/AggregatePOp.h>
#include <fpdb/executor/physical/cache/CacheLoadPOp.h>
#include <fpdb/executor/physical/collate/CollatePOp.h>
#include <fpdb/executor/physical/file/LocalFileScanPOp.h>
#include <fpdb/executor/physical/file/RemoteFileScanPOp.h>
#include <fpdb/executor/physical/filter/FilterPOp.h>
#include <fpdb/executor/physical/group/GroupPOp.h>
#include <fpdb/executor/physical/join/hashjoin/HashJoinBuildPOp.h>
#include <fpdb/executor/physical/join/hashjoin/HashJoinProbePOp.h>
#include <fpdb/executor/physical/join/nestedloopjoin/NestedLoopJoinPOp.h>
#include <fpdb/executor/physical/limitsort/LimitSortPOp.h>
#include <fpdb/executor/physical/merge/MergePOp.h>
#include <fpdb/executor/physical/project/ProjectPOp.h>
#include <fpdb/executor/physical/s3/S3GetPOp.h>
#include <fpdb/executor/physical/s3/S3SelectPOp.h>
#include <fpdb/executor/physical/shuffle/ShufflePOp.h>
#include <fpdb/executor/physical/sort/SortPOp.h>
#include <fpdb/executor/physical/split/SplitPOp.h>
#include <fpdb/executor/physical/store/StoreFileScanPOp.h>
#include <fpdb/executor/physical/store/StoreSuperPOp.h>
#include <fpdb/tuple/serialization/ArrowSerializer.h>
#include <fpdb/caf/CAFUtil.h>

using namespace fpdb::executor::physical;

using POpPtr = std::shared_ptr<PhysicalOp>;

CAF_BEGIN_TYPE_ID_BLOCK(POp, fpdb::caf::CAFUtil::POp_first_custom_type_id)
CAF_ADD_TYPE_ID(POp, (POpPtr))
CAF_ADD_TYPE_ID(POp, (aggregate::AggregatePOp))
CAF_ADD_TYPE_ID(POp, (cache::CacheLoadPOp))
CAF_ADD_TYPE_ID(POp, (fpdb::executor::physical::collate::CollatePOp))
CAF_ADD_TYPE_ID(POp, (file::LocalFileScanPOp))
CAF_ADD_TYPE_ID(POp, (file::RemoteFileScanPOp))
CAF_ADD_TYPE_ID(POp, (filter::FilterPOp))
CAF_ADD_TYPE_ID(POp, (fpdb::executor::physical::group::GroupPOp))
CAF_ADD_TYPE_ID(POp, (join::HashJoinBuildPOp))
CAF_ADD_TYPE_ID(POp, (join::HashJoinProbePOp))
CAF_ADD_TYPE_ID(POp, (join::NestedLoopJoinPOp))
CAF_ADD_TYPE_ID(POp, (limitsort::LimitSortPOp))
CAF_ADD_TYPE_ID(POp, (merge::MergePOp))
CAF_ADD_TYPE_ID(POp, (project::ProjectPOp))
CAF_ADD_TYPE_ID(POp, (fpdb::executor::physical::s3::S3GetPOp))
CAF_ADD_TYPE_ID(POp, (fpdb::executor::physical::s3::S3SelectPOp))
CAF_ADD_TYPE_ID(POp, (shuffle::ShufflePOp))
CAF_ADD_TYPE_ID(POp, (sort::SortPOp))
CAF_ADD_TYPE_ID(POp, (split::SplitPOp))
CAF_ADD_TYPE_ID(POp, (store::StoreFileScanPOp))
CAF_ADD_TYPE_ID(POp, (store::StoreSuperPOp))
CAF_ADD_TYPE_ID(POp, (caf::spawn_options))
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
          type_id_v<fpdb::executor::physical::collate::CollatePOp>,
          type_id_v<file::LocalFileScanPOp>,
          type_id_v<file::RemoteFileScanPOp>,
          type_id_v<filter::FilterPOp>,
          type_id_v<fpdb::executor::physical::group::GroupPOp>,
          type_id_v<join::HashJoinBuildPOp>,
          type_id_v<join::HashJoinProbePOp>,
          type_id_v<join::NestedLoopJoinPOp>,
          type_id_v<limitsort::LimitSortPOp>,
          type_id_v<merge::MergePOp>,
          type_id_v<project::ProjectPOp>,
          type_id_v<fpdb::executor::physical::s3::S3GetPOp>,
          type_id_v<fpdb::executor::physical::s3::S3SelectPOp>,
          type_id_v<shuffle::ShufflePOp>,
          type_id_v<sort::SortPOp>,
          type_id_v<split::SplitPOp>,
          type_id_v<store::StoreFileScanPOp>,
          type_id_v<store::StoreSuperPOp>
  };

  // Returns which type in allowed_types corresponds to x.
  static auto type_index(const value_type &x) {
    if (!x)
      return 0;
    else if (x->getType() == POpType::AGGREGATE)
      return 1;
    else if (x->getType() == POpType::CACHE_LOAD)
      return 2;
    else if (x->getType() == POpType::COLLATE)
      return 3;
    else if (x->getType() == POpType::LOCAL_FILE_SCAN)
      return 4;
    else if (x->getType() == POpType::REMOTE_FILE_SCAN)
      return 5;
    else if (x->getType() == POpType::FILTER)
      return 6;
    else if (x->getType() == POpType::GROUP)
      return 7;
    else if (x->getType() == POpType::HASH_JOIN_BUILD)
      return 8;
    else if (x->getType() == POpType::HASH_JOIN_PROBE)
      return 9;
    else if (x->getType() == POpType::NESTED_LOOP_JOIN)
      return 10;
    else if (x->getType() == POpType::LIMIT_SORT)
      return 11;
    else if (x->getType() == POpType::MERGE)
      return 12;
    else if (x->getType() == POpType::PROJECT)
      return 13;
    else if (x->getType() == POpType::S3_GET)
      return 14;
    else if (x->getType() == POpType::S3_SELECT)
      return 15;
    else if (x->getType() == POpType::SHUFFLE)
      return 16;
    else if (x->getType() == POpType::SORT)
      return 17;
    else if (x->getType() == POpType::SPLIT)
      return 18;
    else if (x->getType() == POpType::STORE_FILE_SCAN)
      return 19;
    else if (x->getType() == POpType::STORE_SUPER)
      return 20;
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
        return f(dynamic_cast<fpdb::executor::physical::collate::CollatePOp &>(*x));
      case 4:
        return f(dynamic_cast<file::LocalFileScanPOp &>(*x));
      case 5:
        return f(dynamic_cast<file::RemoteFileScanPOp &>(*x));
      case 6:
        return f(dynamic_cast<filter::FilterPOp &>(*x));
      case 7:
        return f(dynamic_cast<fpdb::executor::physical::group::GroupPOp &>(*x));
      case 8:
        return f(dynamic_cast<join::HashJoinBuildPOp &>(*x));
      case 9:
        return f(dynamic_cast<join::HashJoinProbePOp &>(*x));
      case 10:
        return f(dynamic_cast<join::NestedLoopJoinPOp &>(*x));
      case 11:
        return f(dynamic_cast<limitsort::LimitSortPOp &>(*x));
      case 12:
        return f(dynamic_cast<merge::MergePOp &>(*x));
      case 13:
        return f(dynamic_cast<project::ProjectPOp &>(*x));
      case 14:
        return f(dynamic_cast<fpdb::executor::physical::s3::S3GetPOp &>(*x));
      case 15:
        return f(dynamic_cast<fpdb::executor::physical::s3::S3SelectPOp &>(*x));
      case 16:
        return f(dynamic_cast<shuffle::ShufflePOp &>(*x));
      case 17:
        return f(dynamic_cast<sort::SortPOp &>(*x));
      case 18:
        return f(dynamic_cast<split::SplitPOp &>(*x));
      case 19:
        return f(dynamic_cast<store::StoreFileScanPOp &>(*x));
      case 20:
        return f(dynamic_cast<store::StoreSuperPOp &>(*x));
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
      case type_id_v<fpdb::executor::physical::collate::CollatePOp>: {
        auto tmp = fpdb::executor::physical::collate::CollatePOp{};
        continuation(tmp);
        return true;
      }
      case type_id_v<file::LocalFileScanPOp>: {
        auto tmp = file::LocalFileScanPOp{};
        continuation(tmp);
        return true;
      }
      case type_id_v<file::RemoteFileScanPOp>: {
        auto tmp = file::RemoteFileScanPOp{};
        continuation(tmp);
        return true;
      }
      case type_id_v<filter::FilterPOp>: {
        auto tmp = filter::FilterPOp{};
        continuation(tmp);
        return true;
      }
      case type_id_v<fpdb::executor::physical::group::GroupPOp>: {
        auto tmp = fpdb::executor::physical::group::GroupPOp{};
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
      case type_id_v<fpdb::executor::physical::s3::S3GetPOp>: {
        auto tmp = fpdb::executor::physical::s3::S3GetPOp{};
        continuation(tmp);
        return true;
      }
      case type_id_v<fpdb::executor::physical::s3::S3SelectPOp>: {
        auto tmp = fpdb::executor::physical::s3::S3SelectPOp{};
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
      case type_id_v<store::StoreFileScanPOp>: {
        auto tmp = store::StoreFileScanPOp{};
        continuation(tmp);
        return true;
      }
      case type_id_v<store::StoreSuperPOp>: {
        auto tmp = store::StoreSuperPOp{};
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

#endif //FPDB_FPDB_EXECUTOR_INCLUDE_FPDB_EXECUTOR_CAFSERIALIZATION_CAFPOPSERIALIZER_H
