//
// Created by Yifei Yang on 10/31/21.
//

#include <normal/plan/prephysical/PrePhysicalPlan.h>
#include <normal/plan/prephysical/AggregatePrePFunction.h>
#include <normal/plan/prephysical/FilterableScanPrePOp.h>
#include <normal/plan/prephysical/ProjectPrePOp.h>

namespace normal::plan::prephysical {

PrePhysicalPlan::PrePhysicalPlan(const shared_ptr<PrePhysicalOp> &rootOp,
                                 const vector<string> &outputColumnNames) :
  rootOp_(rootOp),
  outputColumnNames_(outputColumnNames) {}

const shared_ptr<PrePhysicalOp> &PrePhysicalPlan::getRootOp() const {
  return rootOp_;
}

const vector<string> &PrePhysicalPlan::getOutputColumnNames() const {
  return outputColumnNames_;
}

void PrePhysicalPlan::populateAndTrimProjectColumns() {
  populateProjectColumnsDfs(rootOp_);
  trimProjectColumnsDfs(rootOp_, nullopt);
}

set<string> PrePhysicalPlan::populateProjectColumnsDfs(const shared_ptr<PrePhysicalOp>& op) {
  // collect project columns of upstream ops
  set<string> upProjectColumns;
  for (const auto &producer: op->getProducers()) {
    set<string> producerProjectColumns = populateProjectColumnsDfs(producer);
    upProjectColumns.insert(producerProjectColumns.begin(), producerProjectColumns.end());
  }

  // set project columns if not set, and populate them to downstream ops
  const auto &projectColumnNames = op->getProjectColumnNames();
  if (projectColumnNames.empty()) {
    op->setProjectColumnNames(upProjectColumns);

    // for ProjectPrePOp, also need to update projectColumnNamePairs
    if (op->getType() == PROJECT) {
      const auto &projectPrePOp = static_pointer_cast<ProjectPrePOp>(op);
      projectPrePOp->updateProjectColumnNamePairs(upProjectColumns);
    }

    return upProjectColumns;
  } else {
    return projectColumnNames;
  }
}

void PrePhysicalPlan::trimProjectColumnsDfs(const shared_ptr<PrePhysicalOp>& op,
                                            const optional<set<string>> &optDownUsedColumns) {
  // process used columns of downstream ops
  auto projectColumns = op->getProjectColumnNames();

  if (optDownUsedColumns.has_value()) {
    const auto &downUsedColumns = optDownUsedColumns.value();

    // exclude unused columns, need to check whether all current projectColumnNames are needed, e.g. count(*)
    if (downUsedColumns.find(AggregatePrePFunction::COUNT_STAR_COLUMN) == downUsedColumns.end()) {
      for (auto it = projectColumns.begin(); it != projectColumns.end();) {

        // check if it's dummy column used by ProjectPrePOp
        if (it->find(ProjectPrePOp::DUMMY_COLUMN_PREFIX) == 0) {
          ++it;
          continue;
        }

        if (downUsedColumns.find(*it) == downUsedColumns.end()) {
          it = projectColumns.erase(it);
        } else {
          ++it;
        }
      }
    }

    // scan operator shouldn't scan no columns
    if (projectColumns.empty() && op->getType() == FILTERABLE_SCAN) {
      const auto &filterableScanPrePOp = static_pointer_cast<FilterableScanPrePOp>(op);
      projectColumns = {filterableScanPrePOp->getTable()->getColumnNames()[0]};
    }

    // for ProjectPrePOp, also need to update projectColumnNamePairs
    if (op->getType() == PROJECT) {
      const auto &projectPrePOp = static_pointer_cast<ProjectPrePOp>(op);
      projectPrePOp->updateProjectColumnNamePairs(projectColumns);
    }

    // set it finally
    op->setProjectColumnNames(projectColumns);
  }

  // populate self's used columns to upstream ops
  const auto &usedColumns = op->getUsedColumnNames();
  for (const auto &producer: op->getProducers()) {
    trimProjectColumnsDfs(producer, usedColumns);
  }
}

}
