//
// Created by Yifei Yang on 10/31/21.
//

#include <normal/plan/prephysical/PrePhysicalPlan.h>
#include <normal/plan/prephysical/AggregatePrePFunction.h>

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

    // check whether all current projectColumnNames are needed, e.g. count(*)
    if (downUsedColumns.find(AggregatePrePFunction::COUNT_STAR_COLUMN) == downUsedColumns.end()) {
      for (auto it = projectColumns.begin(); it != projectColumns.end();) {
        if (downUsedColumns.find(*it) == downUsedColumns.end()) {
          it = projectColumns.erase(it);
        } else {
          ++it;
        }
      }
      op->setProjectColumnNames(projectColumns);
    }
  }

  // populate self's used columns to upstream ops
  const auto &usedColumns = op->getUsedColumnNames();
  for (const auto &producer: op->getProducers()) {
    trimProjectColumnsDfs(producer, usedColumns);
  }
}

}
