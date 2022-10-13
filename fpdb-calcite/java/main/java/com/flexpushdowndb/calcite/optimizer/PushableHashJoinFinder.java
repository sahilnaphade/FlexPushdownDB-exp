package com.flexpushdowndb.calcite.optimizer;

import com.flexpushdowndb.calcite.schema.TableImpl;
import org.apache.calcite.adapter.enumerable.EnumerableHashJoin;
import org.apache.calcite.prepare.RelOptTableImpl;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.metadata.RelColumnOrigin;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;

import java.util.*;

public final class PushableHashJoinFinder {
  public static Set<EnumerableHashJoin> find(RelNode plan, Map<String, String> hashKeys) {
    Set<EnumerableHashJoin> bottomHashJoins = findBottomHashJoins(plan);
    return findPushableHashJoins(bottomHashJoins, hashKeys);
  }

  private static Set<EnumerableHashJoin> findBottomHashJoins(RelNode relNode) {
    Set<EnumerableHashJoin> bottomHashJoins = new HashSet<>();
    for (RelNode input: relNode.getInputs()) {
      bottomHashJoins.addAll(findBottomHashJoins(input));
    }
    if (bottomHashJoins.isEmpty()) {
      if (relNode.getClass().getSimpleName().equals("EnumerableHashJoin")) {
        bottomHashJoins.add((EnumerableHashJoin) relNode);
      }
    }
    return bottomHashJoins;
  }

  private static Set<EnumerableHashJoin> findPushableHashJoins(Set<EnumerableHashJoin> bottomHashJoins,
                                                               Map<String, String> hashKeys) {
    Set<EnumerableHashJoin> pushableHashJoins = new HashSet<>();
    RelMetadataQuery mq = null;
    for (EnumerableHashJoin hashJoin: bottomHashJoins) {
      // set metadata query
      if (mq == null) {
        mq = hashJoin.getCluster().getMetadataQuery();
      }

      // check if co-located
      List<Map.Entry<Integer, Integer>> joinKeys = extractJoinKeys(hashJoin.getCondition());
      boolean colocated = false;
      for (Map.Entry<Integer, Integer> joinKey: joinKeys) {
        RelColumnOrigin leftColumnOrigin = mq.getColumnOrigin(hashJoin, joinKey.getKey());
        RelColumnOrigin rightColumnOrigin = mq.getColumnOrigin(hashJoin, joinKey.getValue());
        // check null
        if (leftColumnOrigin == null || rightColumnOrigin == null) {
          continue;
        }
        // check if column is derived
        if (leftColumnOrigin.isDerived() || rightColumnOrigin.isDerived()) {
          continue;
        }
        // check if table is co-located on the hash keys
        RelOptTableImpl leftTable = (RelOptTableImpl) leftColumnOrigin.getOriginTable();
        RelOptTableImpl rightTable = (RelOptTableImpl) rightColumnOrigin.getOriginTable();
        String leftTableName = ((TableImpl) leftTable.table()).getTableName();
        String rightTableName = ((TableImpl) rightTable.table()).getTableName();
        String leftJoinKey = leftTable.getRowType().getFieldNames().get(leftColumnOrigin.getOriginColumnOrdinal());
        String rightJoinKey = rightTable.getRowType().getFieldNames().get(rightColumnOrigin.getOriginColumnOrdinal());
        String leftHashKey = hashKeys.get(leftTableName);
        String rightHashKey = hashKeys.get(rightTableName);
        if (leftHashKey == null || rightHashKey == null) {
          continue;
        }
        if (leftJoinKey.equals(leftHashKey) && rightJoinKey.equals(rightHashKey)) {
          colocated = true;
          break;
        }
      }

      if (colocated) {
        pushableHashJoins.add(hashJoin);
      }
    }
    return pushableHashJoins;
  }

  private static List<Map.Entry<Integer, Integer>> extractJoinKeys(RexNode joinCondition) {
    List<Map.Entry<Integer, Integer>> joinKeys = new ArrayList<>();
    if (!(joinCondition instanceof RexCall)) {
      return joinKeys;
    }
    RexCall call = (RexCall) joinCondition;
    switch (call.getKind()) {
      case AND: {
        for (RexNode operand: call.getOperands()) {
          joinKeys.addAll(extractJoinKeys(operand));
        }
      }
      case EQUALS: {
        RexNode leftOperand = call.getOperands().get(0);
        RexNode rightOperand = call.getOperands().get(1);
        if (leftOperand instanceof RexInputRef && rightOperand instanceof RexInputRef) {
          return new ArrayList<>(Collections.singletonList(new AbstractMap.SimpleImmutableEntry<>(
                  ((RexInputRef) leftOperand).getIndex(),
                  ((RexInputRef) rightOperand).getIndex())));
        } else {
          return new ArrayList<>();
        }
      }
      default: {
        return new ArrayList<>();
      }
    }
  }
}
