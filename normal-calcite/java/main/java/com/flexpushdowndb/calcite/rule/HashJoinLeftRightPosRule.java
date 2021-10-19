package com.flexpushdowndb.calcite.rule;

import org.apache.calcite.adapter.enumerable.EnumerableHashJoin;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.rules.TransformationRule;

/**
 * Rule that makes left input the smaller relation, which is to build the hash table;
 * right input the larger relation, which is to probe the hash table
 */
public class HashJoinLeftRightPosRule extends RelRule<HashJoinLeftRightPosRule.Config>
        implements TransformationRule {
  public static HashJoinLeftRightPosRule INSTANCE = Config.DEFAULT.toRule();

  protected HashJoinLeftRightPosRule(Config config) {
    super(config);
  }

  @Override
  public void onMatch(RelOptRuleCall call) {
    Join join = call.rel(0);
    RelMetadataQuery mq = join.getCluster().getMetadataQuery();
    RelNode leftRel = join.getLeft();
    RelNode rightRel = join.getRight();
    double leftRowCount = leftRel.estimateRowCount(mq);
    double rightRowCount = rightRel.estimateRowCount(mq);
    if (leftRowCount > rightRowCount) {
      RelNode newJoin = join.copy(join.getTraitSet(), join.getCondition(), rightRel, leftRel, join.getJoinType(), join.isSemiJoinDone());
      RelNode newRel = call.builder()
        .push(newJoin)
        .convert(join.getRowType(), false)
        .build();
      call.transformTo(newRel);
    }
  }

  public interface Config extends RelRule.Config {
    Config DEFAULT = EMPTY
            .withOperandSupplier(b -> b.operand(EnumerableHashJoin.class).anyInputs())
            .as(Config.class);

    @Override
    default HashJoinLeftRightPosRule toRule() {
      return new HashJoinLeftRightPosRule(this);
    }
  }
}
