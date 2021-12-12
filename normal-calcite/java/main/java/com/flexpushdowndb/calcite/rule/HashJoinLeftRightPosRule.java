package com.flexpushdowndb.calcite.rule;

import com.flexpushdowndb.calcite.rule.util.SimpleExpressionCanonicalizer;
import org.apache.calcite.adapter.enumerable.EnumerableHashJoin;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.rules.JoinCommuteRule;
import org.apache.calcite.rel.rules.TransformationRule;
import org.apache.calcite.rex.RexBuilder;

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
    // Currently only deal with INNER join
    if (join.getJoinType() != JoinRelType.INNER) {
      return;
    }
    RelMetadataQuery mq = join.getCluster().getMetadataQuery();
    RelNode leftRel = join.getLeft();
    RelNode rightRel = join.getRight();
    double leftRowCount = leftRel.estimateRowCount(mq);
    double rightRowCount = rightRel.estimateRowCount(mq);
    if (leftRowCount > rightRowCount) {
      // swap join inputs and fetch the underlying join
      Project project = (Project) JoinCommuteRule.swap(join, false, call.builder());
      if (project == null) {
        throw new NullPointerException("Null of swapped join, from " + this.getClass().getSimpleName());
      }
      Join joinSwapInputs = (Join) project.getInput(0);
      // mirror the join condition
      RexBuilder rexBuilder = join.getCluster().getRexBuilder();
      RelNode newJoin = joinSwapInputs.copy(
              joinSwapInputs.getTraitSet(),
              SimpleExpressionCanonicalizer.mirrorRexNode(joinSwapInputs.getCondition(), rexBuilder),
              joinSwapInputs.getLeft(),
              joinSwapInputs.getRight(),
              joinSwapInputs.getJoinType(),
              joinSwapInputs.isSemiJoinDone());
      // add back the project
      RelNode newRel = call.builder()
          .push(newJoin)
          .project(project.getProjects(), join.getRowType().getFieldNames())
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
