package com.flexpushdowndb.calcite.rule;

import com.flexpushdowndb.calcite.rule.util.SimpleExpressionCanonicalizer;
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
 * Rule that makes the smaller relation as the left input of a join
 */
public class JoinSmallLeftRule extends RelRule<JoinSmallLeftRule.Config>
        implements TransformationRule {
  public static JoinSmallLeftRule INSTANCE = Config.DEFAULT.toRule();

  protected JoinSmallLeftRule(Config config) {
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
            .withOperandSupplier(b -> b.operand(Join.class).anyInputs())
            .as(Config.class);

    @Override
    default JoinSmallLeftRule toRule() {
      return new JoinSmallLeftRule(this);
    }
  }
}
