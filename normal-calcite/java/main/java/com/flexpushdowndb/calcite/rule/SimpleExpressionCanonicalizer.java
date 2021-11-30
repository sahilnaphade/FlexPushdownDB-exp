package com.flexpushdowndb.calcite.rule;

import com.google.common.collect.ImmutableList;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexShuttle;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;

public class SimpleExpressionCanonicalizer {
  public static RexNode mirrorRexNode(RexNode rexNode, RexBuilder rexBuilder) {
    return rexNode.accept(new RexShuttle(){
      @Override
      public RexNode visitCall(RexCall call) {
        switch (call.getKind()) {
          case LESS_THAN:
          case GREATER_THAN:
          case LESS_THAN_OR_EQUAL:
          case GREATER_THAN_OR_EQUAL:
          case EQUALS:
          case NOT_EQUALS: {
            RexNode leftOp = call.getOperands().get(0);
            RexNode rightOp = call.getOperands().get(1);
            return rexBuilder.makeCall(mirrorOperation(call.getKind()), ImmutableList.of(rightOp, leftOp));
          }
          default:
            return super.visitCall(call);
        }
      }
    });
  }

  /**
   * Rewrite b > a to a < b
   * @param sqlKind kind of op
   * @return mirrored op
   */
  private static SqlOperator mirrorOperation(SqlKind sqlKind) {
    switch (sqlKind) {
      case LESS_THAN:
        return SqlStdOperatorTable.GREATER_THAN;
      case GREATER_THAN:
        return SqlStdOperatorTable.LESS_THAN;
      case LESS_THAN_OR_EQUAL:
        return SqlStdOperatorTable.GREATER_THAN_OR_EQUAL;
      case GREATER_THAN_OR_EQUAL:
        return SqlStdOperatorTable.LESS_THAN_OR_EQUAL;
      case EQUALS:
        return SqlStdOperatorTable.EQUALS;
      case NOT_EQUALS:
        return SqlStdOperatorTable.NOT_EQUALS;
      default:
        throw new UnsupportedOperationException();
    }
  }
}
