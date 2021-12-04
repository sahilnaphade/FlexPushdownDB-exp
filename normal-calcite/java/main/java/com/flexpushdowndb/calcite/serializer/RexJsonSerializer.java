package com.flexpushdowndb.calcite.serializer;

import com.google.common.collect.BoundType;
import com.google.common.collect.Range;
import jdk.nashorn.internal.runtime.regexp.joni.ast.StringNode;
import org.apache.calcite.rex.*;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.type.SqlTypeFamily;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.DateString;
import org.apache.calcite.util.NlsString;
import org.apache.calcite.util.Sarg;
import org.json.JSONArray;
import org.json.JSONObject;

import java.math.BigDecimal;
import java.util.List;
import java.util.Objects;
import java.util.Set;

@SuppressWarnings({"BetaApi", "type.argument.type.incompatible", "UnstableApiUsage"})
public class RexJsonSerializer {
  public static JSONObject serialize(RexNode rexNode, List<String> fieldNames) {
    return rexNode.accept(new RexVisitor<JSONObject>() {
      @Override
      public JSONObject visitInputRef(RexInputRef inputRef) {
        JSONObject jo = new JSONObject();
        jo.put("inputRef", fieldNames.get(inputRef.getIndex()));
        return jo;
      }

      @Override
      public JSONObject visitCall(RexCall call) {
        switch (call.getKind()) {
          case LESS_THAN:
          case GREATER_THAN:
          case LESS_THAN_OR_EQUAL:
          case GREATER_THAN_OR_EQUAL:
          case EQUALS:
          case NOT_EQUALS:
          case AND:
          case OR:
          case NOT:
          case PLUS:
          case MINUS:
          case TIMES:
          case DIVIDE: {
            JSONObject jo = new JSONObject();
            // op
            jo.put("op", call.getKind());
            // operands
            JSONArray operandsJArr = new JSONArray();
            for (RexNode operand : call.getOperands()) {
              operandsJArr.put(serialize(operand, fieldNames));
            }
            jo.put("operands", operandsJArr);
            return jo;
          }
          case SEARCH: {
            RexInputRef searchRef = (RexInputRef) call.getOperands().get(0);
            String searchFieldName = fieldNames.get(searchRef.getIndex());
            RexLiteral literal = (RexLiteral) call.getOperands().get(1);
            return serializeSearch(searchFieldName, literal);
          }
          default: {
            throw new UnsupportedOperationException("Serialize unsupported RexCall: " + call.getKind());
          }
        }
      }

      @Override
      public JSONObject visitLocalRef(RexLocalRef localRef) {
        throw new UnsupportedOperationException("Serialize unsupported RexNode: " + localRef.getClass().getSimpleName());
      }

      @Override
      public JSONObject visitLiteral(RexLiteral literal) {
        JSONObject literalJObj = new JSONObject();
        SqlTypeName basicTypeName = literal.getType().getSqlTypeName();

        switch (basicTypeName) {
          case VARCHAR: {
            literalJObj.put("type", basicTypeName);
            literalJObj.put("value", literal.getValueAs(String.class));
            break;
          }
          case INTEGER: {
            literalJObj.put("type", basicTypeName);
            literalJObj.put("value", literal.getValueAs(Integer.class));
            break;
          }
          case BIGINT: {
            literalJObj.put("type", basicTypeName);
            literalJObj.put("value", literal.getValueAs(Long.class));
            break;
          }
          case DECIMAL: {
            literalJObj.put("type", basicTypeName);
            literalJObj.put("value", literal.getValueAs(Double.class));
            break;
          }
          case DATE: {
            literalJObj.put("type", "DATE_MS");
            literalJObj.put("value", Objects.requireNonNull(literal.getValueAs(DateString.class)).getMillisSinceEpoch());
            break;
          }
          case INTERVAL_DAY: {
            literalJObj.put("type", "INTERVAL_DAY");
            literalJObj.put("value", literal.getValueAs(Long.class) / 86400_000);
            break;
          }
          default:
            throw new UnsupportedOperationException("Serialize unsupported RexLiteral with basicTypeName: "
                    + basicTypeName);
        }

        return new JSONObject().put("literal", literalJObj);
      }

      @Override
      public JSONObject visitOver(RexOver over) {
        throw new UnsupportedOperationException("Serialize unsupported RexNode: " + over.getClass().getSimpleName());
      }

      @Override
      public JSONObject visitCorrelVariable(RexCorrelVariable correlVariable) {
        throw new UnsupportedOperationException("Serialize unsupported RexNode: " + correlVariable.getClass().getSimpleName());
      }

      @Override
      public JSONObject visitDynamicParam(RexDynamicParam dynamicParam) {
        throw new UnsupportedOperationException("Serialize unsupported RexNode: " + dynamicParam.getClass().getSimpleName());
      }

      @Override
      public JSONObject visitRangeRef(RexRangeRef rangeRef) {
        throw new UnsupportedOperationException("Serialize unsupported RexNode: " + rangeRef.getClass().getSimpleName());
      }

      @Override
      public JSONObject visitFieldAccess(RexFieldAccess fieldAccess) {
        throw new UnsupportedOperationException("Serialize unsupported RexNode: " + fieldAccess.getClass().getSimpleName());
      }

      @Override
      public JSONObject visitSubQuery(RexSubQuery subQuery) {
        throw new UnsupportedOperationException("Serialize unsupported RexNode: " + subQuery.getClass().getSimpleName());
      }

      @Override
      public JSONObject visitTableInputRef(RexTableInputRef fieldRef) {
        throw new UnsupportedOperationException("Serialize unsupported RexNode: " + fieldRef.getClass().getSimpleName());
      }

      @Override
      public JSONObject visitPatternFieldRef(RexPatternFieldRef fieldRef) {
        throw new UnsupportedOperationException("Serialize unsupported RexNode: " + fieldRef.getClass().getSimpleName());
      }
    });
  }

  private static JSONObject serializeSearch(String fieldName, RexLiteral literal) {
    Sarg<?> sarg = literal.getValueAs(Sarg.class);
    if (sarg == null) {
      throw new NullPointerException("Invalid Sarg: null");
    }
    SqlTypeName basicTypeName = literal.getType().getSqlTypeName();
    JSONObject rangeSetJObj = null;

    for (Object rangeObj: sarg.rangeSet.asRanges()) {
      Range<?> range = (Range<?>) rangeObj;
      Comparable<?> lowerValue = serializeSargEndPoint(basicTypeName, range.lowerEndpoint());
      Comparable<?> upperValue = serializeSargEndPoint(basicTypeName, range.upperEndpoint());
      JSONObject rangeJObj = new JSONObject();
      JSONObject inputRefOperand = new JSONObject()
              .put("inputRef", fieldName);

      if (lowerValue.equals(upperValue)) {
        // lower value = upper value
        JSONObject literalOperand = new JSONObject()
                .put("literal", new JSONObject()
                        .put("type", basicTypeName)
                        .put("value", lowerValue));
        rangeJObj.put("op", SqlKind.EQUALS)
                .put("operands", new JSONArray()
                        .put(inputRefOperand)
                        .put(literalOperand));
      } else {
        // lower value != upper value
        // lower
        SqlKind lowerOp = range.lowerBoundType() == BoundType.OPEN ?
                SqlKind.GREATER_THAN : SqlKind.GREATER_THAN_OR_EQUAL;
        JSONObject lowerLiteralOperand = new JSONObject()
                .put("literal", new JSONObject()
                        .put("type", basicTypeName)
                        .put("value", lowerValue));
        JSONObject rangeLowerJObj = new JSONObject()
                .put("op", lowerOp)
                .put("operands", new JSONArray()
                        .put(inputRefOperand)
                        .put(lowerLiteralOperand));

        // upper
        SqlKind upperOp = range.upperBoundType() == BoundType.OPEN ?
                SqlKind.LESS_THAN : SqlKind.LESS_THAN_OR_EQUAL;
        JSONObject upperLiteralOperand = new JSONObject()
                .put("literal", new JSONObject()
                        .put("type", basicTypeName)
                        .put("value", upperValue));
        JSONObject rangeUpperJObj = new JSONObject()
                .put("op", upperOp)
                .put("operands", new JSONArray()
                        .put(inputRefOperand)
                        .put(upperLiteralOperand));

        // make lower and upper an And
        rangeJObj.put("op", SqlKind.AND)
                .put("operands", new JSONArray()
                        .put(rangeLowerJObj)
                        .put(rangeUpperJObj));
      }

      // if more than one range, make them an Or
      if (rangeSetJObj == null) {
        rangeSetJObj = rangeJObj;
      } else {
        rangeSetJObj = new JSONObject()
                .put("op", SqlKind.OR)
                .put("operands", new JSONArray()
                        .put(rangeSetJObj)
                        .put(rangeJObj));
      }
    }

    return rangeSetJObj;
  }

  private static Comparable<?> serializeSargEndPoint(SqlTypeName basicTypeName, Comparable<?> endpoint) {
    switch (basicTypeName) {
      case CHAR:
      case VARCHAR: {
        return ((NlsString) endpoint).getValue();
      }
      case INTEGER: {
        return ((BigDecimal) endpoint).intValue();
      }
      case BIGINT: {
        return ((BigDecimal) endpoint).longValue();
      }
      case DECIMAL: {
        return ((BigDecimal) endpoint).doubleValue();
      }
      default: {
        throw new UnsupportedOperationException("Serialize unsupported RexLiteral with basicTypeName: "
                + basicTypeName);
      }
    }
  }
}
