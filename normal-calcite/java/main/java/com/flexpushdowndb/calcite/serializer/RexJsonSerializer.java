package com.flexpushdowndb.calcite.serializer;

import com.google.common.collect.Range;
import org.apache.calcite.rex.*;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.Sarg;
import org.json.JSONArray;
import org.json.JSONObject;

import java.util.List;
import java.util.Objects;

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
        JSONObject jo = new JSONObject();
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
          case SEARCH:
          case PLUS:
          case MINUS:
          case TIMES:
          case DIVIDE: {
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
        SqlTypeName typeName = literal.getTypeName();
        JSONObject jo = new JSONObject();
        jo.put("type", typeName);
        switch (typeName) {
          case CHAR: {
            jo.put("value", literal.getValueAs(String.class));
            return jo;
          }
          case DECIMAL: {
            SqlTypeName basicTypeName = literal.getType().getSqlTypeName();
            switch (basicTypeName) {
              case INTEGER: {
                jo.put("value", literal.getValueAs(Integer.class));
                return jo;
              }
              case BIGINT: {
                jo.put("value", literal.getValueAs(Long.class));
                return jo;
              }
              case DECIMAL: {
                jo.put("value", literal.getValueAs(Double.class));
                return jo;
              }
              default:
                throw new UnsupportedOperationException("Serialize unsupported RexLiteral with basicTypeName: "
                        + basicTypeName);
            }
          }
          case SARG: {
            JSONObject rangeJObj = new JSONObject();
            Range range = ((Sarg) Objects.requireNonNull(literal.getValue())).rangeSet.span();
            // lower
            JSONObject lowerJObj = new JSONObject();
            lowerJObj.put("boundType", range.lowerBoundType());
            lowerJObj.put("value", range.lowerEndpoint());
            rangeJObj.put("lower", lowerJObj);
            // upper
            JSONObject upperJObj = new JSONObject();
            upperJObj.put("boundType", range.upperBoundType());
            upperJObj.put("value", range.upperEndpoint());
            rangeJObj.put("upper", upperJObj);
            jo.put("range", rangeJObj);
            return jo;
          }
          default:
            throw new UnsupportedOperationException("Serialize unsupported RexLiteral with typeName: " + typeName);
        }
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
}
