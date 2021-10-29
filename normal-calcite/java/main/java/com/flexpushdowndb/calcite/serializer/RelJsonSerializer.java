package com.flexpushdowndb.calcite.serializer;

import org.apache.calcite.adapter.enumerable.*;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rex.*;
import org.json.JSONArray;
import org.json.JSONObject;

public final class RelJsonSerializer {
  public static JSONObject serializeRelNode(RelNode relNode) {
    JSONObject jo;
    if (relNode instanceof EnumerableTableScan) {
      jo = serializeEnumerableTableScan((EnumerableTableScan) relNode);
    } else if (relNode instanceof EnumerableFilter) {
      jo = serializeEnumerableFilter((EnumerableFilter) relNode);
    } else if (relNode instanceof EnumerableHashJoin) {
      jo = serializeEnumerableHashJoin((EnumerableHashJoin) relNode);
    } else if (relNode instanceof EnumerableProject) {
      jo = serializeEnumerableProject((EnumerableProject) relNode);
    } else if (relNode instanceof EnumerableAggregate) {
      jo = serializeEnumerableAggregate((EnumerableAggregate) relNode);
    } else if (relNode instanceof EnumerableSort) {
      jo = serializeEnumerableSort((EnumerableSort) relNode);
    } else {
      throw new UnsupportedOperationException("Serialize unsupported RelNode: " + relNode.getClass().getSimpleName());
    }
    return jo;
  }

  private static JSONArray serializeRelInputs(RelNode relNode) {
    JSONArray jArr = new JSONArray();
    for (RelNode input: relNode.getInputs()) {
      jArr.put(serializeRelNode(input));
    }
    return jArr;
  }

  private static JSONObject serializeEnumerableTableScan(EnumerableTableScan scan) {
    JSONObject jo = new JSONObject();
    // operator name
    jo.put("operator", scan.getClass().getSimpleName());
    // schema
    jo.put("schema", scan.getTable().getQualifiedName().get(0));
    // table
    jo.put("table", scan.getTable().getQualifiedName().get(1));
    return jo;
  }

  private static JSONObject serializeEnumerableFilter(EnumerableFilter filter) {
    JSONObject jo = new JSONObject();
    // operator name
    jo.put("operator", filter.getClass().getSimpleName());
    // filter condition
    jo.put("condition", RexJsonSerializer.serializeRexNode(filter.getCondition()));
    // input operators
    jo.put("inputs", serializeRelInputs(filter));
    return jo;
  }

  private static JSONObject serializeEnumerableHashJoin(EnumerableHashJoin join) {
    JSONObject jo = new JSONObject();
    // operator name
    jo.put("operator", join.getClass().getSimpleName());
    // join condition
    jo.put("condition", RexJsonSerializer.serializeRexNode(join.getCondition()));
    // join type
    jo.put("joinType", join.getJoinType());
    // input operators
    jo.put("inputs", serializeRelInputs(join));
    return jo;
  }

  private static JSONObject serializeEnumerableProject(EnumerableProject project) {
    JSONObject jo = new JSONObject();
    // operator name
    jo.put("operator", project.getClass().getSimpleName());
    // project fields
    JSONArray fields = new JSONArray();
    for (RexNode rexNode: project.getProjects()) {
      fields.put(RexJsonSerializer.serializeRexNode(rexNode));
    }
    jo.put("fields", fields);
    // input operators
    jo.put("inputs", serializeRelInputs(project));
    return jo;
  }

  private static JSONObject serializeEnumerableAggregate(EnumerableAggregate aggregate) {
    JSONObject jo = new JSONObject();
    // operator name
    jo.put("operator", aggregate.getClass().getSimpleName());
    // group fields
    JSONArray groupFieldsJArr = new JSONArray();
    for (int fieldIndex: aggregate.getGroupSet().asList()) {
      groupFieldsJArr.put(fieldIndex);
    }
    jo.put("groupFields", groupFieldsJArr);
    // aggregations
    JSONArray aggListJArr = new JSONArray();
    for (AggregateCall aggCall: aggregate.getAggCallList()){
      JSONObject aggCallJObj = new JSONObject();
      aggCallJObj.put("function", aggCall.getAggregation().kind.name());
      // Haven't got an aggregation function with multiple arguments
      aggCallJObj.put("aggField", aggCall.getArgList().get(0));
      aggListJArr.put(aggCallJObj);
    }
    jo.put("aggregations", aggListJArr);
    // input operators
    jo.put("inputs", serializeRelInputs(aggregate));
    return jo;
  }

  private static JSONObject serializeEnumerableSort(EnumerableSort sort) {
    JSONObject jo = new JSONObject();
    // operator name
    jo.put("operator", sort.getClass().getSimpleName());
    // sort fields
    JSONArray sortFieldsJArr = new JSONArray();
    for (RelFieldCollation collation: sort.getCollation().getFieldCollations()) {
      JSONObject sortFieldJObj = new JSONObject();
      sortFieldJObj.put("field", collation.getFieldIndex());
      sortFieldJObj.put("direction", collation.getDirection());
      sortFieldsJArr.put(sortFieldJObj);
    }
    jo.put("sortFields", sortFieldsJArr);
    // input operators
    jo.put("inputs", serializeRelInputs(sort));
    return jo;
  }
}
