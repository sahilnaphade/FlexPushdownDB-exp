package com.flexpushdowndb.calcite.serializer;

import org.apache.calcite.adapter.enumerable.*;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.core.Sort;
import org.apache.calcite.rex.*;
import org.json.JSONArray;
import org.json.JSONObject;

import java.util.List;

public final class RelJsonSerializer {
  public static JSONObject serialize(RelNode relNode) {
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
    } else if (relNode instanceof EnumerableLimitSort) {
      jo = serializeEnumerableLimitSort((EnumerableLimitSort) relNode);
    } else {
      throw new UnsupportedOperationException("Serialize unsupported RelNode: " + relNode.getClass().getSimpleName());
    }
    return jo;
  }

  private static JSONArray serializeRelInputs(RelNode relNode) {
    JSONArray jArr = new JSONArray();
    for (RelNode input: relNode.getInputs()) {
      jArr.put(serialize(input));
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
    // input operators
    jo.put("inputs", serializeRelInputs(scan));
    return jo;
  }

  private static JSONObject serializeEnumerableFilter(EnumerableFilter filter) {
    JSONObject jo = new JSONObject();
    // operator name
    jo.put("operator", filter.getClass().getSimpleName());
    // filter condition
    jo.put("condition", RexJsonSerializer.serialize(filter.getCondition(), filter.getRowType().getFieldNames()));
    // input operators
    jo.put("inputs", serializeRelInputs(filter));
    return jo;
  }

  private static JSONObject serializeEnumerableHashJoin(EnumerableHashJoin join) {
    JSONObject jo = new JSONObject();
    // operator name
    jo.put("operator", join.getClass().getSimpleName());
    // join condition
    jo.put("condition", RexJsonSerializer.serialize(join.getCondition(), join.getRowType().getFieldNames()));
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
    List<String> inputFieldNames = project.getInput().getRowType().getFieldNames();
    List<String> outputFieldNames = project.getRowType().getFieldNames();
    JSONArray fields = new JSONArray();
    int projectId = 0;
    for (RexNode rexNode: project.getProjects()) {
      JSONObject fieldJObj = new JSONObject();
      fieldJObj.put("name", outputFieldNames.get(projectId));
      fieldJObj.put("expr", RexJsonSerializer.serialize(rexNode, inputFieldNames));
      fields.put(fieldJObj);
      ++projectId;
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
    List<String> inputFieldNames = aggregate.getInput().getRowType().getFieldNames();
    List<String> outputFieldNames = aggregate.getRowType().getFieldNames();
    JSONArray groupFieldsJArr = new JSONArray();
    for (int fieldIndex: aggregate.getGroupSet().asList()) {
      groupFieldsJArr.put(inputFieldNames.get(fieldIndex));
    }
    jo.put("groupFields", groupFieldsJArr);

    // aggregations
    JSONArray aggListJArr = new JSONArray();
    int outputFieldId = groupFieldsJArr.length();
    for (AggregateCall aggCall: aggregate.getAggCallList()){
      JSONObject aggCallJObj = new JSONObject();
      aggCallJObj.put("aggFunction", aggCall.getAggregation().kind.name());
      aggCallJObj.put("aggOutputField", outputFieldNames.get(outputFieldId));
      if (!aggCall.getArgList().isEmpty()) {
        // Haven't got an aggregation function with multiple arguments
        int aggFieldId = aggCall.getArgList().get(0);
        aggCallJObj.put("aggInputField", inputFieldNames.get(aggFieldId));
      }
      aggListJArr.put(aggCallJObj);
      ++outputFieldId;
    }
    jo.put("aggregations", aggListJArr);

    // input operators
    jo.put("inputs", serializeRelInputs(aggregate));
    return jo;
  }

  private static JSONArray serializeSortFields(Sort sort) {
    List<String> inputFieldNames = sort.getInput().getRowType().getFieldNames();
    JSONArray sortFieldsJArr = new JSONArray();
    for (RelFieldCollation collation: sort.getCollation().getFieldCollations()) {
      JSONObject sortFieldJObj = new JSONObject();
      sortFieldJObj.put("field", inputFieldNames.get(collation.getFieldIndex()));
      sortFieldJObj.put("direction", collation.getDirection());
      sortFieldsJArr.put(sortFieldJObj);
    }
    return sortFieldsJArr;
  }

  private static JSONObject serializeEnumerableSort(EnumerableSort sort) {
    JSONObject jo = new JSONObject();
    // operator name
    jo.put("operator", sort.getClass().getSimpleName());
    // sort fields
    jo.put("sortFields", serializeSortFields(sort));
    // input operators
    jo.put("inputs", serializeRelInputs(sort));
    return jo;
  }

  private static JSONObject serializeEnumerableLimitSort(EnumerableLimitSort limitSort) {
    JSONObject jo = new JSONObject();
    // operator name
    jo.put("operator", limitSort.getClass().getSimpleName());
    // sort fields
    jo.put("sortFields", serializeSortFields(limitSort));
    // limit
    assert limitSort.fetch != null;
    jo.put("limit", RexJsonSerializer.serialize(limitSort.fetch, null));
    // input operators
    jo.put("inputs", serializeRelInputs(limitSort));
    return jo;
  }
}