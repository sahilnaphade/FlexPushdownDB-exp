package com.flexpushdowndb.calcite;

import com.flexpushdowndb.calcite.optimizer.OptimizeResult;
import com.flexpushdowndb.calcite.optimizer.Optimizer;
import com.flexpushdowndb.calcite.serializer.RelJsonSerializer;
import com.flexpushdowndb.util.FileUtils;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.sql.SqlExplainFormat;
import org.apache.calcite.sql.SqlExplainLevel;
import org.ini4j.Ini;
import org.junit.jupiter.api.Test;

import java.io.InputStream;
import java.nio.file.Path;
import java.nio.file.Paths;

public class TestOptimizer {

  public void testSingle(String schemaName, String queryFileName, boolean showJsonPlan) throws Exception {
    // resource path
    InputStream is = getClass().getResourceAsStream("/config/exec.conf");
    Ini ini = new Ini(is);
    Path resourcePath = Paths.get(ini.get("conf", "RESOURCE_PATH"));

    // read query
    Path queryPath = resourcePath
            .resolve("query")
            .resolve(queryFileName);
    String query = FileUtils.readFile(queryPath);

    // plan
    Optimizer optimizer = new Optimizer(resourcePath);
    OptimizeResult optimizeResult = optimizer.planQuery(query, schemaName);
    System.out.println(RelOptUtil.dumpPlan("[Optimized plan]", optimizeResult.getPlan(), SqlExplainFormat.TEXT,
            SqlExplainLevel.ALL_ATTRIBUTES));
    if (showJsonPlan) {
      System.out.println("[Serialized json plan]\n" + RelJsonSerializer
              .serialize(optimizeResult.getPlan(), optimizeResult.getPushableHashJoins())
              .toString(2));
    }
  }

  @Test
  public void testSSB1_1_Typed() throws Exception {
    testSingle("ssb-sf1-sortlineorder/csv", "ssb/typed/1.1.sql", true);
  }

  @Test
  public void testSSB2_1() throws Exception {
    testSingle("ssb-sf1-sortlineorder/csv", "ssb/original/2.1.sql", true);
  }

  @Test
  public void testMultiQuerySameSchema() throws Exception {
    testSingle("ssb-sf1-sortlineorder/csv", "ssb/original/2.1.sql", false);
    testSingle("ssb-sf1-sortlineorder/csv", "ssb/original/2.1.sql", false);
  }

  @Test
  public void testMultiQueryDiffSchema() throws Exception {
    testSingle("ssb-sf1-sortlineorder/csv", "ssb/original/2.1.sql", false);
    testSingle("ssb-sf10-sortlineorder/csv", "ssb/original/2.1.sql", false);
  }

  @Test
  public void testTPCH_Q01() throws Exception {
    testSingle("tpch-sf0.01/csv", "tpch/original/01.sql", true);
  }

  @Test
  public void testTPCH_Q02() throws Exception {
    testSingle("tpch-sf0.01/csv", "tpch/original/02.sql", true);
  }

  @Test
  public void testTPCH_Q03() throws Exception {
    testSingle("tpch-sf0.01/csv", "tpch/original/03.sql", true);
  }

  @Test
  public void testTPCH_Q04() throws Exception {
    testSingle("tpch-sf0.01/csv", "tpch/original/04.sql", true);
  }

  @Test
  public void testTPCH_Q05() throws Exception {
    testSingle("tpch-sf0.01/csv", "tpch/original/05.sql", true);
  }

  @Test
  public void testTPCH_Q06() throws Exception {
    testSingle("tpch-sf0.01/csv", "tpch/original/06.sql", true);
  }

  @Test
  public void testTPCH_Q07() throws Exception {
    testSingle("tpch-sf0.01/csv", "tpch/original/07.sql", true);
  }

  @Test
  public void testTPCH_Q08() throws Exception {
    testSingle("tpch-sf0.01/csv", "tpch/original/08.sql", true);
  }

  @Test
  public void testTPCH_Q09() throws Exception {
    testSingle("tpch-sf0.01/csv", "tpch/original/09.sql", true);
  }

  @Test
  public void testTPCH_Q10() throws Exception {
    testSingle("tpch-sf0.01/csv", "tpch/original/10.sql", true);
  }

  @Test
  public void testTPCH_Q11() throws Exception {
    testSingle("tpch-sf0.01/csv", "tpch/original/11.sql", true);
  }

  @Test
  public void testTPCH_Q12() throws Exception {
    testSingle("tpch-sf0.01/csv", "tpch/original/12.sql", true);
  }

  @Test
  public void testTPCH_Q13() throws Exception {
    testSingle("tpch-sf0.01/csv", "tpch/original/13.sql", true);
  }

  @Test
  public void testTPCH_Q14() throws Exception {
    testSingle("tpch-sf0.01/csv", "tpch/original/14.sql", true);
  }

  @Test
  public void testTPCH_Q15() throws Exception {
    testSingle("tpch-sf0.01/csv", "tpch/original/15.sql", true);
  }

  @Test
  public void testTPCH_Q16() throws Exception {
    testSingle("tpch-sf0.01/csv", "tpch/original/16.sql", true);
  }

  @Test
  public void testTPCH_Q17() throws Exception {
    testSingle("tpch-sf0.01/csv", "tpch/original/17.sql", true);
  }

  @Test
  public void testTPCH_Q18() throws Exception {
    testSingle("tpch-sf0.01/csv", "tpch/original/18.sql", true);
  }

  @Test
  public void testTPCH_Q19() throws Exception {
    testSingle("tpch-sf0.01/csv", "tpch/original/19.sql", true);
  }

  @Test
  public void testTPCH_Q20() throws Exception {
    testSingle("tpch-sf0.01/csv", "tpch/original/20.sql", true);
  }

  @Test
  public void testTPCH_Q21() throws Exception {
    testSingle("tpch-sf0.01/csv", "tpch/original/21.sql", true);
  }

  @Test
  public void testTPCH_Q22() throws Exception {
    testSingle("tpch-sf0.01/csv", "tpch/original/22.sql", true);
  }

  @Test
  // for 'tpch-sf0.01-1-node-hash-part', 'lineitem' and 'orders' are co-located at 'l_orderkey = o.o_orderkey'
  public void testCoHashJoinPushable() throws Exception {
    testSingle("tpch-sf0.01-1-node-hash-part/parquet", "tpch/synthetic/co-join-3-table.sql", true);
  }

  @Test
  // for 'tpch-sf0.01-1-node-hash-part', 'lineitem' and 'orders' are co-located at 'l_orderkey = o.o_orderkey'
  public void testCoHashJoinNotPushable() throws Exception {
    testSingle("tpch-sf0.01-1-node-hash-part/parquet", "tpch/original/19.sql", true);
  }
}
