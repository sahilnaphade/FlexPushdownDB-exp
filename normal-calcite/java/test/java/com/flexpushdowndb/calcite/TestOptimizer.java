package com.flexpushdowndb.calcite;

import com.flexpushdowndb.calcite.optimizer.Optimizer;
import com.flexpushdowndb.calcite.serializer.RelJsonSerializer;
import com.flexpushdowndb.util.FileUtils;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.sql.SqlExplainFormat;
import org.apache.calcite.sql.SqlExplainLevel;
import org.junit.jupiter.api.Test;

import java.nio.file.Path;
import java.nio.file.Paths;

public class TestOptimizer {
  private static final Path ssbQueryDirPath = Paths.get(System.getProperty("user.dir"))
          .resolve("test/resources/query/ssb");
  private static final Path tpchQueryDirPath = Paths.get(System.getProperty("user.dir"))
          .resolve("test/resources/query/tpch");

  public void testSingle(String schemaName, Path queryPath, boolean showJsonPlan) throws Exception {
    String query = FileUtils.readFile(queryPath);
    Optimizer optimizer = new Optimizer();
    RelNode queryPlan = optimizer.planQuery(query, schemaName);
    System.out.println(RelOptUtil.dumpPlan("[Optimized plan]", queryPlan, SqlExplainFormat.TEXT,
            SqlExplainLevel.ALL_ATTRIBUTES));
    if (showJsonPlan) {
      System.out.println("[Serialized json plan]\n" + RelJsonSerializer.serialize(queryPlan).toString(2));
    }
  }

  @Test
  public void testSSB2_1() throws Exception {
    Path queryPath = ssbQueryDirPath.resolve("2.1.sql");
    String schemaName = "ssb-sf1-sortlineorder/csv";
    testSingle(schemaName, queryPath, true);
  }

  @Test
  public void testMultiQuerySameSchema() throws Exception {
    Path queryPath = ssbQueryDirPath.resolve("2.1.sql");
    String schemaName = "ssb-sf1-sortlineorder/csv";
    testSingle(schemaName, queryPath, false);
    testSingle(schemaName, queryPath, false);
  }

  @Test
  public void testMultiQueryDiffSchema() throws Exception {
    Path queryPath = ssbQueryDirPath.resolve("2.1.sql");
    String schemaName1 = "ssb-sf1-sortlineorder/csv";
    String schemaName2 = "ssb-sf10-sortlineorder/csv";
    testSingle(schemaName1, queryPath, false);
    testSingle(schemaName2, queryPath, false);
  }

  @Test
  public void testTPCH_Q01() throws Exception {
    Path queryPath = tpchQueryDirPath.resolve("01.sql");
    String schemaName = "tpch-sf0.01/csv";
    testSingle(schemaName, queryPath, true);
  }

  @Test
  public void testTPCH_Q02() throws Exception {
    Path queryPath = tpchQueryDirPath.resolve("02.sql");
    String schemaName = "tpch-sf0.01/csv";
    testSingle(schemaName, queryPath, true);
  }

  @Test
  public void testTPCH_Q03() throws Exception {
    Path queryPath = tpchQueryDirPath.resolve("03.sql");
    String schemaName = "tpch-sf0.01/csv";
    testSingle(schemaName, queryPath, true);
  }

  @Test
  public void testTPCH_Q04() throws Exception {
    Path queryPath = tpchQueryDirPath.resolve("04.sql");
    String schemaName = "tpch-sf0.01/csv";
    testSingle(schemaName, queryPath, true);
  }

  @Test
  public void testTPCH_Q05() throws Exception {
    Path queryPath = tpchQueryDirPath.resolve("05.sql");
    String schemaName = "tpch-sf0.01/csv";
    testSingle(schemaName, queryPath, true);
  }

  @Test
  public void testTPCH_Q06() throws Exception {
    Path queryPath = tpchQueryDirPath.resolve("06.sql");
    String schemaName = "tpch-sf0.01/csv";
    testSingle(schemaName, queryPath, true);
  }

  @Test
  public void testTPCH_Q07() throws Exception {
    Path queryPath = tpchQueryDirPath.resolve("07.sql");
    String schemaName = "tpch-sf0.01/csv";
    testSingle(schemaName, queryPath, true);
  }

  @Test
  public void testTPCH_Q08() throws Exception {
    Path queryPath = tpchQueryDirPath.resolve("08.sql");
    String schemaName = "tpch-sf0.01/csv";
    testSingle(schemaName, queryPath, true);
  }

  @Test
  public void testTPCH_Q09() throws Exception {
    Path queryPath = tpchQueryDirPath.resolve("09.sql");
    String schemaName = "tpch-sf0.01/csv";
    testSingle(schemaName, queryPath, true);
  }

  @Test
  public void testTPCH_Q10() throws Exception {
    Path queryPath = tpchQueryDirPath.resolve("10.sql");
    String schemaName = "tpch-sf0.01/csv";
    testSingle(schemaName, queryPath, true);
  }

  @Test
  public void testTPCH_Q11() throws Exception {
    Path queryPath = tpchQueryDirPath.resolve("11.sql");
    String schemaName = "tpch-sf0.01/csv";
    testSingle(schemaName, queryPath, true);
  }

  @Test
  public void testTPCH_Q12() throws Exception {
    Path queryPath = tpchQueryDirPath.resolve("12.sql");
    String schemaName = "tpch-sf0.01/csv";
    testSingle(schemaName, queryPath, true);
  }

  @Test
  public void testTPCH_Q13() throws Exception {
    Path queryPath = tpchQueryDirPath.resolve("13.sql");
    String schemaName = "tpch-sf0.01/csv";
    testSingle(schemaName, queryPath, true);
  }

  @Test
  public void testTPCH_Q14() throws Exception {
    Path queryPath = tpchQueryDirPath.resolve("14.sql");
    String schemaName = "tpch-sf0.01/csv";
    testSingle(schemaName, queryPath, true);
  }

  @Test
  public void testTPCH_Q16() throws Exception {
    Path queryPath = tpchQueryDirPath.resolve("16.sql");
    String schemaName = "tpch-sf0.01/csv";
    testSingle(schemaName, queryPath, true);
  }

  @Test
  public void testTPCH_Q17() throws Exception {
    Path queryPath = tpchQueryDirPath.resolve("17.sql");
    String schemaName = "tpch-sf0.01/csv";
    testSingle(schemaName, queryPath, true);
  }

  @Test
  public void testTPCH_Q18() throws Exception {
    Path queryPath = tpchQueryDirPath.resolve("18.sql");
    String schemaName = "tpch-sf0.01/csv";
    testSingle(schemaName, queryPath, true);
  }

  @Test
  public void testTPCH_Q19() throws Exception {
    Path queryPath = tpchQueryDirPath.resolve("19.sql");
    String schemaName = "tpch-sf0.01/csv";
    testSingle(schemaName, queryPath, true);
  }

  @Test
  public void testTPCH_Q20() throws Exception {
    Path queryPath = tpchQueryDirPath.resolve("20.sql");
    String schemaName = "tpch-sf0.01/csv";
    testSingle(schemaName, queryPath, true);
  }

  @Test
  public void testTPCH_Q22() throws Exception {
    Path queryPath = tpchQueryDirPath.resolve("22.sql");
    String schemaName = "tpch-sf0.01/csv";
    testSingle(schemaName, queryPath, true);
  }
}
