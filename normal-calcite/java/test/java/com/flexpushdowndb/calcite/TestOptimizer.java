package com.flexpushdowndb.calcite;

import com.flexpushdowndb.calcite.optimizer.Optimizer;
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

  @Test
  public void testSsbSingle() throws Exception {
    Path queryPath = ssbQueryDirPath.resolve("3.1.sql");
    String query = FileUtils.readFile(queryPath);
    String schemaName = "ssb-sf1-sortlineorder/csv";
    Optimizer optimizer = new Optimizer();
    RelNode queryPlan = optimizer.optimize(query, schemaName);
    System.out.println(RelOptUtil.dumpPlan("[Optimized plan]", queryPlan, SqlExplainFormat.TEXT,
            SqlExplainLevel.ALL_ATTRIBUTES));
  }

  @Test
  public void testMultiQuerySameSchema() throws Exception {
    Path queryPath = ssbQueryDirPath.resolve("2.1.sql");
    String query = FileUtils.readFile(queryPath);
    String schemaName = "ssb-sf1-sortlineorder/csv";
    Optimizer optimizer = new Optimizer();
    RelNode queryPlan = optimizer.optimize(query, schemaName);
    System.out.println(RelOptUtil.dumpPlan("[Optimized plan]", queryPlan, SqlExplainFormat.TEXT,
            SqlExplainLevel.ALL_ATTRIBUTES));
    queryPlan = optimizer.optimize(query, schemaName);
    System.out.println(RelOptUtil.dumpPlan("[Optimized plan]", queryPlan, SqlExplainFormat.TEXT,
            SqlExplainLevel.ALL_ATTRIBUTES));
  }

  @Test
  public void testMultiQueryDiffSchema() throws Exception {
    Path queryPath = ssbQueryDirPath.resolve("2.1.sql");
    String query = FileUtils.readFile(queryPath);
    String schemaName1 = "ssb-sf1-sortlineorder/csv";
    String schemaName2 = "ssb-sf10-sortlineorder/csv";
    Optimizer optimizer = new Optimizer();
    RelNode queryPlan = optimizer.optimize(query, schemaName1);
    System.out.println(RelOptUtil.dumpPlan("[Optimized plan]", queryPlan, SqlExplainFormat.TEXT,
            SqlExplainLevel.ALL_ATTRIBUTES));
    queryPlan = optimizer.optimize(query, schemaName2);
    System.out.println(RelOptUtil.dumpPlan("[Optimized plan]", queryPlan, SqlExplainFormat.TEXT,
            SqlExplainLevel.ALL_ATTRIBUTES));
  }
}
