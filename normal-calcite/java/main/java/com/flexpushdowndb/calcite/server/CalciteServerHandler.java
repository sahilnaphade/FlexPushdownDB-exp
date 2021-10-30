package com.flexpushdowndb.calcite.server;

import com.flexpushdowndb.calcite.optimizer.Optimizer;
import com.flexpushdowndb.calcite.serializer.RelJsonSerializer;
import com.thrift.calciteserver.CalciteServer;
import com.thrift.calciteserver.ParsePlanningError;
import com.thrift.calciteserver.TPlanResult;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.sql.SqlExplainFormat;
import org.apache.calcite.sql.SqlExplainLevel;
import org.apache.thrift.TException;
import org.apache.thrift.server.TServer;
import org.apache.thrift.transport.TServerTransport;

import java.util.Arrays;

public class CalciteServerHandler implements CalciteServer.Iface{
  private final Optimizer optimizer;
  private TServerTransport serverTransport;
  private TServer server;

  public CalciteServerHandler() {
    this.optimizer = new Optimizer();
  }

  public void setServerTransport(TServerTransport serverTransport) {
    this.serverTransport = serverTransport;
  }

  public void setServer(TServer server) {
    this.server = server;
  }

  public void ping() throws TException {
    System.out.println("Client ping");
  }

  public void shutdown() throws TException{
    server.stop();
    serverTransport.close();
    System.out.println("Calcite server shutdown...");
  }

  @Override
  public TPlanResult sql2Plan(String query, String schemaName) throws TException {
    long startTime = System.currentTimeMillis();
    TPlanResult tPlanResult = new TPlanResult();
    try {
      RelNode queryPlan = optimizer.planQuery(query, schemaName);
      tPlanResult.plan_result = RelJsonSerializer.serializeRelNode(queryPlan).toString();
    } catch (Exception e) {
      e.printStackTrace();
      throw new ParsePlanningError(Arrays.toString(e.getStackTrace()));
    }
    tPlanResult.execution_time_ms = System.currentTimeMillis() - startTime;
    return tPlanResult;
  }

  @Override
  public void updateMetadata(String catalog, String table) throws TException{}
}
