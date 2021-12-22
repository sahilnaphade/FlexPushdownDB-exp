package com.flexpushdowndb.calcite.optimizer;

import com.flexpushdowndb.calcite.rule.EnhancedFilterJoinRule;
import com.flexpushdowndb.calcite.rule.JoinSmallLeftRule;
import com.flexpushdowndb.calcite.schema.SchemaImpl;
import com.flexpushdowndb.calcite.schema.SchemaReader;
import com.google.common.collect.ImmutableList;
import org.apache.calcite.adapter.enumerable.EnumerableConvention;
import org.apache.calcite.adapter.enumerable.EnumerableRules;
import org.apache.calcite.config.CalciteConnectionConfig;
import org.apache.calcite.config.CalciteConnectionProperty;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.plan.*;
import org.apache.calcite.plan.hep.HepPlanner;
import org.apache.calcite.plan.hep.HepProgram;
import org.apache.calcite.plan.hep.HepProgramBuilder;
import org.apache.calcite.plan.volcano.VolcanoPlanner;
import org.apache.calcite.prepare.CalciteCatalogReader;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.rules.CoreRules;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorUtil;
import org.apache.calcite.sql2rel.RelDecorrelator;
import org.apache.calcite.sql2rel.RelFieldTrimmer;
import org.apache.calcite.sql2rel.SqlToRelConverter;
import org.apache.calcite.sql2rel.StandardConvertletTable;
import org.apache.calcite.tools.*;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class Optimizer {
  private static final RelOptTable.ViewExpander NOOP_EXPANDER = (type, query, schema, path) -> null;
  private final RelDataTypeFactory typeFactory;
  private final RelOptCluster cluster;
  private final RelOptPlanner planner;
  private final RelBuilder relBuilder;
  private final CalciteSchema rootSchema;

  // assigned during query parsing
  private SqlValidator sqlValidator = null;

  public Optimizer() {
    this.typeFactory = new JavaTypeFactoryImpl();
    this.cluster = newCluster(typeFactory);
    this.planner = cluster.getPlanner();
    this.relBuilder = RelBuilder.proto(planner.getContext()).create(cluster, null);
    this.rootSchema = CalciteSchema.createRootSchema(false, true);
  }

  public RelNode planQuery(String query, String schemaName) throws Exception {
    // Parse and Validate
    RelNode logicalPlan = parseAndValidate(query, schemaName);

    // Decorrelate
    RelNode decorrelatedPlan = RelDecorrelator.decorrelateQuery(logicalPlan, relBuilder);

    // Filter pushdown
    RelNode filterPushdownPlan = filterPushdown(decorrelatedPlan);

    // Volcano cost-based optimization
    RelNode volOptPlan = logicalOptimize(filterPushdownPlan);

    // Trim unused fields
    RelNode trimmedPhysicalPlan = trim(volOptPlan);

    // Heuristics to apply
    return postHeuristics(trimmedPhysicalPlan);
  }

  private RelNode parseAndValidate(String query, String schemaName) throws Exception {
    // Load schema if not loaded
    if (!rootSchema.getSubSchemaMap().containsKey(schemaName)) {
      SchemaImpl schema = SchemaReader.readSchema(schemaName);
      rootSchema.add(schemaName, schema);
    }

    // Create an SQL parser and parse the query into AST
    SqlParser parser = SqlParser.create(query);
    SqlNode parseAst = parser.parseQuery();

    // Create a CatalogReader and an SQL validator to validate the AST
    CalciteCatalogReader catalogReader = new CalciteCatalogReader(rootSchema, Collections.singletonList(schemaName),
            typeFactory, CalciteConnectionConfig.DEFAULT.set(CalciteConnectionProperty.CASE_SENSITIVE, "false"));
    sqlValidator = SqlValidatorUtil.newValidator(SqlStdOperatorTable.instance(), catalogReader,
            typeFactory, SqlValidator.Config.DEFAULT);
    SqlNode validAst = sqlValidator.validate(parseAst);

    // Convert the AST into a RelNode
    SqlToRelConverter sqlToRelConverter = new SqlToRelConverter(NOOP_EXPANDER,
            sqlValidator,
            catalogReader,
            cluster,
            StandardConvertletTable.INSTANCE,
            SqlToRelConverter.config());
    return sqlToRelConverter.convertQuery(validAst, false, true).rel;
  }

  private RelNode filterPushdown(RelNode relNode) {
    HepProgram hepProgram = new HepProgramBuilder()
            .addRuleCollection(ImmutableList.of(
                    CoreRules.FILTER_PROJECT_TRANSPOSE,
                    CoreRules.FILTER_AGGREGATE_TRANSPOSE,
                    EnhancedFilterJoinRule.WITH_FILTER,
                    EnhancedFilterJoinRule.NO_FILTER))
            .build();
    HepPlanner hepPlanner = new HepPlanner(hepProgram);
    hepPlanner.setRoot(relNode);
    return hepPlanner.findBestExp();
  }

  private RelNode logicalOptimize(RelNode relNode) {
    Program program = Programs.of(getDefaultRuleSet());
    return program.run(
            planner,
            relNode,
            relNode.getTraitSet().plus(EnumerableConvention.INSTANCE),
            Collections.emptyList(),
            Collections.emptyList()
    );
  }

  private RelNode trim(RelNode relNode) {
    RelFieldTrimmer trimmer = new RelFieldTrimmer(sqlValidator, relBuilder);
    RelNode trimmedPlan = trimmer.trim(relNode);

    // Convert trimmedPlan to physical
    Program program = Programs.of(getConvertToPhysicalRuleSet());
    return program.run(
            planner,
            trimmedPlan,
            trimmedPlan.getTraitSet().plus(EnumerableConvention.INSTANCE),
            Collections.emptyList(),
            Collections.emptyList()
    );
  }

  private RelNode postHeuristics(RelNode relNode) {
    HepProgram hepProgram = new HepProgramBuilder()
            .addRuleInstance(JoinSmallLeftRule.INSTANCE)
            .addRuleInstance(EnumerableRules.ENUMERABLE_PROJECT_RULE)
            .build();
    HepPlanner hepPlanner = new HepPlanner(hepProgram);
    hepPlanner.setRoot(relNode);
    return hepPlanner.findBestExp();
  }

  private static RuleSet getDefaultRuleSet() {
    // Currently the merge join is unsupported
    List<RelOptRule> ruleList = new ArrayList<>();
    for (RelOptRule rule: Programs.RULE_SET) {
      if (!rule.equals(EnumerableRules.ENUMERABLE_MERGE_JOIN_RULE)  // engine currently does not support merge join
        &&!rule.equals(CoreRules.JOIN_COMMUTE)                      // this will make planning of some queries infinitely long
        &&!rule.equals(CoreRules.FILTER_INTO_JOIN))                 // filters are already pushed by EnhancedFilterJoinRule
        ruleList.add(rule);
    }
    ruleList.add(EnumerableRules.ENUMERABLE_LIMIT_SORT_RULE);
    return RuleSets.ofList(ruleList);
  }

  private static RuleSet getConvertToPhysicalRuleSet() {
    List<RelOptRule> ruleList = new ArrayList<>();
    ruleList.add(EnumerableRules.ENUMERABLE_TABLE_SCAN_RULE);
    ruleList.add(EnumerableRules.ENUMERABLE_FILTER_RULE);
    ruleList.add(EnumerableRules.ENUMERABLE_PROJECT_RULE);
    ruleList.add(EnumerableRules.ENUMERABLE_JOIN_RULE);
    ruleList.add(EnumerableRules.ENUMERABLE_AGGREGATE_RULE);
    ruleList.add(EnumerableRules.ENUMERABLE_SORT_RULE);
    ruleList.add(EnumerableRules.ENUMERABLE_LIMIT_SORT_RULE);
    ruleList.add(EnumerableRules.ENUMERABLE_LIMIT_RULE);
    return RuleSets.ofList(ruleList);
  }

  private static RelOptCluster newCluster(RelDataTypeFactory typeFactory) {
    RelOptPlanner planner = new VolcanoPlanner(RelOptCostImpl.FACTORY, Contexts.of(CalciteConnectionConfig.DEFAULT));
    planner.addRelTraitDef(ConventionTraitDef.INSTANCE);
    return RelOptCluster.create(planner, new RexBuilder(typeFactory));
  }
}
