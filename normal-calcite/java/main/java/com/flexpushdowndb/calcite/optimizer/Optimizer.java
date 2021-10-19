package com.flexpushdowndb.calcite.optimizer;

import com.flexpushdowndb.calcite.rule.HashJoinLeftRightPosRule;
import com.flexpushdowndb.calcite.schema.SchemaImpl;
import com.flexpushdowndb.calcite.schema.SchemaReader;
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
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorUtil;
import org.apache.calcite.sql2rel.SqlToRelConverter;
import org.apache.calcite.sql2rel.StandardConvertletTable;
import org.apache.calcite.tools.Program;
import org.apache.calcite.tools.Programs;
import org.apache.calcite.tools.RuleSet;
import org.apache.calcite.tools.RuleSets;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class Optimizer {
  private static final RelOptTable.ViewExpander NOOP_EXPANDER = (type, query, schema, path) -> null;
  private final RelDataTypeFactory typeFactory;
  private final RelOptCluster cluster;
  private final RelOptPlanner planner;
  private final CalciteSchema rootSchema;

  public Optimizer() {
    this.typeFactory = new JavaTypeFactoryImpl();
    this.cluster = newCluster(typeFactory);
    this.planner = cluster.getPlanner();
    this.rootSchema = CalciteSchema.createRootSchema(false, true);
  }

  public RelNode optimize(String query, String schemaName) throws Exception {
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
    SqlValidator validator = SqlValidatorUtil.newValidator(SqlStdOperatorTable.instance(), catalogReader,
            typeFactory, SqlValidator.Config.DEFAULT);
    SqlNode validAst = validator.validate(parseAst);

    // Convert the AST into a RelNode
    SqlToRelConverter sqlToRelConverter = new SqlToRelConverter(NOOP_EXPANDER,
            validator,
            catalogReader,
            cluster,
            StandardConvertletTable.INSTANCE,
            SqlToRelConverter.config());
    RelNode logicalPlan = sqlToRelConverter.convertQuery(validAst, false, true).rel;

    // Volcano cost-based optimization
    Program program = Programs.of(RuleSets.ofList(getDefaultRuleSet()));
    RelNode volOptPlan = program.run(
            planner,
            logicalPlan,
            logicalPlan.getTraitSet().plus(EnumerableConvention.INSTANCE),
            Collections.emptyList(),
            Collections.emptyList()
    );

    // Heuristics to apply
    HepProgram hepProgram = new HepProgramBuilder()
            .addRuleInstance(HashJoinLeftRightPosRule.INSTANCE)
            .addRuleInstance(EnumerableRules.ENUMERABLE_PROJECT_RULE)
            .build();
    HepPlanner hepPlanner = new HepPlanner(hepProgram);
    hepPlanner.setRoot(volOptPlan);
    return hepPlanner.findBestExp();
  }

  private static RuleSet getDefaultRuleSet() {
    // Currently the merge join is unsupported
    List<RelOptRule> ruleList = new ArrayList<>();
    for (RelOptRule rule: Programs.RULE_SET) {
      if (!rule.equals(EnumerableRules.ENUMERABLE_MERGE_JOIN_RULE)) {
        ruleList.add(rule);
      }
    }
    return RuleSets.ofList(ruleList);
  }

  private static RelOptCluster newCluster(RelDataTypeFactory typeFactory) {
    RelOptPlanner planner = new VolcanoPlanner(RelOptCostImpl.FACTORY, Contexts.of(CalciteConnectionConfig.DEFAULT));
    planner.addRelTraitDef(ConventionTraitDef.INSTANCE);
    return RelOptCluster.create(planner, new RexBuilder(typeFactory));
  }
}
