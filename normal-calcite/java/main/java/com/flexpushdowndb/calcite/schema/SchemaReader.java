package com.flexpushdowndb.calcite.schema;

import com.flexpushdowndb.util.FileUtils;
import org.apache.calcite.schema.Table;
import org.apache.calcite.sql.type.SqlTypeName;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import java.io.FileReader;
import java.io.IOException;
import java.math.BigDecimal;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;

public class SchemaReader {
  private static final Path metadataRootPath = Paths.get(System.getProperty("user.dir"))
          .getParent()
          .getParent()
          .resolve("resources/metadata");

  public static SchemaImpl readSchema(String schemaName) throws Exception {
    Path schemaDirPath = metadataRootPath.resolve(schemaName);

    // Read schema
    Path schemaPath = schemaDirPath.resolve("schemas");
    Map<String, Map<String, SqlTypeName>> fieldTypesMap = readFieldTypes(schemaPath);

    // Read stats
    Path statsPath = schemaDirPath.resolve("stats.json");
    Map<String, Double> rowCounts = readRowCounts(statsPath);

    // Construct tables
    Map<String, Table> tableMap = new HashMap<>();
    for (String tableName: fieldTypesMap.keySet()) {
      Map<String, SqlTypeName> fieldTypes = fieldTypesMap.get(tableName);
      double rowCount = rowCounts.get(tableName);
      tableMap.put(tableName, new TableImpl(tableName, fieldTypes, rowCount));
    }

    return new SchemaImpl(schemaName, tableMap);
  }

  private static Map<String, Map<String, SqlTypeName>> readFieldTypes(Path schemaPath) throws Exception {
    Map<String, Map<String, SqlTypeName>> fieldTypesMap = new HashMap<>();
    for (String line: FileUtils.readFileByLine(schemaPath)) {
      Map<String, SqlTypeName> fieldTypes = new HashMap<>();
      String[] lineSplits = line.split(":");
      String tableName = lineSplits[0];
      for (String fieldTypeStr: lineSplits[1].split(",")) {
        String[] fieldTypeStrSplits = fieldTypeStr.split("/");
        String fieldName = fieldTypeStrSplits[0];
        SqlTypeName fieldType = stringToSqlTypeName(fieldTypeStrSplits[1]);
        fieldTypes.put(fieldName, fieldType);
      }
      fieldTypesMap.put(tableName, fieldTypes);
    }
    return fieldTypesMap;
  }

  private static Map<String, Double> readRowCounts(Path statsPath) throws IOException, ParseException {
    Map<String, Double> rowCounts = new HashMap<>();
    Object obj = new JSONParser().parse(new FileReader(statsPath.toFile()));
    JSONObject jObj = (JSONObject) obj;
    for (Object o: (JSONArray) jObj.get("tables")) {
      JSONObject tableStats = (JSONObject) o;
      String tableName = (String) tableStats.get("name");
      JSONObject statsMap = (JSONObject) tableStats.get("stats");
      long rowCount = (long) statsMap.get("rowCount");
      double castRowCount = BigDecimal.valueOf(rowCount).doubleValue();
      rowCounts.put(tableName, castRowCount);
    }
    return rowCounts;
  }

  private static SqlTypeName stringToSqlTypeName(String typeString) {
    switch (typeString) {
      case "int32":
        return SqlTypeName.INTEGER;
      case "int64":
        return SqlTypeName.BIGINT;
      case "utf8":
        return SqlTypeName.VARCHAR;
      case "boolean":
        return SqlTypeName.BOOLEAN;
      default:
        throw new UnsupportedOperationException("Unsupported field type: " + typeString);
    }
  }
}
