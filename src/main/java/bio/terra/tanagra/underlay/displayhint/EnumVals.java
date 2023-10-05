package bio.terra.tanagra.underlay.displayhint;

import bio.terra.tanagra.query.CellValue;
import bio.terra.tanagra.query.ColumnHeaderSchema;
import bio.terra.tanagra.query.ColumnSchema;
import bio.terra.tanagra.query.FieldPointer;
import bio.terra.tanagra.query.FieldVariable;
import bio.terra.tanagra.query.Literal;
import bio.terra.tanagra.query.OrderByVariable;
import bio.terra.tanagra.query.Query;
import bio.terra.tanagra.query.QueryExecutor;
import bio.terra.tanagra.query.QueryRequest;
import bio.terra.tanagra.query.QueryResult;
import bio.terra.tanagra.query.RowResult;
import bio.terra.tanagra.query.TablePointer;
import bio.terra.tanagra.query.TableVariable;
import bio.terra.tanagra.underlay.DataPointer;
import bio.terra.tanagra.underlay.DisplayHint;
import bio.terra.tanagra.underlay.ValueDisplay;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class EnumVals extends DisplayHint {
  private static final Logger LOGGER = LoggerFactory.getLogger(EnumVals.class);
  private static final String ENUM_VALUE_COLUMN_ALIAS = "enumVal";
  private static final String ENUM_COUNT_COLUMN_ALIAS = "enumCount";
  private static final String ENUM_DISPLAY_COLUMN_ALIAS = "enumDisplay";
  private static final int MAX_ENUM_VALS_FOR_DISPLAY_HINT = 100;

  private final List<EnumVal> enumValsList;

  public EnumVals(List<EnumVal> enumValsList) {
    super(Type.ENUM);
    this.enumValsList = enumValsList;
  }

  @Override
  public List<EnumVal> getEnumValsList() {
    return Collections.unmodifiableList(enumValsList);
  }

  /**
   * Build a query to fetch a set of distinct values, up to the maximum allowed. e.g.
   *
   * <p>SELECT c.standard_concept AS enumVal, count(*) AS enumCount
   *
   * <p>FROM concept AS c
   *
   * <p>GROUP BY c.standard_concept
   *
   * <p>ORDER BY c.standard_concept
   *
   * <p>LIMIT 101
   */
  public static EnumVals computeForField(
      Literal.DataType dataType, FieldPointer value, QueryExecutor executor) {
    Query query = queryPossibleEnumVals(value);
    List<ColumnSchema> columnSchemas =
        List.of(
            new ColumnSchema(
                ENUM_VALUE_COLUMN_ALIAS, CellValue.SQLDataType.fromUnderlayDataType(dataType)),
            new ColumnSchema(ENUM_COUNT_COLUMN_ALIAS, CellValue.SQLDataType.INT64));

    QueryRequest queryRequest = new QueryRequest(query, new ColumnHeaderSchema(columnSchemas));
    QueryResult queryResult = executor.execute(queryRequest);

    List<EnumVal> enumVals = new ArrayList<>();
    for (RowResult rowResult : queryResult.rowResults()) {
      String val = rowResult.get(ENUM_VALUE_COLUMN_ALIAS).getString().orElse(null);
      long count = rowResult.get(ENUM_COUNT_COLUMN_ALIAS).getLong().getAsLong();
      enumVals.add(new EnumVal(new ValueDisplay(val), count));
      if (enumVals.size() > MAX_ENUM_VALS_FOR_DISPLAY_HINT) {
        // if there are more than the max number of values, then skip the display hint
        LOGGER.info(
            "Skipping enum values display hint because there are >{} possible values: {}",
            MAX_ENUM_VALS_FOR_DISPLAY_HINT,
            value.getColumnName());
        return null;
      }
    }
    if (enumVals.isEmpty()) {
      return null;
    }
    return new EnumVals(enumVals);
  }

  /**
   * Build a query to fetch a set of distinct values and their display strings, up to the maximum
   * allowed. e.g.
   *
   * <p>SELECT x.enumVal, x.enumCount, v.vocabulary_name AS enumDisplay
   *
   * <p>FROM (SELECT c.vocabulary_id AS enumVal, count(*) AS enumCount FROM concept GROUP BY
   * c.vocabulary_id ORDER BY c.vocabulary_id) AS x
   *
   * <p>JOIN vocabulary as v
   *
   * <p>ON v.id = x.enumVal
   *
   * <p>GROUP BY x.enumVal
   *
   * <p>ORDER BY x.enumVal
   *
   * <p>LIMIT 101
   */
  public static EnumVals computeForField(
      Literal.DataType dataType, FieldPointer value, FieldPointer display, QueryExecutor executor) {
    Query possibleValuesQuery = queryPossibleEnumVals(value);
    DataPointer dataPointer = value.getTablePointer().dataPointer();
    TablePointer possibleValsTable =
        TablePointer.fromRawSql(dataPointer, executor.renderSQL(possibleValuesQuery));
    FieldPointer possibleValField =
        new FieldPointer.Builder()
            .tablePointer(possibleValsTable)
            .columnName(ENUM_VALUE_COLUMN_ALIAS)
            .build();
    FieldPointer possibleCountField =
        new FieldPointer.Builder()
            .tablePointer(possibleValsTable)
            .columnName(ENUM_COUNT_COLUMN_ALIAS)
            .build();
    FieldPointer possibleDisplayField =
        new FieldPointer.Builder()
            .tablePointer(possibleValsTable)
            .columnName(ENUM_VALUE_COLUMN_ALIAS)
            .foreignTablePointer(display.getForeignTablePointer())
            .foreignKeyColumnName(display.getForeignKeyColumnName())
            .foreignColumnName(display.getForeignColumnName())
            .sqlFunctionWrapper(display.getSqlFunctionWrapper())
            .build();

    // build the outer query for the list of (possible value, display) pairs
    List<TableVariable> tables = new ArrayList<>();
    TableVariable primaryTable = TableVariable.forPrimary(possibleValsTable);
    tables.add(primaryTable);

    FieldVariable valueFieldVar =
        possibleValField.buildVariable(primaryTable, tables, ENUM_VALUE_COLUMN_ALIAS);
    FieldVariable countFieldVar =
        possibleCountField.buildVariable(primaryTable, tables, ENUM_COUNT_COLUMN_ALIAS);
    FieldVariable displayFieldVar =
        possibleDisplayField.buildVariable(primaryTable, tables, ENUM_DISPLAY_COLUMN_ALIAS);
    Query query =
        new Query(
            List.of(valueFieldVar, countFieldVar, displayFieldVar),
            tables,
            List.of(new OrderByVariable(displayFieldVar)),
            MAX_ENUM_VALS_FOR_DISPLAY_HINT + 1);

    LOGGER.info(
        "SQL data type of value is: {}", CellValue.SQLDataType.fromUnderlayDataType(dataType));
    List<ColumnSchema> columnSchemas =
        List.of(
            new ColumnSchema(
                ENUM_VALUE_COLUMN_ALIAS, CellValue.SQLDataType.fromUnderlayDataType(dataType)),
            new ColumnSchema(ENUM_COUNT_COLUMN_ALIAS, CellValue.SQLDataType.INT64),
            new ColumnSchema(ENUM_DISPLAY_COLUMN_ALIAS, CellValue.SQLDataType.STRING));

    // run the query
    QueryRequest queryRequest = new QueryRequest(query, new ColumnHeaderSchema(columnSchemas));
    QueryResult queryResult = executor.execute(queryRequest);

    // iterate through the query results, building the list of enum values
    List<EnumVal> enumVals = new ArrayList<>();
    for (RowResult rowResult : queryResult.rowResults()) {
      CellValue cellValue = rowResult.get(ENUM_VALUE_COLUMN_ALIAS);
      enumVals.add(
          new EnumVal(
              new ValueDisplay(
                  // TODO: Make a static NULL Literal instance, instead of overloading the String
                  // value.
                  cellValue.getLiteral().orElse(new Literal(null)),
                  rowResult.get(ENUM_DISPLAY_COLUMN_ALIAS).getString().orElse(null)),
              rowResult.get(ENUM_COUNT_COLUMN_ALIAS).getLong().getAsLong()));
      if (enumVals.size() > MAX_ENUM_VALS_FOR_DISPLAY_HINT) {
        // if there are more than the max number of values, then skip the display hint
        LOGGER.info(
            "Skipping enum values display hint because there are >{} possible values: {}",
            MAX_ENUM_VALS_FOR_DISPLAY_HINT,
            valueFieldVar.getAlias());
        return null;
      }
    }
    if (enumVals.isEmpty()) {
      return null;
    }
    return new EnumVals(enumVals);
  }

  private static Query queryPossibleEnumVals(FieldPointer value) {
    List<TableVariable> nestedQueryTables = new ArrayList<>();
    TableVariable nestedPrimaryTable = TableVariable.forPrimary(value.getTablePointer());
    nestedQueryTables.add(nestedPrimaryTable);

    FieldVariable nestedValueFieldVar =
        value.buildVariable(nestedPrimaryTable, nestedQueryTables, ENUM_VALUE_COLUMN_ALIAS);

    FieldPointer countFieldPointer = value.toBuilder().sqlFunctionWrapper("COUNT").build();
    FieldVariable nestedCountFieldVar =
        countFieldPointer.buildVariable(
            nestedPrimaryTable, nestedQueryTables, ENUM_COUNT_COLUMN_ALIAS);

    return new Query(
        List.of(nestedValueFieldVar, nestedCountFieldVar),
        nestedQueryTables,
        List.of(new OrderByVariable(nestedValueFieldVar)),
        List.of(nestedValueFieldVar),
        MAX_ENUM_VALS_FOR_DISPLAY_HINT + 1);
  }
}
