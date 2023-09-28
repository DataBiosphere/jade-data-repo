package bio.terra.tanagra.underlay.displayhint;

import bio.terra.tanagra.query.CellValue;
import bio.terra.tanagra.query.ColumnHeaderSchema;
import bio.terra.tanagra.query.ColumnSchema;
import bio.terra.tanagra.query.FieldPointer;
import bio.terra.tanagra.query.FieldVariable;
import bio.terra.tanagra.query.Query;
import bio.terra.tanagra.query.QueryExecutor;
import bio.terra.tanagra.query.QueryRequest;
import bio.terra.tanagra.query.QueryResult;
import bio.terra.tanagra.query.RowResult;
import bio.terra.tanagra.query.TablePointer;
import bio.terra.tanagra.query.TableVariable;
import bio.terra.tanagra.underlay.DataPointer;
import bio.terra.tanagra.underlay.DisplayHint;
import java.util.ArrayList;
import java.util.List;

public final class NumericRange extends DisplayHint {
  private final Double minVal;
  private final Double maxVal;

  public NumericRange(Double minVal, Double maxVal) {
    super(Type.RANGE);
    this.minVal = minVal;
    this.maxVal = maxVal;
  }

  public Double getMinVal() {
    return minVal;
  }

  public Double getMaxVal() {
    return maxVal;
  }

  public static NumericRange computeForField(FieldPointer value, QueryExecutor executor) {
    // build the nested query for the possible values
    List<TableVariable> nestedQueryTables = new ArrayList<>();
    TableVariable nestedPrimaryTable = TableVariable.forPrimary(value.getTablePointer());
    nestedQueryTables.add(nestedPrimaryTable);

    final String possibleValAlias = "possibleVal";
    FieldVariable nestedValueFieldVar =
        value.buildVariable(nestedPrimaryTable, nestedQueryTables, possibleValAlias);
    Query possibleValuesQuery =
        new Query.Builder().select(List.of(nestedValueFieldVar)).tables(nestedQueryTables).build();

    DataPointer dataPointer = value.getTablePointer().dataPointer();
    TablePointer possibleValsTable =
        TablePointer.fromRawSql(executor.renderSQL(possibleValuesQuery), dataPointer);

    // build the outer query for the list of (possible value, display) pairs
    List<TableVariable> tables = new ArrayList<>();
    TableVariable primaryTable = TableVariable.forPrimary(possibleValsTable);
    tables.add(primaryTable);

    final String minValAlias = "minVal";
    final String maxValAlias = "maxVal";

    List<FieldVariable> select = new ArrayList<>();
    FieldPointer minVal =
        new FieldPointer.Builder()
            .tablePointer(possibleValsTable)
            .columnName(possibleValAlias)
            .sqlFunctionWrapper("MIN")
            .build();
    select.add(minVal.buildVariable(primaryTable, tables, minValAlias));
    FieldPointer maxVal =
        new FieldPointer.Builder()
            .tablePointer(possibleValsTable)
            .columnName(possibleValAlias)
            .sqlFunctionWrapper("MAX")
            .build();
    select.add(maxVal.buildVariable(primaryTable, tables, maxValAlias));
    Query query = new Query.Builder().select(select).tables(tables).build();

    List<ColumnSchema> columnSchemas =
        List.of(
            new ColumnSchema(minValAlias, CellValue.SQLDataType.INT64),
            new ColumnSchema(maxValAlias, CellValue.SQLDataType.INT64));

    QueryRequest queryRequest = new QueryRequest(query, new ColumnHeaderSchema(columnSchemas));
    QueryResult queryResult = executor.execute(queryRequest);
    RowResult rowResult = queryResult.getSingleRowResult();
    return new NumericRange(
        rowResult.get(minValAlias).getDouble().getAsDouble(),
        rowResult.get(maxValAlias).getDouble().getAsDouble());
  }
}
