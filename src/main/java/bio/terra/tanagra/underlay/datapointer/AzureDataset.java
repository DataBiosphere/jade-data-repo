package bio.terra.tanagra.underlay.datapointer;

import bio.terra.common.Column;
import bio.terra.model.TableDataType;
import bio.terra.tanagra.exception.SystemException;
import bio.terra.tanagra.query.CellValue;
import bio.terra.tanagra.query.FieldPointer;
import bio.terra.tanagra.query.FieldVariable;
import bio.terra.tanagra.query.Literal;
import bio.terra.tanagra.query.Query;
import bio.terra.tanagra.query.QueryExecutor;
import bio.terra.tanagra.query.TablePointer;
import bio.terra.tanagra.query.TableVariable;
import bio.terra.tanagra.underlay.DataPointer;
import java.util.List;
import java.util.Objects;
import java.util.UUID;
import org.apache.commons.lang3.NotImplementedException;
import org.stringtemplate.v4.ST;

public final class AzureDataset extends DataPointer {
  private static final String TABLE_SQL =
      "OPENROWSET(BULK 'parquet/<tableName>/*/*.parquet', DATA_SOURCE = 'ds-<datasetId>-<userName>', FORMAT='PARQUET')";
  private final UUID datasetId;
  private final String datasetName;
  private final String userName;

  public AzureDataset(String name, UUID datasetId, String datasetName, String userName) {
    super(Type.AZURE_DATASET, name);
    this.datasetId = Objects.requireNonNull(datasetId, "No dataset ID defined");
    this.datasetName = datasetName;
    this.userName = userName;
  }

  @Override
  public String getTableSQL(String tableName) {
    return new ST(TABLE_SQL)
        .add("tableName", tableName)
        .add("datasetId", datasetId)
        .add("userName", userName)
        .render();
  }

  @Override
  public String getTablePathForIndexing(String tableName) {
    // API used for indexing output.
    throw new NotImplementedException();
  }

  @Override
  public Literal.DataType lookupDatatype(FieldPointer fieldPointer, QueryExecutor executor) {
    // If this is a foreign-key field pointer, then we want the data type of the foreign table
    // field, not the key field.
    TablePointer tablePointer =
        fieldPointer.isForeignKey()
            ? fieldPointer.getForeignTablePointer()
            : fieldPointer.getTablePointer();
    String columnName =
        fieldPointer.isForeignKey()
            ? fieldPointer.getForeignColumnName()
            : fieldPointer.getColumnName();

    if (tablePointer.isRawSql()) {
      // If the table is a raw SQL string, then we can't fetch a table schema directly.
      // Instead, fetch a single row result and inspect the data types of that.
      TableVariable tableVar = TableVariable.forPrimary(tablePointer);
      List<TableVariable> tableVars = List.of(tableVar);
      FieldVariable fieldVarStar =
          FieldPointer.allFields(tablePointer).buildVariable(tableVar, tableVars);
      Query queryOneRow =
          new Query.Builder().select(List.of(fieldVarStar)).tables(tableVars).limit(1).build();
      return executor.readTableRows(queryOneRow).stream()
          .map(row -> row.get(columnName))
          .map(CellValue::dataType)
          .map(CellValue.SQLDataType::toUnderlayDataType)
          .findFirst()
          .orElseThrow();
    } else {
      // If the table is not a raw SQL string, then just fetch the table schema directly.
      return executor
          .getSchema(datasetId, tablePointer.tableName())
          .getColumnByName(columnName)
          .map(Column::getType)
          .map(AzureDataset::toDataType)
          .orElseThrow(
              () ->
                  new RuntimeException(
                      "couldn't find %s in %s".formatted(columnName, tablePointer.tableName())));
    }
  }

  private static Literal.DataType toDataType(TableDataType fieldType) {
    return switch (fieldType) {
      case STRING, DIRREF, FILEREF, TEXT -> Literal.DataType.STRING;
      case BOOLEAN -> Literal.DataType.BOOLEAN;
      case DATE, DATETIME, TIME, TIMESTAMP -> Literal.DataType.DATE;
      case INTEGER, INT64 -> Literal.DataType.INT64;
      case FLOAT, FLOAT64, NUMERIC -> Literal.DataType.DOUBLE;
      case BYTES, RECORD -> throw new SystemException("Data type not supported: " + fieldType);
    };
  }

  public UUID getDatasetId() {
    return datasetId;
  }

  public String getDatasetName() {
    return datasetName;
  }

  public String getUserName() {
    return userName;
  }
}
