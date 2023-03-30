package bio.terra.tanagra.underlay.datapointer;

import bio.terra.model.TableDataType;
import bio.terra.service.dataset.DatasetTable;
import bio.terra.tanagra.exception.SystemException;
import bio.terra.tanagra.query.FieldPointer;
import bio.terra.tanagra.query.FieldVariable;
import bio.terra.tanagra.query.Literal;
import bio.terra.tanagra.query.Query;
import bio.terra.tanagra.query.TablePointer;
import bio.terra.tanagra.query.TableVariable;
import bio.terra.tanagra.query.azure.AzureExecutor;
import bio.terra.tanagra.underlay.DataPointer;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import org.apache.commons.lang3.NotImplementedException;
import org.apache.commons.text.StringSubstitutor;

public final class AzureDataset extends DataPointer {
  private static final String TABLE_SQL =
      "OPENROWSET(BULK 'parquet/${tableName}/*/*.parquet', DATA_SOURCE = 'ds-${datasetId}-${userName}', FORMAT='PARQUET')";
  private final UUID datasetId;
  private final String datasetName;
  private final String userName;

  public AzureDataset(String name, UUID datasetId, String datasetName, String userName) {
    super(name);
    this.datasetId = Objects.requireNonNull(datasetId, "No dataset ID defined");
    this.datasetName = datasetName;
    this.userName = userName;
  }

  @Override
  public Type getType() {
    return Type.BQ_DATASET;
  }

  @Override
  public String getTableSQL(String tableName) {
    return StringSubstitutor.replace(
        TABLE_SQL, Map.of("tableName", tableName, "datasetId", datasetId, "userName", userName));
  }

  @Override
  public String getTablePathForIndexing(String tableName) {
    // API used for indexing output.
    throw new NotImplementedException();
  }

  @Override
  public Literal.DataType lookupDatatype(FieldPointer fieldPointer, AzureExecutor executor) {
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

    DatasetTable tableSchema;
    if (tablePointer.isRawSql()) {
      // If the table is a raw SQL string, then we can't fetch a table schema directly.
      // Instead, fetch a single row result and inspect the data types of that.
      TableVariable tableVar = TableVariable.forPrimary(tablePointer);
      List<TableVariable> tableVars = List.of(tableVar);
      FieldVariable fieldVarStar =
          FieldPointer.allFields(tablePointer).buildVariable(tableVar, tableVars);
      Query queryOneRow =
          new Query.Builder().select(List.of(fieldVarStar)).tables(tableVars).limit(1).build();
      //      tableSchema = getBigQueryService().getQuerySchemaWithCaching(queryOneRow.renderSQL());
      throw new NotImplementedException();
    } else {
      // If the table is not a raw SQL string, then just fetch the table schema directly.
      tableSchema = executor.getSchema(datasetId, tablePointer.getTableName());
    }

    return toDataType(tableSchema.getColumnByName(columnName).orElseThrow().getType());
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
