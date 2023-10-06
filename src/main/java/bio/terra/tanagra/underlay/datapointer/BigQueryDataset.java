package bio.terra.tanagra.underlay.datapointer;

import bio.terra.tanagra.exception.SystemException;
import bio.terra.tanagra.query.CellValue;
import bio.terra.tanagra.query.FieldPointer;
import bio.terra.tanagra.query.FieldVariable;
import bio.terra.tanagra.query.Literal;
import bio.terra.tanagra.query.Query;
import bio.terra.tanagra.query.QueryExecutor;
import bio.terra.tanagra.query.TablePointer;
import bio.terra.tanagra.query.TableVariable;
import bio.terra.tanagra.query.bigquery.BigQueryExecutor;
import bio.terra.tanagra.underlay.DataPointer;
import bio.terra.tanagra.utils.GoogleBigQuery;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.bigquery.LegacySQLTypeName;
import com.google.cloud.bigquery.Schema;
import java.io.IOException;
import java.util.List;
import org.stringtemplate.v4.ST;

public final class BigQueryDataset extends DataPointer {
  private final String projectId;
  private final String datasetId;
  private final String queryProjectId;
  private final String dataflowServiceAccountEmail;
  private final String dataflowTempLocation;

  private GoogleBigQuery bigQueryService;

  public BigQueryDataset(
      String name,
      String projectId,
      String datasetId,
      String queryProjectId,
      String dataflowServiceAccountEmail,
      String dataflowTempLocation) {
    super(Type.BQ_DATASET, name);
    this.projectId = projectId;
    this.datasetId = datasetId;
    this.queryProjectId = queryProjectId;
    this.dataflowServiceAccountEmail = dataflowServiceAccountEmail;
    this.dataflowTempLocation = dataflowTempLocation;
  }

  @Override
  public String getTableSQL(String tableName) {
    return new ST("`<projectId>.<datasetId>`.<tableName>")
        .add("projectId", projectId)
        .add("datasetId", datasetId)
        .add("tableName", tableName)
        .render();
  }

  @Override
  public String getTablePathForIndexing(String tableName) {
    return new ST("<projectId>:<datasetId>.<tableName>")
        .add("projectId", projectId)
        .add("datasetId", datasetId)
        .add("tableName", tableName)
        .render();
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

    Schema tableSchema;
    if (tablePointer.isRawSql()) {
      // If the table is a raw SQL string, then we can't fetch a table schema directly.
      // Instead, fetch a single row result and inspect the data types of that.
      TableVariable tableVar = TableVariable.forPrimary(tablePointer);
      List<TableVariable> tableVars = List.of(tableVar);
      FieldVariable fieldVarStar =
          FieldPointer.allFields(tablePointer).buildVariable(tableVar, tableVars);
      Query queryOneRow = new Query(List.of(fieldVarStar), tableVars, 1);
      tableSchema = getBigQueryService().getQuerySchemaWithCaching(executor.renderSQL(queryOneRow));
    } else {
      // If the table is not a raw SQL string, then just fetch the table schema directly.
      tableSchema =
          getBigQueryService()
              .getTableSchemaWithCaching(projectId, datasetId, tablePointer.tableName());
    }

    LegacySQLTypeName fieldType = tableSchema.getFields().get(columnName).getType();
    if (LegacySQLTypeName.STRING.equals(fieldType)) {
      return Literal.DataType.STRING;
    } else if (LegacySQLTypeName.INTEGER.equals(fieldType)) {
      return Literal.DataType.INT64;
    } else if (LegacySQLTypeName.BOOLEAN.equals(fieldType)) {
      return Literal.DataType.BOOLEAN;
    } else if (LegacySQLTypeName.DATE.equals(fieldType)) {
      return Literal.DataType.DATE;
    } else if (LegacySQLTypeName.FLOAT.equals(fieldType)
        || LegacySQLTypeName.NUMERIC.equals(fieldType)) {
      return Literal.DataType.DOUBLE;
    } else {
      throw new UnsupportedOperationException("BigQuery SQL data type not supported: " + fieldType);
    }
  }

  public static LegacySQLTypeName fromSqlDataType(CellValue.SQLDataType sqlDataType) {
    return switch (sqlDataType) {
      case STRING -> LegacySQLTypeName.STRING;
      case INT64 -> LegacySQLTypeName.INTEGER;
      case BOOLEAN -> LegacySQLTypeName.BOOLEAN;
      case DATE -> LegacySQLTypeName.DATE;
      case FLOAT -> LegacySQLTypeName.FLOAT;
    };
  }

  public GoogleBigQuery getBigQueryService() {
    if (bigQueryService == null) {
      GoogleCredentials credentials;
      try {
        credentials = GoogleCredentials.getApplicationDefault();
      } catch (IOException ioEx) {
        throw new SystemException("Error loading application default credentials", ioEx);
      }
      bigQueryService = new GoogleBigQuery(credentials, queryProjectId);
    }
    return bigQueryService;
  }

  public BigQueryExecutor getQueryExecutor() {
    return new BigQueryExecutor(getBigQueryService());
  }

  public String getProjectId() {
    return projectId;
  }

  public String getDatasetId() {
    return datasetId;
  }

  public String getQueryProjectId() {
    return queryProjectId;
  }

  public String getDataflowServiceAccountEmail() {
    return dataflowServiceAccountEmail;
  }

  public String getDataflowTempLocation() {
    return dataflowTempLocation;
  }
}
