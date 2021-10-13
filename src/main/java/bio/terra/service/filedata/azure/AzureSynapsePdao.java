package bio.terra.service.filedata.azure;

import static bio.terra.common.PdaoConstant.PDAO_ROW_ID_COLUMN;
import static bio.terra.common.PdaoConstant.PDAO_ROW_ID_TABLE;
import static bio.terra.common.PdaoConstant.PDAO_TABLE_ID_COLUMN;

import bio.terra.common.Column;
import bio.terra.common.SynapseColumn;
import bio.terra.model.IngestRequestModel.FormatEnum;
import bio.terra.service.dataset.Dataset;
import bio.terra.service.dataset.DatasetTable;
import bio.terra.service.dataset.flight.ingest.IngestUtils;
import bio.terra.service.resourcemanagement.azure.AzureResourceConfiguration;
import bio.terra.service.resourcemanagement.exception.AzureResourceException;
import com.azure.core.credential.AzureSasCredential;
import com.azure.storage.blob.BlobUrlParts;
import com.google.common.annotations.VisibleForTesting;
import com.microsoft.sqlserver.jdbc.SQLServerDataSource;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.UUID;
import java.util.stream.Collectors;
import org.apache.commons.lang3.NotImplementedException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.stringtemplate.v4.ST;

@Component
public class AzureSynapsePdao {
  private static final Logger logger = LoggerFactory.getLogger(AzureSynapsePdao.class);

  private final AzureResourceConfiguration azureResourceConfiguration;
  private static final String PARSER_VERSION = "2.0";
  private static final String DEFAULT_CSV_FIELD_TERMINATOR = ",";
  private static final String DEFAULT_CSV_QUOTE = "\"";

  private static final String scopedCredentialCreateTemplate =
      "CREATE DATABASE SCOPED CREDENTIAL [<scopedCredentialName>]\n"
          + "WITH IDENTITY = 'SHARED ACCESS SIGNATURE',\n"
          + "SECRET = '<secret>';";

  private static final String dataSourceCreateTemplate =
      "CREATE EXTERNAL DATA SOURCE [<dataSourceName>]\n"
          + "WITH (\n"
          + "    LOCATION = '<scheme>://<host>/<container>',\n"
          + "    CREDENTIAL = [<credential>]\n"
          + ");";

  private static final String createSnapshotTableTemplate =
      "CREATE EXTERNAL TABLE [<tableName>]\n"
          + "WITH (\n"
          + "    LOCATION = '<destinationParquetFile>',\n"
          + "    DATA_SOURCE = [<destinationDataSourceName>],\n" // metadata container
          + "    FILE_FORMAT = [<fileFormat>]\n"
          + ") AS SELECT * FROM\n"
          + "    OPENROWSET(\n"
          + "       BULK '<ingestFileName>',\n"
          + "       DATA_SOURCE = '<ingestFileDataSourceName>',\n"
          + "       FORMAT = 'parquet') "
          + "WITH (<columns:{c|<c.name> <c.synapseDataType>\n"
          + " <if(c.requiresCollate)> COLLATE Latin1_General_100_CI_AI_SC_UTF8<endif>\n"
          + "}; separator=\",\n\">) AS rows;";

  private static final String createSnapshotRowIdTableTemplate =
      "CREATE EXTERNAL TABLE [<tableName>]\n"
          + "WITH (\n"
          + "    LOCATION = '<destinationParquetFile>',\n"
          + "    DATA_SOURCE = [<destinationDataSourceName>],\n"
          + "    FILE_FORMAT = [<fileFormat>]) AS  <selectStatements>";

  private static final String getLiveViewTableTemplate =
      "SELECT '<tableId>' as "
          + PDAO_TABLE_ID_COLUMN
          + ", <dataRepoRowId> FROM\n"
          + "    OPENROWSET(\n"
          + "       BULK '<datasetParquetFileName>',\n"
          + "       DATA_SOURCE = '<datasetDataSourceName>',\n"
          + "       FORMAT = 'parquet') AS rows;";

  private static final String mergeLiveViewTablesTemplate =
      "<selectStatements; separator=\" UNION ALL \">";

  private static final String createTableTemplate =
      "CREATE EXTERNAL TABLE [<tableName>]\n"
          + "WITH (\n"
          + "    LOCATION = '<destinationParquetFile>',\n"
          + "    DATA_SOURCE = [<destinationDataSourceName>],\n"
          + "    FILE_FORMAT = [<fileFormat>]\n"
          + ") AS SELECT "
          + "<if(isCSV)>newid() as datarepo_row_id,\n       "
          + "<columns:{c|<c.name>}; separator=\",\n       \">"
          + "<else>"
          + "newid() as datarepo_row_id,\n       "
          + "<columns:{c|"
          + "<if(c.requiresJSONCast)>"
          + "cast(JSON_VALUE(doc, '$.<c.name>') as <c.synapseDataType>) <c.name>"
          + "<else>JSON_VALUE(doc, '$.<c.name>') <c.name>"
          + "<endif>"
          + "}; separator=\",\n       \">\n"
          + "<endif>"
          + " FROM\n"
          + "    OPENROWSET(\n"
          + "       BULK '<ingestFileName>',\n"
          + "       DATA_SOURCE = '<controlFileDataSourceName>',\n"
          + "       FORMAT = 'CSV',\n"
          + "<if(isCSV)>"
          + "       PARSER_VERSION = '<parserVersion>',\n"
          + "       FIRSTROW = <firstRow>,\n"
          + "       FIELDTERMINATOR = '<fieldTerminator>',\n"
          + "       FIELDQUOTE = '<csvQuote>'\n"
          + "<else>"
          + "       FIELDTERMINATOR ='0x0b',\n"
          + "       FIELDQUOTE = '0x0b'\n"
          + "<endif>"
          + "    ) WITH (\n"
          + "      <if(isCSV)>"
          + "<columns:{c|<c.name> <c.synapseDataType>"
          + "<if(c.requiresCollate)> COLLATE Latin1_General_100_CI_AI_SC_UTF8<endif>"
          + "}; separator=\",\n\">"
          + "<else>doc nvarchar(max)"
          + "<endif>\n"
          + ") AS rows;";

  private static final String queryColumnsFromExternalTableTemplate =
      "SELECT DISTINCT <refCol> FROM [<tableName>] WHERE <refCol> IS NOT NULL;";

  private static final String dropTableTemplate = "DROP EXTERNAL TABLE [<resourceName>];";

  private static final String dropDataSourceTemplate =
      "DROP EXTERNAL DATA SOURCE [<resourceName>];";

  private static final String dropScopedCredentialTemplate =
      "DROP DATABASE SCOPED CREDENTIAL [<resourceName>];";

  public List<String> getRefIds(String tableName, SynapseColumn refColumn) {

    var template = new ST(queryColumnsFromExternalTableTemplate);
    template.add("refCol", refColumn.getName());
    template.add("tableName", tableName);

    SQLServerDataSource ds = getDatasource();
    var query = template.render();
    try (Connection connection = ds.getConnection();
        Statement statement = connection.createStatement();
        ResultSet resultSet = statement.executeQuery(query)) {
      var refIds = new ArrayList<String>();
      while (resultSet.next()) {
        refIds.add(resultSet.getString(refColumn.getName()));
      }
      return refIds;
    } catch (SQLException ex) {
      throw new AzureResourceException("Could not query dataset table for fileref columns", ex);
    }
  }

  public List<String> getRefIdsForDataset(Dataset dataset) {
    List<String> refIds = new ArrayList<>();
    dataset
        .getTables()
        .forEach(
            table -> {
              table.getColumns().stream()
                  .map(Column::toSynapseColumn)
                  .filter(Column::isFileOrDirRef)
                  .forEach(
                      column -> refIds.addAll(getRefIds(table.getName(), column)));
            });
    return refIds;
  }

  @Autowired
  public AzureSynapsePdao(AzureResourceConfiguration azureResourceConfiguration) {
    this.azureResourceConfiguration = azureResourceConfiguration;
  }

  public void createExternalDataSource(
      BlobUrlParts signedBlobUrl, String scopedCredentialName, String dataSourceName)
      throws NotImplementedException, SQLException {
    AzureSasCredential blobContainerSasTokenCreds =
        new AzureSasCredential(signedBlobUrl.getCommonSasQueryParameters().encode());

    ST sqlScopedCredentialCreateTemplate = new ST(scopedCredentialCreateTemplate);
    sqlScopedCredentialCreateTemplate.add("scopedCredentialName", scopedCredentialName);
    sqlScopedCredentialCreateTemplate.add("secret", blobContainerSasTokenCreds.getSignature());
    executeSynapseQuery(sqlScopedCredentialCreateTemplate.render());

    ST sqlDataSourceCreateTemplate = new ST(dataSourceCreateTemplate);
    sqlDataSourceCreateTemplate.add("dataSourceName", dataSourceName);
    sqlDataSourceCreateTemplate.add("scheme", signedBlobUrl.getScheme());
    sqlDataSourceCreateTemplate.add("host", signedBlobUrl.getHost());
    sqlDataSourceCreateTemplate.add("container", signedBlobUrl.getBlobContainerName());
    sqlDataSourceCreateTemplate.add("credential", scopedCredentialName);
    executeSynapseQuery(sqlDataSourceCreateTemplate.render());
  }

  public int createParquetFiles(
      FormatEnum ingestType,
      DatasetTable datasetTable,
      String ingestFileName,
      String destinationParquetFile,
      String destinationDataSourceName,
      String controlFileDataSourceName,
      String ingestTableName,
      Integer csvSkipLeadingRows,
      String csvFieldTerminator,
      String csvStringDelimiter)
      throws SQLException {

    List<SynapseColumn> columns =
        datasetTable.getColumns().stream()
            .map(Column::toSynapseColumn)
            .collect(Collectors.toList());
    boolean isCSV = ingestType == FormatEnum.CSV;

    ST sqlCreateTableTemplate = new ST(createTableTemplate);
    sqlCreateTableTemplate.add("isCSV", isCSV);
    if (isCSV) {
      sqlCreateTableTemplate.add("parserVersion", PARSER_VERSION);
      sqlCreateTableTemplate.add("firstRow", csvSkipLeadingRows);
      sqlCreateTableTemplate.add(
          "fieldTerminator",
          Objects.requireNonNullElse(csvFieldTerminator, DEFAULT_CSV_FIELD_TERMINATOR));
      sqlCreateTableTemplate.add(
          "csvQuote", Objects.requireNonNullElse(csvStringDelimiter, DEFAULT_CSV_QUOTE));
    }
    sqlCreateTableTemplate.add("tableName", ingestTableName);
    sqlCreateTableTemplate.add("destinationParquetFile", destinationParquetFile);
    sqlCreateTableTemplate.add("destinationDataSourceName", destinationDataSourceName);
    sqlCreateTableTemplate.add(
        "fileFormat", azureResourceConfiguration.getSynapse().getParquetFileFormatName());
    sqlCreateTableTemplate.add("ingestFileName", ingestFileName);
    sqlCreateTableTemplate.add("controlFileDataSourceName", controlFileDataSourceName);
    sqlCreateTableTemplate.add("columns", columns);
    return executeSynapseQuery(sqlCreateTableTemplate.render());
  }

  public void createSnapshotRowIdsParquetFile(
      List<DatasetTable> tables,
      UUID snapshotId,
      String datasetDataSourceName,
      String snapshotDataSourceName,
      String datasetFlightId)
      throws SQLException {

    // Get all row ids from the dataset
    List<String> selectStatements = new ArrayList<>();
    for (DatasetTable table : tables) {
      String datasetParquetFileName =
          IngestUtils.getSourceDatasetParquetFilePath(table.getName(), datasetFlightId);
      ST sqlTableTemplate =
          new ST(getLiveViewTableTemplate)
              .add("tableId", table.getId().toString())
              .add("dataRepoRowId", PDAO_ROW_ID_COLUMN)
              .add("datasetParquetFileName", datasetParquetFileName)
              .add("datasetDataSourceName", datasetDataSourceName);
      selectStatements.add(sqlTableTemplate.render());
    }
    ST sqlMergeTablesTemplate =
        new ST(mergeLiveViewTablesTemplate).add("selectStatements", selectStatements);

    // Create row id table
    String rowIdTableName = IngestUtils.formatSnapshotTableName(snapshotId, PDAO_ROW_ID_TABLE);
    String rowIdParquetFile = IngestUtils.getSnapshotParquetFilePath(snapshotId, PDAO_ROW_ID_TABLE);
    ST sqlCreateRowIdTable =
        new ST(createSnapshotRowIdTableTemplate)
            .add("tableName", rowIdTableName)
            .add("destinationParquetFile", rowIdParquetFile)
            .add("destinationDataSourceName", snapshotDataSourceName)
            .add("fileFormat", azureResourceConfiguration.getSynapse().getParquetFileFormatName())
            .add("selectStatements", sqlMergeTablesTemplate.render());
    executeSynapseQuery(sqlCreateRowIdTable.render());
  }

  public void createSnapshotParquetFiles(
      List<DatasetTable> tables,
      UUID snapshotId,
      String datasetDataSourceName,
      String snapshotDataSourceName,
      String datasetFlightId)
      throws SQLException {
    for (DatasetTable table : tables) {
      // Create a copy of each dataset "table" (parquet file)
      List<SynapseColumn> columns =
          table.getColumns().stream().map(Column::toSynapseColumn).collect(Collectors.toList());
      String datasetParquetFileName =
          IngestUtils.getSourceDatasetParquetFilePath(table.getName(), datasetFlightId);
      String snapshotParquetFileName =
          IngestUtils.getSnapshotParquetFilePath(snapshotId, table.getName());
      String tableName = IngestUtils.formatSnapshotTableName(snapshotId, table.getName());

      ST sqlCreateSnapshotTableTemplate =
          new ST(createSnapshotTableTemplate)
              .add("tableName", tableName)
              .add("destinationParquetFile", snapshotParquetFileName)
              .add("destinationDataSourceName", snapshotDataSourceName)
              .add("fileFormat", azureResourceConfiguration.getSynapse().getParquetFileFormatName())
              .add("ingestFileName", datasetParquetFileName)
              .add("ingestFileDataSourceName", datasetDataSourceName)
              .add("columns", columns);
      executeSynapseQuery(sqlCreateSnapshotTableTemplate.render());
    }
  }

  public void dropTables(List<String> tableNames) {
    cleanup(tableNames, dropTableTemplate);
  }

  public void dropDataSources(List<String> dataSourceNames) {
    cleanup(dataSourceNames, dropDataSourceTemplate);
  }

  public void dropScopedCredentials(List<String> credentialNames) {
    cleanup(credentialNames, dropScopedCredentialTemplate);
  }

  private void cleanup(List<String> resourceNames, String sql) {
    resourceNames.stream()
        .forEach(
            resource -> {
              try {
                ST sqlTemplate = new ST(sql);
                sqlTemplate.add("resourceName", resource);
                executeSynapseQuery(sqlTemplate.render());
              } catch (Exception ex) {
                logger.warn("Unable to clean up synapse resource {}.", resource, ex);
              }
            });
  }

  public int executeSynapseQuery(String query) throws SQLException {
    SQLServerDataSource ds = getDatasource();
    try (Connection connection = ds.getConnection();
        Statement statement = connection.createStatement()) {
      statement.execute(query);
      return statement.getUpdateCount();
    }
  }

  @VisibleForTesting
  public SQLServerDataSource getDatasource() {
    SQLServerDataSource ds = new SQLServerDataSource();
    ds.setServerName(azureResourceConfiguration.getSynapse().getWorkspaceName());
    ds.setUser(azureResourceConfiguration.getSynapse().getSqlAdminUser());
    ds.setPassword(azureResourceConfiguration.getSynapse().getSqlAdminPassword());
    ds.setDatabaseName(azureResourceConfiguration.getSynapse().getDatabaseName());
    return ds;
  }
}
