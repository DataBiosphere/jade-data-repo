package bio.terra.service.filedata.azure;

import static bio.terra.common.PdaoConstant.PDAO_ROW_ID_COLUMN;
import static bio.terra.common.PdaoConstant.PDAO_ROW_ID_TABLE;
import static bio.terra.common.PdaoConstant.PDAO_TABLE_ID_COLUMN;

import bio.terra.app.configuration.ApplicationConfiguration;
import bio.terra.common.CollectionType;
import bio.terra.common.Column;
import bio.terra.common.SynapseColumn;
import bio.terra.model.IngestRequestModel.FormatEnum;
import bio.terra.model.SnapshotRequestRowIdModel;
import bio.terra.model.SnapshotRequestRowIdTableModel;
import bio.terra.service.dataset.DatasetTable;
import bio.terra.service.dataset.exception.TableNotFoundException;
import bio.terra.service.dataset.flight.ingest.IngestUtils;
import bio.terra.service.filedata.DrsId;
import bio.terra.service.filedata.DrsIdService;
import bio.terra.service.filedata.DrsService;
import bio.terra.service.resourcemanagement.azure.AzureResourceConfiguration;
import bio.terra.service.resourcemanagement.exception.AzureResourceException;
import bio.terra.service.snapshot.Snapshot;
import bio.terra.service.snapshot.SnapshotTable;
import com.azure.core.credential.AzureSasCredential;
import com.azure.storage.blob.BlobUrlParts;
import com.google.common.annotations.VisibleForTesting;
import com.microsoft.sqlserver.jdbc.SQLServerDataSource;
import com.microsoft.sqlserver.jdbc.SQLServerException;
import java.net.URI;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;
import java.util.stream.Collectors;
import org.apache.commons.lang3.NotImplementedException;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.stereotype.Component;
import org.stringtemplate.v4.ST;

@Component
public class AzureSynapsePdao {

  private static final Logger logger = LoggerFactory.getLogger(AzureSynapsePdao.class);

  private final AzureResourceConfiguration azureResourceConfiguration;
  private final NamedParameterJdbcTemplate synapseJdbcTemplate;
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
          + ") AS SELECT    datarepo_row_id,\n"
          + "       <columns:{c|"
          + "          <if(c.isFileType)>"
          + "             <if(c.arrayOf)>"
          + "               (SELECT '[' + STRING_AGG('\"drs://<hostname>/v1_<snapshotId>_' + [file_id] + '\"', ',') + ']' FROM OPENJSON([<c.name>]) WITH ([file_id] VARCHAR(36) '$') WHERE [<c.name>] != '') AS [<c.name>]"
          + "             <else>"
          + "               'drs://<hostname>/v1_<snapshotId>_' + [<c.name>] AS [<c.name>]"
          + "             <endif>"
          + "          <else>"
          + "             <c.name> AS [<c.name>]"
          + "          <endif>"
          + "          }; separator=\",\n\">\n"
          + "    FROM OPENROWSET(\n"
          + "       BULK '<ingestFileName>',\n"
          + "       DATA_SOURCE = '<ingestFileDataSourceName>',\n"
          + "       FORMAT = 'parquet') AS rows \n";

  private static final String createSnapshotTableByRowIdTemplate =
      createSnapshotTableTemplate + "WHERE rows.datarepo_row_id IN (:datarepoRowIds);";

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
          + "       FORMAT = 'parquet') AS rows";

  private static final String mergeLiveViewTablesTemplate =
      "<selectStatements; separator=\" UNION ALL \">;";

  private static final String createTableTemplate =
      "CREATE EXTERNAL TABLE [<tableName>]\n"
          + "WITH (\n"
          + "    LOCATION = '<destinationParquetFile>',\n"
          + "    DATA_SOURCE = [<destinationDataSourceName>],\n"
          + "    FILE_FORMAT = [<fileFormat>]\n"
          + ") AS SELECT "
          + "<if(isCSV)>newid() as datarepo_row_id,\n       "
          + "<columns:{c|[<c.name>]}; separator=\",\n       \">"
          + "<else>"
          + "newid() as datarepo_row_id,\n       "
          + "<columns:{c|"
          + "<if(c.requiresJSONCast)>"
          + "cast(JSON_VALUE(doc, '$.<c.name>') as <c.synapseDataType>) [<c.name>]"
          + "<elseif (c.arrayOf)>cast(JSON_QUERY(doc, '$.<c.name>') as VARCHAR(8000)) [<c.name>]"
          + "<else>JSON_VALUE(doc, '$.<c.name>') [<c.name>]"
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
          + "<columns:{c|[<c.name>] <c.synapseDataType>"
          + "<if(c.requiresCollate)> COLLATE Latin1_General_100_CI_AI_SC_UTF8<endif>"
          + "}; separator=\",\n\">"
          + "<else>doc nvarchar(max)"
          + "<endif>\n"
          + ") AS rows;";

  private static final String countNullsInTableTemplate =
      "SELECT COUNT(DISTINCT(datarepo_row_id)) AS rows_with_nulls FROM [<tableName>] WHERE <nullChecks>;";

  private static final String createFinalParquetFilesTemplate =
      "CREATE EXTERNAL TABLE [<finalTableName>]\n"
          + "WITH (\n"
          + "    LOCATION = '<destinationParquetFile>',\n"
          + "    DATA_SOURCE = [<destinationDataSourceName>],\n"
          + "    FILE_FORMAT = [<fileFormat>]\n"
          + ") AS SELECT datarepo_row_id, <columns> FROM [<scratchTableName>] <where> <nullChecks>;";

  private static final String queryColumnsFromExternalTableTemplate =
      "SELECT DISTINCT [<refCol>] FROM [<tableName>] WHERE [<refCol>] IS NOT NULL;";

  private static final String queryArrayColumnsFromExternalTableTemplate =
      "SELECT DISTINCT [Element] AS [<refCol>] "
          + "FROM [<tableName>] "
          // Note, refIds can be either UUIDs or drs ids, which is why we are extracting Element as
          // a large value
          + "CROSS APPLY OPENJSON([<refCol>]) WITH (Element VARCHAR(8000) '$') AS ARRAY_VALUES "
          + "WHERE [<refCol>] IS NOT NULL "
          + "AND [Element] IS NOT NULL;";

  private static final String dropTableTemplate = "DROP EXTERNAL TABLE [<resourceName>];";

  private static final String dropDataSourceTemplate =
      "DROP EXTERNAL DATA SOURCE [<resourceName>];";

  private static final String dropScopedCredentialTemplate =
      "DROP DATABASE SCOPED CREDENTIAL [<resourceName>];";

  private final ApplicationConfiguration applicationConfiguration;
  private final DrsIdService drsIdService;

  @Autowired
  public AzureSynapsePdao(
      AzureResourceConfiguration azureResourceConfiguration,
      ApplicationConfiguration applicationConfiguration,
      DrsIdService drsIdService,
      @Qualifier("synapseJdbcTemplate") NamedParameterJdbcTemplate synapseJdbcTemplate) {
    this.azureResourceConfiguration = azureResourceConfiguration;
    this.applicationConfiguration = applicationConfiguration;
    this.drsIdService = drsIdService;
    this.synapseJdbcTemplate = synapseJdbcTemplate;
  }

  public List<String> getRefIds(
      String tableName, SynapseColumn refColumn, CollectionType collectionType) {

    var template =
        refColumn.isArrayOf()
            ? new ST(queryArrayColumnsFromExternalTableTemplate)
            : new ST(queryColumnsFromExternalTableTemplate);

    template.add("refCol", refColumn.getName());
    template.add("tableName", tableName);

    SQLServerDataSource ds = getDatasource();
    var query = template.render();
    try (Connection connection = ds.getConnection();
        Statement statement = connection.createStatement();
        ResultSet resultSet = statement.executeQuery(query)) {
      var refIds = new ArrayList<String>();
      switch (collectionType) {
        case SNAPSHOT:
          while (resultSet.next()) {
            extractFileIdFromDrs(resultSet.getString(refColumn.getName())).ifPresent(refIds::add);
          }
          break;
        case DATASET:
          while (resultSet.next()) {
            String refId = resultSet.getString(refColumn.getName());
            if (!StringUtils.isEmpty(refId)) {
              refIds.add(refId);
            }
          }
          break;
      }
      return refIds;
    } catch (SQLException ex) {
      throw new AzureResourceException("Could not query dataset table for fileref columns", ex);
    }
  }

  private Optional<String> extractFileIdFromDrs(final String drsUri) {
    if (StringUtils.isEmpty(drsUri)) {
      return Optional.empty();
    }
    URI uri = URI.create(drsUri);
    String fileName = DrsService.getLastNameFromPath(uri.getPath());
    DrsId drsId = drsIdService.fromObjectId(fileName);

    return Optional.of(drsId.getFsObjectId());
  }

  public List<String> getRefIdsForSnapshot(Snapshot snapshot) {
    return snapshot.getTables().stream()
        .filter(table -> table.getRowCount() > 0)
        .flatMap(
            table -> {
              String tableName =
                  IngestUtils.formatSnapshotTableName(snapshot.getId(), table.getName());
              return table.getColumns().stream()
                  .map(Column::toSynapseColumn)
                  .filter(Column::isFileOrDirRef)
                  .flatMap(
                      column ->
                          getRefIds(tableName, column, snapshot.getCollectionType()).stream());
            })
        .collect(Collectors.toList());
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

  public int createScratchParquetFiles(
      FormatEnum ingestType,
      DatasetTable datasetTable,
      String ingestFileName,
      String scratchParquetFile,
      String scratchDataSourceName,
      String controlFileDataSourceName,
      String scratchTableName,
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
    sqlCreateTableTemplate.add("tableName", scratchTableName);
    sqlCreateTableTemplate.add("destinationParquetFile", scratchParquetFile);
    sqlCreateTableTemplate.add("destinationDataSourceName", scratchDataSourceName);
    sqlCreateTableTemplate.add(
        "fileFormat", azureResourceConfiguration.getSynapse().getParquetFileFormatName());
    sqlCreateTableTemplate.add("ingestFileName", ingestFileName);
    sqlCreateTableTemplate.add("controlFileDataSourceName", controlFileDataSourceName);
    sqlCreateTableTemplate.add("columns", columns);
    return executeSynapseQuery(sqlCreateTableTemplate.render());
  }

  public int validateScratchParquetFiles(DatasetTable datasetTable, String scratchTableName)
      throws SQLException {

    String nullChecks =
        datasetTable.getColumns().stream()
            .filter(Column::isRequired)
            .map(column -> String.format("%s IS NULL", column.getName()))
            .collect(Collectors.joining(" OR "));

    if (StringUtils.isBlank(nullChecks)) {
      return 0;
    }

    ST sqlCountNullsTemplate = new ST(countNullsInTableTemplate);
    sqlCountNullsTemplate.add("tableName", scratchTableName);
    sqlCountNullsTemplate.add("nullChecks", nullChecks);

    return executeCountQuery(sqlCountNullsTemplate.render());
  }

  public long createFinalParquetFiles(
      String finalTableName,
      String destinationParquetFile,
      String destinationDataSourceName,
      String scratchTableName,
      DatasetTable datasetTable)
      throws SQLException {

    ST sqlCreateFinalParquetFilesTemplate = new ST(createFinalParquetFilesTemplate);
    sqlCreateFinalParquetFilesTemplate.add("finalTableName", finalTableName);
    sqlCreateFinalParquetFilesTemplate.add("destinationParquetFile", destinationParquetFile);
    sqlCreateFinalParquetFilesTemplate.add("destinationDataSourceName", destinationDataSourceName);
    sqlCreateFinalParquetFilesTemplate.add(
        "fileFormat", azureResourceConfiguration.getSynapse().getParquetFileFormatName());
    sqlCreateFinalParquetFilesTemplate.add("scratchTableName", scratchTableName);

    String columns =
        datasetTable.getColumns().stream()
            .map(Column::getName)
            .map(s -> String.format("[%s]", s))
            .collect(Collectors.joining(", "));

    sqlCreateFinalParquetFilesTemplate.add("columns", columns);

    String nullChecks =
        datasetTable.getColumns().stream()
            .filter(Column::isRequired)
            .map(column -> String.format("%s IS NOT NULL", column.getName()))
            .collect(Collectors.joining(" AND "));

    if (StringUtils.isBlank(nullChecks)) {
      sqlCreateFinalParquetFilesTemplate.add("where", "");
      sqlCreateFinalParquetFilesTemplate.add("nullChecks", "");
    } else {
      sqlCreateFinalParquetFilesTemplate.add("where", "WHERE");
      sqlCreateFinalParquetFilesTemplate.add("nullChecks", nullChecks);
    }

    return executeSynapseQuery(sqlCreateFinalParquetFilesTemplate.render());
  }

  public void createSnapshotRowIdsParquetFile(
      List<SnapshotTable> tables,
      UUID snapshotId,
      String datasetDataSourceName,
      String snapshotDataSourceName,
      Map<String, Long> tableRowCounts,
      String datasetFlightId)
      throws SQLException {

    // Get all row ids from the dataset
    List<String> selectStatements = new ArrayList<>();
    for (SnapshotTable table : tables) {
      if (!tableRowCounts.containsKey(table.getName())) {
        logger.warn(
            "Table {} is not contained in the TableRowCounts map and therefore will be skipped in the snapshotRowIds parquet file.",
            table.getName());
      } else if (tableRowCounts.get(table.getName()) > 0) {
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

  public Map<String, Long> createSnapshotParquetFilesByRowId(
      List<SnapshotTable> tables,
      UUID snapshotId,
      String datasetDataSourceName,
      String snapshotDataSourceName,
      String datasetFlightId,
      SnapshotRequestRowIdModel rowIdModel)
      throws SQLException {
    Map<String, Long> tableRowCounts = new HashMap<>();

    for (SnapshotTable table : tables) {
      ST sqlCreateSnapshotTableTemplate;
      List<SynapseColumn> columns;
      MapSqlParameterSource params;
      String query;
      Optional<SnapshotRequestRowIdTableModel> rowIdTableModel =
          rowIdModel.getTables().stream()
              .filter(t -> Objects.equals(t.getTableName(), table.getName()))
              .findFirst();

      if (rowIdTableModel.isPresent()) {
        List<String> columnsToInclude = rowIdTableModel.get().getColumns();
        columns =
            table.getColumns().stream()
                .filter(c -> columnsToInclude.contains(c.getName()))
                .map(Column::toSynapseColumn)
                .collect(Collectors.toList());
        sqlCreateSnapshotTableTemplate = new ST(createSnapshotTableByRowIdTemplate);
        query =
            generateSnapshotParquetCreateQuery(
                sqlCreateSnapshotTableTemplate,
                table,
                snapshotId,
                datasetDataSourceName,
                snapshotDataSourceName,
                datasetFlightId,
                columns);

        List<UUID> rowIds = rowIdTableModel.get().getRowIds();
        params = new MapSqlParameterSource().addValue("datarepoRowIds", rowIds);
      } else {
        throw new TableNotFoundException("Matching row id table not found");
      }
      int rows = synapseJdbcTemplate.update(query, params);

      tableRowCounts.put(table.getName(), (long) rows);
      if (rows == 0) {
        logger.info(
            "Unable to copy files from table {} - this usually means that the source dataset's table is empty.",
            table.getName());
      }
    }
    return tableRowCounts;
  }

  public Map<String, Long> createSnapshotParquetFiles(
      List<SnapshotTable> tables,
      UUID snapshotId,
      String datasetDataSourceName,
      String snapshotDataSourceName,
      String datasetFlightId)
      throws SQLException {
    Map<String, Long> tableRowCounts = new HashMap<>();

    for (SnapshotTable table : tables) {
      ST sqlCreateSnapshotTableTemplate = new ST(createSnapshotTableTemplate);
      List<SynapseColumn> columns =
          table.getColumns().stream().map(Column::toSynapseColumn).collect(Collectors.toList());

      String query =
          generateSnapshotParquetCreateQuery(
              sqlCreateSnapshotTableTemplate,
              table,
              snapshotId,
              datasetDataSourceName,
              snapshotDataSourceName,
              datasetFlightId,
              columns);
      try {
        int rows = executeSynapseQuery(query);
        tableRowCounts.put(table.getName(), (long) rows);
      } catch (SQLServerException ex) {
        tableRowCounts.put(table.getName(), 0L);
        logger.info(
            "Unable to copy files from table {} - this usually means that the source dataset's table is empty.",
            table.getName());
      }
    }
    return tableRowCounts;
  }

  private String generateSnapshotParquetCreateQuery(
      ST sqlCreateSnapshotTableTemplate,
      SnapshotTable table,
      UUID snapshotId,
      String datasetDataSourceName,
      String snapshotDataSourceName,
      String datasetFlightId,
      List<SynapseColumn> columns) {
    String datasetParquetFileName =
        IngestUtils.getSourceDatasetParquetFilePath(table.getName(), datasetFlightId);
    String snapshotParquetFileName =
        IngestUtils.getSnapshotParquetFilePath(snapshotId, table.getName());
    String tableName = IngestUtils.formatSnapshotTableName(snapshotId, table.getName());

    sqlCreateSnapshotTableTemplate
        .add("columns", columns)
        .add("tableName", tableName)
        .add("destinationParquetFile", snapshotParquetFileName)
        .add("destinationDataSourceName", snapshotDataSourceName)
        .add("fileFormat", azureResourceConfiguration.getSynapse().getParquetFileFormatName())
        .add("ingestFileName", datasetParquetFileName)
        .add("ingestFileDataSourceName", datasetDataSourceName)
        .add("hostname", applicationConfiguration.getDnsName())
        .add("snapshotId", snapshotId);

    return sqlCreateSnapshotTableTemplate.render();
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

  public int executeSynapseQuery(String query) throws SQLException {
    SQLServerDataSource ds = getDatasource();
    try (Connection connection = ds.getConnection();
        Statement statement = connection.createStatement()) {
      statement.execute(query);
      return statement.getUpdateCount();
    }
  }

  public int executeCountQuery(String query) throws SQLException {
    SQLServerDataSource ds = getDatasource();
    try (Connection connection = ds.getConnection();
        Statement statement = connection.createStatement()) {
      try (ResultSet resultSet = statement.executeQuery(query)) {
        resultSet.next();
        return resultSet.getInt(1);
      }
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
}
