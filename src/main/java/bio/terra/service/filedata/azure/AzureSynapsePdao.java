package bio.terra.service.filedata.azure;

import static bio.terra.common.PdaoConstant.PDAO_COUNT_COLUMN_NAME;
import static bio.terra.common.PdaoConstant.PDAO_FILTERED_ROW_COUNT_COLUMN_NAME;
import static bio.terra.common.PdaoConstant.PDAO_MAX_VALUE_COLUMN_NAME;
import static bio.terra.common.PdaoConstant.PDAO_MIN_VALUE_COLUMN_NAME;
import static bio.terra.common.PdaoConstant.PDAO_ROW_ID_COLUMN;
import static bio.terra.common.PdaoConstant.PDAO_ROW_ID_PARQUET_NAME;
import static bio.terra.common.PdaoConstant.PDAO_ROW_ID_TABLE;
import static bio.terra.common.PdaoConstant.PDAO_TABLE_ID_COLUMN;
import static bio.terra.common.PdaoConstant.PDAO_TOTAL_ROW_COUNT_COLUMN_NAME;

import bio.terra.app.configuration.ApplicationConfiguration;
import bio.terra.common.CollectionType;
import bio.terra.common.Column;
import bio.terra.common.SynapseColumn;
import bio.terra.common.Table;
import bio.terra.common.exception.PdaoException;
import bio.terra.grammar.Query;
import bio.terra.model.ColumnStatisticsDoubleModel;
import bio.terra.model.ColumnStatisticsIntModel;
import bio.terra.model.ColumnStatisticsTextModel;
import bio.terra.model.ColumnStatisticsTextValue;
import bio.terra.model.IngestRequestModel.FormatEnum;
import bio.terra.model.SnapshotRequestAssetModel;
import bio.terra.model.SnapshotRequestRowIdModel;
import bio.terra.model.SnapshotRequestRowIdTableModel;
import bio.terra.model.SqlSortDirection;
import bio.terra.model.TableDataType;
import bio.terra.service.dataset.AssetColumn;
import bio.terra.service.dataset.AssetSpecification;
import bio.terra.service.dataset.AssetTable;
import bio.terra.service.dataset.DatasetTable;
import bio.terra.service.dataset.exception.InvalidColumnException;
import bio.terra.service.dataset.exception.TableNotFoundException;
import bio.terra.service.dataset.flight.ingest.IngestUtils;
import bio.terra.service.filedata.DrsId;
import bio.terra.service.filedata.DrsIdService;
import bio.terra.service.resourcemanagement.azure.AzureResourceConfiguration;
import bio.terra.service.resourcemanagement.exception.AzureResourceException;
import bio.terra.service.snapshot.Snapshot;
import bio.terra.service.snapshot.SnapshotTable;
import bio.terra.service.snapshot.exception.InvalidSnapshotException;
import bio.terra.service.tabulardata.WalkRelationship;
import com.azure.core.credential.AzureSasCredential;
import com.azure.storage.blob.BlobUrlParts;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.annotations.VisibleForTesting;
import com.microsoft.sqlserver.jdbc.SQLServerDataSource;
import com.microsoft.sqlserver.jdbc.SQLServerException;
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
import org.apache.commons.collections4.ListUtils;
import org.apache.commons.lang3.NotImplementedException;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.dao.DataAccessException;
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
      """
          IF EXISTS (SELECT * FROM sys.database_scoped_credentials WHERE name = '<scopedCredentialName>')
              ALTER DATABASE SCOPED CREDENTIAL [<scopedCredentialName>]
              WITH IDENTITY = 'SHARED ACCESS SIGNATURE',
                   SECRET = '<secret>' ;
          ELSE
              CREATE DATABASE SCOPED CREDENTIAL [<scopedCredentialName>]
              WITH IDENTITY = 'SHARED ACCESS SIGNATURE',
                   SECRET = '<secret>' ;""";

  private static final String dataSourceCreateTemplate =
      """
      IF NOT EXISTS (SELECT * FROM sys.external_data_sources WHERE name = '<dataSourceName>')
      CREATE EXTERNAL DATA SOURCE [<dataSourceName>]
          WITH (
              LOCATION = '<scheme>://<host>/<container>',
              CREDENTIAL = [<credential>]
          );""";

  private static final String createSnapshotTableTemplate =
      """
      CREATE EXTERNAL TABLE [<tableName>]
          WITH (
              LOCATION = '<destinationParquetFile>',
              DATA_SOURCE = [<destinationDataSourceName>], /* metadata container */
              FILE_FORMAT = [<fileFormat>]
          ) AS SELECT    datarepo_row_id,
                 <columns:{c|
                    <if(c.isFileType)>
                       <if(c.arrayOf)>
                         <if(isGlobalFileIds)>
                           (SELECT '[' + STRING_AGG('"drs://<drsLocator>v2_' + [file_id] + '"', ',') + ']' FROM OPENJSON([<c.name>]) WITH ([file_id] VARCHAR(36) '$') WHERE [<c.name>] != '') AS [<c.name>]
                         <else>
                           (SELECT '[' + STRING_AGG('"drs://<drsLocator>v1_<snapshotId>_' + [file_id] + '"', ',') + ']' FROM OPENJSON([<c.name>]) WITH ([file_id] VARCHAR(36) '$') WHERE [<c.name>] != '') AS [<c.name>]
                         <endif>
                       <else>
                         <if(isGlobalFileIds)>
                           'drs://<drsLocator>v2_' + [<c.name>] AS [<c.name>]
                         <else>
                           'drs://<drsLocator>v1_<snapshotId>_' + [<c.name>] AS [<c.name>]
                         <endif>
                       <endif>
                    <else>
                       <c.name> AS [<c.name>]
                    <endif>
                    }; separator=",\n">
              FROM OPENROWSET(
                 BULK '<ingestFileName>',
                 DATA_SOURCE = '<ingestFileDataSourceName>',
                 FORMAT = 'parquet') AS rows
              """;

  private static final String createSnapshotTableByRowIdTemplate =
      createSnapshotTableTemplate + " WHERE rows.datarepo_row_id IN (:datarepoRowIds);";

  private static final String createSnapshotTableByQueryTemplate =
      createSnapshotTableTemplate + " WHERE rows.datarepo_row_id IN (<query>);";

  private static final String createSnapshotTableArrayRootColumnClause =
      """
          <if(isRootColumnArray)>
           CROSS APPLY OPENJSON(<rootColumn>) WITH (value <arrayRootColumnType> '$')
          <endif>
          """;
  private static final String createSnapshotTableArrayFromColumnClause =
      """
          <if(isFromColumnArray)>
           CROSS APPLY OPENJSON(<fromTableColumn>) WITH (value <arrayFromColumnType> '$')
          <endif>
          """;
  private static final String createSnapshotTableByAssetTemplate =
      createSnapshotTableTemplate
          + createSnapshotTableArrayRootColumnClause
          + " WHERE <if(isRootColumnArray)>value<else>rows.<rootColumn><endif> in (:rootValues);";

  private static final String createSnapshotTableWalkRelationshipTemplate =
      createSnapshotTableTemplate
          + createSnapshotTableArrayRootColumnClause
          + """
            WHERE
            (<if(isRootColumnArray)>value<else>rows.<toTableColumn><endif> IN
            (
             SELECT <if(isFromColumnArray)>value<else><fromTableColumn><endif> FROM
                OPENROWSET(
                  BULK '<fromTableParquetFileLocation>',
                  DATA_SOURCE = '<snapshotDataSource>',
                  FORMAT = 'parquet'
                ) as from_rows
          """
          + createSnapshotTableArrayFromColumnClause
          + "   ))";

  private static final String createSnapshotTableWithExistingRowsTemplate =
      createSnapshotTableWalkRelationshipTemplate
          + """
         AND (rows.datarepo_row_id NOT IN
             (
              SELECT datarepo_row_id FROM
                  OPENROWSET(
                    BULK '<toTableParquetFileLocation>',
                    DATA_SOURCE = '<snapshotDataSource>',
                    FORMAT = 'parquet'
                  ) AS already_existing_to_rows
            ));
          """;

  private static final String createSnapshotRowIdTableTemplate =
      """
      CREATE EXTERNAL TABLE [<tableName>]
          WITH (
              LOCATION = '<destinationParquetFile>',
              DATA_SOURCE = [<destinationDataSourceName>],
              FILE_FORMAT = [<fileFormat>]) AS  <selectStatements>""";

  private static final String getLiveViewTableTemplate =
      """
      SELECT '<tableId>' as <tableIdColumn>, <dataRepoRowIdColumn>
        FROM OPENROWSET(
                 BULK '<snapshotParquetFileName>',
                 DATA_SOURCE = '<snapshotDataSourceName>',
                 FORMAT = 'parquet') AS rows""";

  private static final String mergeLiveViewTablesTemplate =
      "<selectStatements; separator=\" UNION ALL \">;";

  private static final String createTableTemplate =
      """
      CREATE EXTERNAL TABLE [<tableName>]
          WITH (
              LOCATION = '<destinationParquetFile>',
              DATA_SOURCE = [<destinationDataSourceName>],
              FILE_FORMAT = [<fileFormat>]
          ) AS SELECT
          <if(isCSV)>newid() as datarepo_row_id,
          <columns:{c|[<c.name>]}; separator=",">
          <else>
          newid() as datarepo_row_id,
          <columns:{c|
          <if(c.requiresJSONCast)>cast(JSON_VALUE(doc, '$.<c.name>') as <c.synapseDataType>) [<c.name>]
          <elseif (c.arrayOf)>cast(JSON_QUERY(doc, '$.<c.name>') as VARCHAR(8000)) [<c.name>]
          <else>JSON_VALUE(doc, '$.<c.name>') [<c.name>]
          <endif>
          }; separator=", ">
          <endif>
           FROM
              OPENROWSET(
                 BULK '<ingestFileName>',
                 DATA_SOURCE = '<controlFileDataSourceName>',
                 FORMAT = 'CSV',
          <if(isCSV)>
                 PARSER_VERSION = '<parserVersion>',
                 FIRSTROW = <firstRow>,
                 FIELDTERMINATOR = '<fieldTerminator>',
                 FIELDQUOTE = '<csvQuote>'
          <else>
                 FIELDTERMINATOR ='0x0b',
                 FIELDQUOTE = '0x0b'
          <endif>
              ) WITH (
                <if(isCSV)>
          <columns:{c|[<c.name>] <c.synapseDataType>
          <if(c.requiresCollate)> COLLATE Latin1_General_100_CI_AI_SC_UTF8<endif>
          }; separator=", ">
          <else>doc nvarchar(max)
          <endif>
          ) AS rows;""";

  private static final String countNullsInTableTemplate =
      "SELECT COUNT(DISTINCT(datarepo_row_id)) AS rows_with_nulls FROM [<tableName>] WHERE <nullChecks>;";

  private static final String createFinalParquetFilesTemplate =
      """
      CREATE EXTERNAL TABLE [<finalTableName>]
          WITH (
              LOCATION = '<destinationParquetFile>',
              DATA_SOURCE = [<destinationDataSourceName>],
              FILE_FORMAT = [<fileFormat>]
          ) AS SELECT datarepo_row_id, <columns> FROM [<scratchTableName>] <where> <nullChecks>;""";

  private static final String queryColumnsFromExternalTableTemplate =
      "SELECT DISTINCT [<refCol>] FROM [<tableName>] WHERE [<refCol>] IS NOT NULL;";

  private static final String queryArrayColumnsFromExternalTableTemplate =
      """
      SELECT DISTINCT [Element] AS [<refCol>]
          FROM [<tableName>]
          /* Note, refIds can be either UUIDs or drs ids, which is why we are extracting Element as a large value */
          CROSS APPLY OPENJSON([<refCol>]) WITH (Element VARCHAR(8000) '$') AS ARRAY_VALUES
          WHERE [<refCol>] IS NOT NULL
          AND [Element] IS NOT NULL;""";

  private static final String queryTableTotalRowCountTemplate =
      """
          SELECT count(*) <totalRowCountColumnName>
            FROM OPENROWSET(
              BULK '<parquetFileLocation>',
              DATA_SOURCE = '<datasource>',
              FORMAT='PARQUET'
            ) AS rows;
          """;
  private static final String queryFromDatasourceTemplate =
      """
      SELECT <columns:{c|tbl.[<c>]}; separator=",">,count(*) over () <filteredRowCountColumnName><if(includeTotalRowCount)>,tbl.[<totalRowCountColumnName>]<endif>
        FROM (SELECT row_number() over (order by <sort> <direction>) AS datarepo_row_number,
                 <columns:{c|all_rows.[<c>]}; separator=","><if(includeTotalRowCount)>,all_rows.[<totalRowCountColumnName>]<endif>
            FROM (
              SELECT <columns:{c|rows.[<c>]}; separator=","> <if(includeTotalRowCount)>,
              count(*) over () <totalRowCountColumnName> <endif>
              FROM OPENROWSET(BULK '<parquetFileLocation>',
                            DATA_SOURCE = '<datasource>',
                            FORMAT='PARQUET') AS rows
              ) AS all_rows
            WHERE (<userFilter>)
         ) AS tbl
      WHERE tbl.datarepo_row_number >= :offset
        AND tbl.datarepo_row_number \\<= :offset + :limit;""";

  private static final String queryTextColumnStatsTemplate =
      """
      SELECT <column>,count(*) AS <countColumn>
        FROM OPENROWSET(BULK '<parquetFileLocation>',
                        DATA_SOURCE = '<datasource>',
                        FORMAT='PARQUET') AS rows
          WHERE (<userFilter>)
          GROUP BY <column>
          ORDER BY <column> <direction>;""";

  private static final String queryNumericColumnStatsTemplate =
      """
        SELECT MIN(<column>) AS min, MAX(<column>) AS max
          FROM OPENROWSET(BULK '<parquetFileLocation>',
                          DATA_SOURCE = '<datasource>',
                          FORMAT='PARQUET') AS rows
          WHERE (<userFilter>)
          GROUP BY <column>
          ORDER BY <column> <direction>;""";
  private static final String dropTableTemplate = "DROP EXTERNAL TABLE [<resourceName>];";

  private static final String dropDataSourceTemplate =
      "DROP EXTERNAL DATA SOURCE [<resourceName>];";

  private static final String dropScopedCredentialTemplate =
      "DROP DATABASE SCOPED CREDENTIAL [<resourceName>];";

  private final ApplicationConfiguration applicationConfiguration;
  private final DrsIdService drsIdService;
  private final ObjectMapper objectMapper;

  @Autowired
  public AzureSynapsePdao(
      AzureResourceConfiguration azureResourceConfiguration,
      ApplicationConfiguration applicationConfiguration,
      DrsIdService drsIdService,
      ObjectMapper objectMapper,
      @Qualifier("synapseJdbcTemplate") NamedParameterJdbcTemplate synapseJdbcTemplate) {
    this.azureResourceConfiguration = azureResourceConfiguration;
    this.applicationConfiguration = applicationConfiguration;
    this.drsIdService = drsIdService;
    this.objectMapper = objectMapper;
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
    DrsId drsId = DrsIdService.fromUri(drsUri);
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

  public void getOrCreateExternalDataSource(
      String blobUrl, String scopedCredentialName, String dataSourceName) throws SQLException {
    getOrCreateExternalDataSource(
        BlobUrlParts.parse(blobUrl), scopedCredentialName, dataSourceName);
  }

  public void getOrCreateExternalDataSource(
      BlobUrlParts signedBlobUrl, String scopedCredentialName, String dataSourceName)
      throws NotImplementedException, SQLException {
    AzureSasCredential blobContainerSasTokenCreds =
        new AzureSasCredential(signedBlobUrl.getCommonSasQueryParameters().encode());

    String credentialName = sanitizeStringForSql(scopedCredentialName);
    String dsName = sanitizeStringForSql(dataSourceName);
    ST sqlScopedCredentialCreateTemplate = new ST(scopedCredentialCreateTemplate);
    sqlScopedCredentialCreateTemplate.add("scopedCredentialName", credentialName);
    sqlScopedCredentialCreateTemplate.add("secret", blobContainerSasTokenCreds.getSignature());
    executeSynapseQuery(sqlScopedCredentialCreateTemplate.render());

    ST sqlDataSourceCreateTemplate = new ST(dataSourceCreateTemplate);
    sqlDataSourceCreateTemplate.add("dataSourceName", dsName);
    sqlDataSourceCreateTemplate.add("scheme", signedBlobUrl.getScheme());
    sqlDataSourceCreateTemplate.add("host", signedBlobUrl.getHost());
    sqlDataSourceCreateTemplate.add("container", signedBlobUrl.getBlobContainerName());
    sqlDataSourceCreateTemplate.add("credential", credentialName);
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
    sqlCreateTableTemplate.add("columns", datasetTable.getSynapseColumns());
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
      String snapshotDataSourceName,
      Map<String, Long> tableRowCounts)
      throws SQLException {
    // Get all row ids from the dataset
    List<String> selectStatements = new ArrayList<>();
    for (SnapshotTable table : tables) {
      if (!tableRowCounts.containsKey(table.getName())) {
        logger.warn(
            "Table {} is not contained in the TableRowCounts map and therefore will be skipped in the snapshotRowIds parquet file.",
            table.getName());
      } else if (tableRowCounts.get(table.getName()) > 0) {
        String snapshotParquetFileName =
            IngestUtils.getSnapshotParquetFilePathForQuery(table.getName());

        ST sqlTableTemplate =
            new ST(getLiveViewTableTemplate)
                .add("tableId", table.getId().toString())
                .add("tableIdColumn", PDAO_TABLE_ID_COLUMN)
                .add("dataRepoRowIdColumn", PDAO_ROW_ID_COLUMN)
                .add("snapshotParquetFileName", snapshotParquetFileName)
                .add("snapshotDataSourceName", snapshotDataSourceName);
        selectStatements.add(sqlTableTemplate.render());
      }
    }
    if (selectStatements.isEmpty()) {
      throw new InvalidSnapshotException("Snapshot cannot be empty.");
    }
    ST sqlMergeTablesTemplate =
        new ST(mergeLiveViewTablesTemplate).add("selectStatements", selectStatements);

    // Create row id table
    String rowIdTableName = IngestUtils.formatSnapshotTableName(snapshotId, PDAO_ROW_ID_TABLE);
    String rowIdParquetFile =
        IngestUtils.getSnapshotSliceParquetFilePath(PDAO_ROW_ID_TABLE, PDAO_ROW_ID_PARQUET_NAME);
    ST sqlCreateRowIdTable =
        new ST(createSnapshotRowIdTableTemplate)
            .add("tableName", rowIdTableName)
            .add("destinationParquetFile", rowIdParquetFile)
            .add("destinationDataSourceName", snapshotDataSourceName)
            .add("fileFormat", azureResourceConfiguration.getSynapse().getParquetFileFormatName())
            .add("selectStatements", sqlMergeTablesTemplate.render());
    executeSynapseQuery(sqlCreateRowIdTable.render());
  }

  public Map<String, Long> createSnapshotParquetFilesByQuery(
      AssetSpecification assetSpec,
      UUID snapshotId,
      String datasetDataSourceName,
      String snapshotDataSourceName,
      String translatedQuery,
      boolean isGlobalFieldIds,
      String compactIdPrefix)
      throws SQLException, PdaoException {
    Map<String, Long> tableRowCounts = new HashMap<>();

    // First handle root table
    AssetTable rootTable = assetSpec.getRootTable();
    String rootTableName = rootTable.getTable().getName();

    ST sqlCreateSnapshotTableTemplate = new ST(createSnapshotTableByQueryTemplate);
    ST queryTemplate =
        generateSnapshotParquetCreateQuery(
            sqlCreateSnapshotTableTemplate,
            IngestUtils.getSourceDatasetParquetFilePath(rootTable.getTable().getName()),
            rootTable.getTable().getName(),
            snapshotId,
            IngestUtils.getSnapshotSliceParquetFilePath(rootTableName, "root"),
            datasetDataSourceName,
            snapshotDataSourceName,
            rootTable.getSynapseColumns(),
            isGlobalFieldIds,
            compactIdPrefix);

    queryTemplate.add("query", translatedQuery);
    int rows;
    try {
      rows = executeSynapseQuery(queryTemplate.render());
    } catch (DataAccessException ex) {
      throw new PdaoException(
          "Unable to create parquet file for the root table. This is most likely because the source dataset table is empty.  See exception details if this does not appear to be the case.",
          ex);
    }

    tableRowCounts.put(rootTableName, (long) rows);
    if (rows == 0) {
      throw new PdaoException(
          "Snapshot by Query - No rows included from root table due to root values that were specified. Cannot create entirely empty snapshot.");
    } else {
      logger.info("Snapshot by Query - {} rows included in root table {}", rows, rootTableName);
    }

    // Then walk relationships
    UUID rootTableId = rootTable.getTable().getId();
    List<WalkRelationship> walkRelationships = WalkRelationship.ofAssetSpecification(assetSpec);
    walkRelationships(
        snapshotId,
        assetSpec,
        datasetDataSourceName,
        snapshotDataSourceName,
        rootTableId,
        walkRelationships,
        tableRowCounts,
        isGlobalFieldIds,
        compactIdPrefix);

    return tableRowCounts;
  }

  /**
   * Create Snapshot by Asset. First, create parquet file for root table, including only rows that
   * match the root values Then, walk relationships defined by asset, creating a parquet file per
   * relationship
   *
   * @param assetSpec Asset Specification used to define the root table and which tables and columns
   *     to include in snapshot
   * @param snapshotId UUID associated with destination snapshot
   * @param datasetDataSourceName Data source associated with source data from dataset
   * @param snapshotDataSourceName Data source associated with destination snapshot
   * @param requestModel Specifies the root values which determines which rows to include in root
   *     table snapshot
   * @return tableRowCounts - hash map of snapshot table names and number of rows included in
   *     snapshot
   * @throws SQLException
   */
  public Map<String, Long> createSnapshotParquetFilesByAsset(
      AssetSpecification assetSpec,
      UUID snapshotId,
      String datasetDataSourceName,
      String snapshotDataSourceName,
      SnapshotRequestAssetModel requestModel,
      boolean isGlobalFieldIds,
      String compactIdPrefix)
      throws SQLException, PdaoException {
    Map<String, Long> tableRowCounts = new HashMap<>();

    // First handle root table
    AssetTable rootTable = assetSpec.getRootTable();
    String rootTableName = rootTable.getTable().getName();

    ST sqlCreateSnapshotTableTemplate = new ST(createSnapshotTableByAssetTemplate);
    ST queryTemplate =
        generateSnapshotParquetCreateQuery(
            sqlCreateSnapshotTableTemplate,
            IngestUtils.getSourceDatasetParquetFilePath(rootTable.getTable().getName()),
            rootTable.getTable().getName(),
            snapshotId,
            IngestUtils.getSnapshotSliceParquetFilePath(rootTableName, "root"),
            datasetDataSourceName,
            snapshotDataSourceName,
            rootTable.getSynapseColumns(),
            isGlobalFieldIds,
            compactIdPrefix);

    AssetColumn rootColumn = assetSpec.getRootColumn();

    queryTemplate.add("isRootColumnArray", rootColumn.getDatasetColumn().isArrayOf());
    queryTemplate.add(
        "arrayRootColumnType",
        SynapseColumn.translateDataType(
            rootColumn.getDatasetColumn().getType(), rootColumn.getDatasetColumn().isArrayOf()));
    queryTemplate.add("rootColumn", rootColumn.getDatasetColumn().getName());
    String query = queryTemplate.render();
    MapSqlParameterSource params =
        new MapSqlParameterSource().addValue("rootValues", requestModel.getRootValues());
    int rows;
    try {
      rows = synapseJdbcTemplate.update(query, params);
    } catch (DataAccessException ex) {
      throw new PdaoException(
          "Unable to create parquet file for the root table. This is most likely because the source dataset table is empty.  See exception details if this does not appear to be the case.",
          ex);
    }

    tableRowCounts.put(rootTableName, (long) rows);
    if (rows == 0) {
      throw new PdaoException(
          "Snapshot by Asset - No rows included from root table due to root values that were specified. Cannot create entirely empty snapshot.");
    } else {
      logger.info("Snapshot by Asset - {} rows included in root table {}", rows, rootTableName);
    }

    // Then walk relationships
    UUID rootTableId = rootTable.getTable().getId();
    List<WalkRelationship> walkRelationships = WalkRelationship.ofAssetSpecification(assetSpec);
    walkRelationships(
        snapshotId,
        assetSpec,
        datasetDataSourceName,
        snapshotDataSourceName,
        rootTableId,
        walkRelationships,
        tableRowCounts,
        isGlobalFieldIds,
        compactIdPrefix);

    return tableRowCounts;
  }

  public void walkRelationships(
      UUID snapshotId,
      AssetSpecification assetSpec,
      String datasetDataSourceName,
      String snapshotDataSourceName,
      UUID startTableId,
      List<WalkRelationship> walkRelationships,
      Map<String, Long> tableRowCounts,
      boolean isGlobalFieldIds,
      String compactIdPrefix) {
    for (WalkRelationship relationship : walkRelationships) {
      if (relationship.visitRelationship(startTableId)) {
        createSnapshotParquetFilesByRelationship(
            snapshotId,
            assetSpec,
            relationship,
            datasetDataSourceName,
            snapshotDataSourceName,
            tableRowCounts,
            isGlobalFieldIds,
            compactIdPrefix);
        walkRelationships(
            snapshotId,
            assetSpec,
            datasetDataSourceName,
            snapshotDataSourceName,
            relationship.getToTableId(),
            walkRelationships,
            tableRowCounts,
            isGlobalFieldIds,
            compactIdPrefix);
      }
    }
  }

  /**
   * Creates a parquet file for a given snapshot table based on relationship, which defines the
   * 'FROM' and 'TO' table and column. The 'TO' table is the destination snapshot table that will be
   * represented by the parquet file generated during this method. A JOIN is preformed between the
   * 'TO' and 'FROM' tables so that only rows related to the FROM table are included in the TO table
   * snapshot. The number of rows included in the snapshot is recorded in the tableRowCounts map.
   * NOTE: A table could be designated as the 'TO' table in multiple relationships from different
   * 'FROM' tables. So, this parquet file is just one of potentially many slices for the destination
   * snapshot.
   *
   * @param snapshotId UUID associated with snapshot for snapshot table
   * @param assetSpecification Asset Specification that defines which columns should be included in
   *     the snapshot table
   * @param relationship Object defining the "FROM" and "TO" snapshot table and column
   * @param datasetDataSourceName Azure data source associated with parent dataset
   * @param snapshotDataSourceName Azure data source associated with destination snapshot
   * @param tableRowCounts Map tracking the number of rows included in each table of the snapshot
   * @param isGlobalFieldIds If true, configure query to use the global drs id format
   * @param compactIdPrefix If specified, configure the query to use a compact drs id format
   * @throws SQLException
   */
  public void createSnapshotParquetFilesByRelationship(
      UUID snapshotId,
      AssetSpecification assetSpecification,
      WalkRelationship relationship,
      String datasetDataSourceName,
      String snapshotDataSourceName,
      Map<String, Long> tableRowCounts,
      boolean isGlobalFieldIds,
      String compactIdPrefix) {
    String fromTableName = relationship.getFromTableName();
    String toTableName = relationship.getToTableName();
    AssetTable toAssetTable = assetSpecification.getAssetTableByName(toTableName);
    AssetColumn toTableColumn =
        toAssetTable.getColumns().stream()
            .filter(col -> col.getDatasetColumn().getName().equals(relationship.getToColumnName()))
            .findFirst()
            .orElseThrow();
    AssetTable fromAssetTable = assetSpecification.getAssetTableByName(fromTableName);
    AssetColumn fromTableColumn =
        fromAssetTable.getColumns().stream()
            .filter(
                col -> col.getDatasetColumn().getName().equals(relationship.getFromColumnName()))
            .findFirst()
            .orElseThrow();

    // If there are no rows in the "from" table's snapshot, then there is nothing to be added
    // to the "to" snapshot table
    Long fromTableCount = tableRowCounts.get(fromTableName);
    if (fromTableCount == null || fromTableCount <= Long.valueOf(0)) {
      logger.info(
          "Snapshot by Asset - No rows included in from table {}. Parquet file for snapshot table will not be created.",
          fromTableName);
      tableRowCounts.put(toTableName, (long) 0);
      return;
    }

    ST queryTemplate =
        generateSnapshotParquetCreateQuery(
            buildSnapshotByAssetQueryTemplate(tableRowCounts, toTableName),
            IngestUtils.getSourceDatasetParquetFilePath(toTableName),
            toTableName,
            snapshotId,
            IngestUtils.getSnapshotSliceParquetFilePath(
                toTableName, String.format("%s_%s_relationship", fromTableName, toTableName)),
            datasetDataSourceName,
            snapshotDataSourceName,
            toAssetTable.getSynapseColumns(),
            isGlobalFieldIds,
            compactIdPrefix);

    queryTemplate.add("rootColumn", relationship.getToColumnName());
    queryTemplate.add("isRootColumnArray", toTableColumn.getDatasetColumn().isArrayOf());
    queryTemplate.add(
        "arrayRootColumnType",
        SynapseColumn.translateDataType(
            toTableColumn.getDatasetColumn().getType(),
            toTableColumn.getDatasetColumn().isArrayOf()));
    queryTemplate.add("isFromColumnArray", fromTableColumn.getDatasetColumn().isArrayOf());
    queryTemplate.add(
        "arrayFromColumnType",
        SynapseColumn.translateDataType(
            fromTableColumn.getDatasetColumn().getType(),
            fromTableColumn.getDatasetColumn().isArrayOf()));
    queryTemplate.add("toTableColumn", relationship.getToColumnName());
    queryTemplate.add("fromTableColumn", relationship.getFromColumnName());
    queryTemplate.add(
        "fromTableParquetFileLocation",
        IngestUtils.getSnapshotParquetFilePathForQuery(fromTableName));
    queryTemplate.add("snapshotDataSource", snapshotDataSourceName);
    queryTemplate.add(
        "toTableParquetFileLocation", IngestUtils.getSnapshotParquetFilePathForQuery(toTableName));
    String sql = queryTemplate.render();
    int rows = 0;
    try {
      rows = executeSynapseQuery(sql);
      logger.info("Snapshot by Asset - {} rows included in table {}", rows, toTableName);
    } catch (SQLException ex) {
      logger.warn(
          "No rows were added to the Snapshot for table {}. This may be because the source dataset was empty or because the rows were filtered out by the query/asset specification defined in the snapshot create request. Exception: {}",
          toTableName,
          ex.getMessage());
    }
    tableRowCounts.put(toTableName, (long) rows);
  }

  /**
   * Select query template depending on whether rows have already been added to snapshot table Since
   * there could be multiple relationships pointing to the same 'TO' table, We could run this
   * process multiple times for the same table Add clause to query to avoid rows already included in
   * snapshot
   *
   * @param tableRowCounts Map of snapshot tables and included row counts
   * @param toTableName Destination snapshot table name
   * @return String Template for creating snapshot by asset parquet query
   */
  @VisibleForTesting
  ST buildSnapshotByAssetQueryTemplate(Map<String, Long> tableRowCounts, String toTableName) {
    Long toTableRowCount = tableRowCounts.get(toTableName);
    boolean toTableAlreadyHasRows = toTableRowCount != null && toTableRowCount > Long.valueOf(0);

    return new ST(
        toTableAlreadyHasRows
            ? createSnapshotTableWithExistingRowsTemplate
            : createSnapshotTableWalkRelationshipTemplate + ";");
  }

  public Map<String, Long> createSnapshotParquetFilesByRowId(
      List<SnapshotTable> tables,
      UUID snapshotId,
      String datasetDataSourceName,
      String snapshotDataSourceName,
      SnapshotRequestRowIdModel rowIdModel,
      boolean isGlobalFileIds,
      String compactIdPrefix)
      throws SQLException, PdaoException {
    Map<String, Long> tableRowCounts = new HashMap<>();

    for (SnapshotTable table : tables) {
      ST sqlCreateSnapshotTableTemplate;
      List<SynapseColumn> columns;
      MapSqlParameterSource params;
      String query;
      // Match snapshot table - optional b/c not every table
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
                    IngestUtils.getSourceDatasetParquetFilePath(table.getName()),
                    table.getName(),
                    snapshotId,
                    IngestUtils.getSnapshotSliceParquetFilePath(table.getName(), table.getName()),
                    datasetDataSourceName,
                    snapshotDataSourceName,
                    columns,
                    isGlobalFileIds,
                    compactIdPrefix)
                .render();

        List<UUID> rowIds = rowIdTableModel.get().getRowIds();
        params = new MapSqlParameterSource().addValue("datarepoRowIds", rowIds);
      } else {
        throw new TableNotFoundException("Matching row id table not found");
      }
      int rows = 0;
      try {
        rows = synapseJdbcTemplate.update(query, params);
      } catch (DataAccessException ex) {
        logger.warn(
            "No rows were added to the Snapshot for table "
                + table.getName()
                + ". This may be because the source dataset was empty or because the rows were filtered out by the query/asset specification defined in the snapshot create request. Exception: "
                + ex.getMessage());
      }
      tableRowCounts.put(table.getName(), (long) rows);
    }
    return tableRowCounts;
  }

  public Map<String, Long> createSnapshotParquetFiles(
      List<SnapshotTable> tables,
      UUID snapshotId,
      String datasetDataSourceName,
      String snapshotDataSourceName,
      boolean isGlobalFileIds,
      String compactIdPrefix)
      throws SQLException {
    Map<String, Long> tableRowCounts = new HashMap<>();

    for (SnapshotTable table : tables) {
      ST sqlCreateSnapshotTableTemplate = new ST(createSnapshotTableTemplate);

      String query =
          generateSnapshotParquetCreateQuery(
                  sqlCreateSnapshotTableTemplate,
                  IngestUtils.getSourceDatasetParquetFilePath(table.getName()),
                  table.getName(),
                  snapshotId,
                  IngestUtils.getSnapshotSliceParquetFilePath(table.getName(), table.getName()),
                  datasetDataSourceName,
                  snapshotDataSourceName,
                  table.getSynapseColumns(),
                  isGlobalFileIds,
                  compactIdPrefix)
              .render();
      int rows = 0;
      try {
        rows = executeSynapseQuery(query);
      } catch (SQLServerException ex) {
        logger.warn(
            "No rows were added to the Snapshot for table "
                + table.getName()
                + ". This could mean that the source dataset's table is empty.",
            ex);
      }
      tableRowCounts.put(table.getName(), (long) rows);
    }
    return tableRowCounts;
  }

  private ST generateSnapshotParquetCreateQuery(
      ST sqlCreateSnapshotTableTemplate,
      String datasetParquetFileName,
      String tableName,
      UUID snapshotId,
      String snapshotParquetFileName,
      String datasetDataSourceName,
      String snapshotDataSourceName,
      List<SynapseColumn> columns,
      boolean isGlobalFileIds,
      String compactIdPrefix) {
    String snapshotTableName = IngestUtils.formatSnapshotTableName(snapshotId, tableName);

    String drsLocator;
    if (StringUtils.isEmpty(compactIdPrefix)) {
      drsLocator = applicationConfiguration.getDnsName() + "/";
    } else {
      drsLocator = compactIdPrefix + ":";
    }
    sqlCreateSnapshotTableTemplate
        .add("columns", columns)
        .add("tableName", snapshotTableName)
        .add("destinationParquetFile", snapshotParquetFileName)
        .add("destinationDataSourceName", snapshotDataSourceName)
        .add("fileFormat", azureResourceConfiguration.getSynapse().getParquetFileFormatName())
        .add("ingestFileName", datasetParquetFileName)
        .add("ingestFileDataSourceName", datasetDataSourceName)
        .add("drsLocator", drsLocator)
        .add("snapshotId", snapshotId)
        .add("isGlobalFileIds", isGlobalFileIds);

    return sqlCreateSnapshotTableTemplate;
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

  public int getTableTotalRowCount(
      String tableName, String dataSourceName, String parquetFileLocation) {
    try {
      final String sql =
          new ST(queryTableTotalRowCountTemplate)
              .add("datasource", dataSourceName)
              .add("parquetFileLocation", parquetFileLocation)
              .add("tableName", tableName)
              .add("totalRowCountColumnName", PDAO_TOTAL_ROW_COUNT_COLUMN_NAME)
              .render();
      return executeCountQuery(sql);
    } catch (SQLException ex) {
      logger.warn(
          "Unable to query the parquet file for this table. This is most likely because the table is empty.  See exception details if this does not appear to be the case.",
          ex);
      return 0;
    }
  }

  public ColumnStatisticsDoubleModel getStatsForDoubleColumn(
      Column column, String dataSourceName, String parquetFileLocation, String filter) {
    final String sql =
        queryColumnStats(
            column, dataSourceName, parquetFileLocation, filter, queryNumericColumnStatsTemplate);
    ColumnStatisticsDoubleModel doubleModel =
        (ColumnStatisticsDoubleModel)
            new ColumnStatisticsDoubleModel().dataType(column.getType().toString());
    try {
      synapseJdbcTemplate.query(
          sql,
          (rs, rowNum) ->
              doubleModel
                  .maxValue(rs.getDouble(PDAO_MAX_VALUE_COLUMN_NAME))
                  .minValue(rs.getDouble(PDAO_MIN_VALUE_COLUMN_NAME)));
    } catch (DataAccessException ex) {
      logger.warn(
          "Unable to query the parquet file for this table. This is most likely because the table is empty.  See exception details if this does not appear to be the case.",
          ex);
    }
    return doubleModel;
  }

  private String queryColumnStats(
      Column column,
      String dataSourceName,
      String parquetFileLocation,
      String filter,
      String sqlTemplate) {
    String userFilter = StringUtils.defaultIfBlank(filter, "1=1");
    String columnName = column.getName();
    return new ST(sqlTemplate)
        .add("column", columnName)
        .add("countColumn", PDAO_COUNT_COLUMN_NAME)
        .add("datasource", dataSourceName)
        .add("parquetFileLocation", parquetFileLocation)
        .add("direction", SqlSortDirection.ASC)
        .add("userFilter", userFilter)
        .render();
  }

  public ColumnStatisticsIntModel getStatsForIntColumn(
      Column column, String dataSourceName, String parquetFileLocation, String filter) {
    final String sql =
        queryColumnStats(
            column, dataSourceName, parquetFileLocation, filter, queryNumericColumnStatsTemplate);
    ColumnStatisticsIntModel intModel =
        (ColumnStatisticsIntModel)
            new ColumnStatisticsIntModel().dataType(column.getType().toString());
    try {
      synapseJdbcTemplate.query(
          sql,
          (rs, rowNum) ->
              intModel
                  .maxValue(rs.getInt(PDAO_MAX_VALUE_COLUMN_NAME))
                  .minValue(rs.getInt(PDAO_MIN_VALUE_COLUMN_NAME)));
    } catch (DataAccessException ex) {
      logger.warn(
          "Unable to query the parquet file for this table. This is most likely because the table is empty.  See exception details if this does not appear to be the case.",
          ex);
    }
    return intModel;
  }

  public ColumnStatisticsTextModel getStatsForTextColumn(
      Column column, String dataSourceName, String parquetFileLocation, String filter) {
    String userFilter = StringUtils.defaultIfBlank(filter, "1=1");
    String columnName = column.getName();
    final String sql =
        new ST(queryTextColumnStatsTemplate)
            .add("column", columnName)
            .add("countColumn", PDAO_COUNT_COLUMN_NAME)
            .add("datasource", dataSourceName)
            .add("parquetFileLocation", parquetFileLocation)
            .add("direction", SqlSortDirection.ASC)
            .add("userFilter", userFilter)
            .render();

    try {
      return (ColumnStatisticsTextModel)
          new ColumnStatisticsTextModel()
              .values(
                  synapseJdbcTemplate.query(
                      sql,
                      (rs, rowNum) ->
                          new ColumnStatisticsTextValue()
                              .value(rs.getString(columnName))
                              .count((int) rs.getLong(PDAO_COUNT_COLUMN_NAME))))
              .dataType(column.getType().toString());
    } catch (DataAccessException ex) {
      logger.warn(
          "Unable to query the parquet file for this table. This is most likely because the table is empty.  See exception details if this does not appear to be the case.",
          ex);
      return (ColumnStatisticsTextModel)
          new ColumnStatisticsTextModel().values(List.of()).dataType(column.getType().toString());
    }
  }

  public List<SynapseDataResultModel> getTableData(
      Table table,
      String tableName,
      String dataSourceName,
      String parquetFileLocation,
      int limit,
      int offset,
      String sort,
      SqlSortDirection direction,
      String filter,
      CollectionType collectionType) {

    // Ensure that the sort column is a valid column
    if (!sort.equals(PDAO_ROW_ID_COLUMN)) {
      table
          .getColumnByName(sort)
          .orElseThrow(
              () ->
                  new InvalidColumnException(
                      "Column %s was not found in the snapshot table %s"
                          .formatted(sort, tableName)));
    }
    String userFilter = StringUtils.defaultIfBlank(filter, "1=1");

    // Parse a Sql skeleton with the filter
    Query.parse("select * from schema.table where (" + userFilter + ")");

    List<Column> columns =
        ListUtils.union(
            List.of(new Column().name(PDAO_ROW_ID_COLUMN).type(TableDataType.STRING)),
            table.getColumns());
    boolean includeTotalRowCount = collectionType.equals(CollectionType.DATASET);
    final String sql =
        new ST(queryFromDatasourceTemplate)
            .add("columns", columns.stream().map(Column::getName).toList())
            .add("datasource", dataSourceName)
            .add("parquetFileLocation", parquetFileLocation)
            .add("sort", sort)
            .add("direction", direction)
            .add("userFilter", userFilter)
            .add("includeTotalRowCount", includeTotalRowCount)
            .add("totalRowCountColumnName", PDAO_TOTAL_ROW_COUNT_COLUMN_NAME)
            .add("filteredRowCountColumnName", PDAO_FILTERED_ROW_COUNT_COLUMN_NAME)
            .render();

    try {
      return synapseJdbcTemplate.query(
          sql,
          Map.of(
              "offset", offset,
              "limit", limit),
          (rs, rowNum) -> {
            SynapseDataResultModel resultModel =
                new SynapseDataResultModel()
                    .rowResult(
                        columns.stream()
                            .collect(
                                Collectors.toMap(
                                    Column::getName,
                                    c -> Optional.ofNullable(extractValue(rs, c)))))
                    .filteredCount(rs.getInt(PDAO_FILTERED_ROW_COUNT_COLUMN_NAME));
            if (includeTotalRowCount) {
              resultModel.totalCount(rs.getInt(PDAO_TOTAL_ROW_COUNT_COLUMN_NAME));
            }
            return resultModel;
          });
    } catch (DataAccessException ex) {
      logger.warn(
          "Unable to query the parquet file for this table. This is most likely because the table is empty.  See exception details if this does not appear to be the case.",
          ex);
      return new ArrayList<>();
    }
  }

  public int executeSynapseQuery(String query) throws SQLException {
    SQLServerDataSource ds = getDatasource();
    try (Connection connection = ds.getConnection();
        Statement statement = connection.createStatement()) {
      logQuery(query);
      statement.execute(query);
      return statement.getUpdateCount();
    }
  }

  public int executeCountQuery(String query) throws SQLException {
    SQLServerDataSource ds = getDatasource();
    try (Connection connection = ds.getConnection();
        Statement statement = connection.createStatement()) {
      logQuery(query);
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

  public static String getCredentialName(UUID collectionId, String email) {
    return "cred-%s-%s".formatted(collectionId, email);
  }

  public static String getDataSourceName(UUID collectionId, String email) {
    return "ds-%s-%s".formatted(collectionId, email);
  }

  private String sanitizeStringForSql(String value) {
    return value.replaceAll("['\\[\\]]", "");
  }

  private Object extractValue(ResultSet resultSet, Column column) {
    if (!column.isArrayOf()) {
      try {
        return switch (column.getType()) {
          case BOOLEAN -> resultSet.getBoolean(column.getName());
          case BYTES -> resultSet.getBytes(column.getName());
          case DIRREF, FILEREF, STRING, TEXT, DATE, DATETIME, TIMESTAMP -> resultSet.getString(
              column.getName());
          case FLOAT -> resultSet.getFloat(column.getName());
          case FLOAT64 -> resultSet.getDouble(column.getName());
          case INTEGER -> resultSet.getInt(column.getName());
          case INT64 -> resultSet.getLong(column.getName());
          case NUMERIC -> resultSet.getFloat(column.getName());
          case TIME -> resultSet.getTime(column.getName());
          default -> throw new IllegalArgumentException(
              "Unknown datatype '" + column.getType() + "'");
        };
      } catch (SQLException e) {
        throw new PdaoException("Error reading data", e);
      }
    } else {
      String rawValue = null;
      try {
        rawValue = resultSet.getString(column.getName());
      } catch (SQLException e) {
        throw new PdaoException("Error reading data", e);
      }
      if (rawValue == null) {
        return null;
      }
      if (StringUtils.isBlank(rawValue)) {
        return "";
      }
      TypeReference<?> targetType =
          switch (column.getType()) {
            case BOOLEAN -> new TypeReference<List<Boolean>>() {};
            case DATE,
                DATETIME,
                DIRREF,
                FILEREF,
                STRING,
                TEXT,
                TIME,
                TIMESTAMP -> new TypeReference<List<String>>() {};
            case FLOAT, FLOAT64, INTEGER, INT64, NUMERIC -> new TypeReference<List<Number>>() {};
            default -> throw new IllegalArgumentException(
                "Unknown datatype '" + column.getType() + "'");
          };
      try {
        return objectMapper.readValue(rawValue, targetType);
      } catch (JsonProcessingException e) {
        throw new PdaoException("Could not deserialize value %s".formatted(rawValue), e);
      }
    }
  }

  public static void logQuery(String query) {
    logger.debug("Running query:\n#########\n{}", query);
  }
}
