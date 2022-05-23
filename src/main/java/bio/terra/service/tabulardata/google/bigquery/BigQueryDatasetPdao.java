package bio.terra.service.tabulardata.google.bigquery;

import static bio.terra.common.PdaoConstant.PDAO_DELETED_AT_COLUMN;
import static bio.terra.common.PdaoConstant.PDAO_DELETED_BY_COLUMN;
import static bio.terra.common.PdaoConstant.PDAO_FLIGHT_ID_COLUMN;
import static bio.terra.common.PdaoConstant.PDAO_INGESTED_BY_COLUMN;
import static bio.terra.common.PdaoConstant.PDAO_INGEST_DATE_COLUMN_ALIAS;
import static bio.terra.common.PdaoConstant.PDAO_INGEST_TIME_COLUMN;
import static bio.terra.common.PdaoConstant.PDAO_LOAD_HISTORY_STAGING_TABLE_PREFIX;
import static bio.terra.common.PdaoConstant.PDAO_LOAD_HISTORY_TABLE;
import static bio.terra.common.PdaoConstant.PDAO_LOAD_TAG_COLUMN;
import static bio.terra.common.PdaoConstant.PDAO_ROW_ID_COLUMN;
import static bio.terra.common.PdaoConstant.PDAO_TRANSACTIONS_TABLE;
import static bio.terra.common.PdaoConstant.PDAO_TRANSACTION_ID_COLUMN;
import static bio.terra.common.PdaoConstant.PDAO_TRANSACTION_STATUS_COLUMN;
import static bio.terra.common.PdaoConstant.PDAO_TRANSACTION_TERMINATED_AT_COLUMN;

import bio.terra.app.model.GoogleCloudResource;
import bio.terra.app.model.GoogleRegion;
import bio.terra.common.Column;
import bio.terra.common.PdaoLoadStatistics;
import bio.terra.common.exception.PdaoException;
import bio.terra.common.iam.AuthenticatedUserRequest;
import bio.terra.model.BulkLoadFileState;
import bio.terra.model.BulkLoadHistoryModel;
import bio.terra.model.DataDeletionTableModel;
import bio.terra.model.IngestRequestModel;
import bio.terra.model.TableDataType;
import bio.terra.model.TransactionModel;
import bio.terra.service.dataset.BigQueryPartitionConfigV1;
import bio.terra.service.dataset.Dataset;
import bio.terra.service.dataset.DatasetTable;
import bio.terra.service.dataset.exception.ControlFileNotFoundException;
import bio.terra.service.dataset.exception.IngestFailureException;
import bio.terra.service.resourcemanagement.exception.GoogleResourceException;
import bio.terra.service.tabulardata.LoadHistoryUtil;
import bio.terra.service.tabulardata.exception.BadExternalFileException;
import bio.terra.service.tabulardata.exception.MismatchedRowIdException;
import bio.terra.service.tabulardata.google.BigQueryProject;
import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryError;
import com.google.cloud.bigquery.BigQueryException;
import com.google.cloud.bigquery.CsvOptions;
import com.google.cloud.bigquery.ExternalTableDefinition;
import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.FieldList;
import com.google.cloud.bigquery.FieldValueList;
import com.google.cloud.bigquery.FormatOptions;
import com.google.cloud.bigquery.Job;
import com.google.cloud.bigquery.JobInfo;
import com.google.cloud.bigquery.LegacySQLTypeName;
import com.google.cloud.bigquery.LoadJobConfiguration;
import com.google.cloud.bigquery.QueryJobConfiguration;
import com.google.cloud.bigquery.QueryParameterValue;
import com.google.cloud.bigquery.Schema;
import com.google.cloud.bigquery.StandardTableDefinition;
import com.google.cloud.bigquery.Table;
import com.google.cloud.bigquery.TableId;
import com.google.cloud.bigquery.TableInfo;
import com.google.cloud.bigquery.TableResult;
import com.google.cloud.bigquery.ViewDefinition;
import com.google.common.annotations.VisibleForTesting;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Component;
import org.stringtemplate.v4.ST;

@Component
@Profile("google")
public class BigQueryDatasetPdao {

  private static final Logger logger = LoggerFactory.getLogger(BigQueryDatasetPdao.class);

  public void createDataset(Dataset dataset) throws InterruptedException {
    BigQueryProject bigQueryProject = BigQueryProject.from(dataset);
    BigQuery bigQuery = bigQueryProject.getBigQuery();

    // Keep the dataset name from colliding with a snapshot name by prefixing it.
    // TODO: validate against people using the prefix for snapshots
    String datasetName = BigQueryPdao.prefixName(dataset.getName());
    try {
      // For idempotency, if we find the dataset exists, we assume that we started to
      // create it before and failed in the middle. We delete it and re-create it from scratch.
      if (bigQueryProject.datasetExists(datasetName)) {
        bigQueryProject.deleteDataset(datasetName);
      }

      GoogleRegion region =
          (GoogleRegion)
              dataset.getDatasetSummary().getStorageResourceRegion(GoogleCloudResource.BIGQUERY);

      bigQueryProject.createDataset(datasetName, dataset.getDescription(), region);
      bigQueryProject.createTable(datasetName, PDAO_LOAD_HISTORY_TABLE, buildLoadDatasetSchema());
      bigQueryProject.createTable(
          datasetName,
          PDAO_TRANSACTIONS_TABLE,
          BigQueryTransactionPdao.buildTransactionsTableSchema());

      for (DatasetTable table : dataset.getTables()) {
        createTable(bigQueryProject, bigQuery, datasetName, table);
      }

      // TODO: don't catch generic exceptions
    } catch (Exception ex) {
      throw new PdaoException("create dataset failed for " + datasetName, ex);
    }
  }

  public void createTable(
      BigQueryProject bigQueryProject, BigQuery bigQuery, String datasetName, DatasetTable table) {
    bigQueryProject.createTable(
        datasetName,
        table.getRawTableName(),
        buildSchema(table, true, true),
        table.getBigQueryPartitionConfig());
    bigQueryProject.createTable(
        datasetName, table.getSoftDeleteTableName(), buildSoftDeletesSchema());
    bigQueryProject.createTable(
        datasetName, table.getRowMetadataTableName(), buildRowMetadataSchema());
    bigQuery.create(buildLiveView(bigQueryProject.getProjectId(), datasetName, table));
  }

  public void createColumn(
      String datasetName, BigQuery bigQuery, DatasetTable table, Column column) {
    // Shamelessly pulled from the BigQuery documentation
    Table bigQueryTable = bigQuery.getTable(TableId.of(datasetName, table.getRawTableName()));
    Schema schema = bigQueryTable.getDefinition().getSchema();
    FieldList fields = schema.getFields();

    boolean fieldExists =
        fields.stream().anyMatch(field -> field.getName().equalsIgnoreCase(column.getName()));
    if (!fieldExists) {
      // Create the new field/column
      Field newField = Field.of(column.getName(), translateType(column.getType()));
      // Create a new schema adding the current fields, plus the new one
      List<Field> newFields = new ArrayList<>(fields);
      newFields.add(newField);
      Schema newSchema = Schema.of(newFields);

      // Update the table with the new schema
      Table updatedTable =
          bigQueryTable.toBuilder().setDefinition(StandardTableDefinition.of(newSchema)).build();
      updatedTable.update();
    } else {
      logger.warn(
          "Column {} already exists in table {}", column.getName(), table.getRawTableName());
    }
  }

  public void deleteColumn(
      String datasetName, BigQuery bigQuery, DatasetTable table, String columnName) {
    Table bigQueryTable = bigQuery.getTable(TableId.of(datasetName, table.getRawTableName()));
    Schema schema = bigQueryTable.getDefinition().getSchema();
    FieldList fields = schema.getFields();

    // Create a new schema adding the current fields, plus the new one
    List<Field> updatedFields =
        fields.stream().filter(f -> !f.getName().equals(columnName)).collect(Collectors.toList());
    Schema newSchema = Schema.of(updatedFields);

    // Update the table with the new schema
    Table updatedTable =
        bigQueryTable.toBuilder().setDefinition(StandardTableDefinition.of(newSchema)).build();
    updatedTable.update();
  }

  public boolean deleteDataset(Dataset dataset) throws InterruptedException {
    BigQueryProject bigQueryProject = BigQueryProject.from(dataset);
    return bigQueryProject.deleteDataset(BigQueryPdao.prefixName(dataset.getName()));
  }

  // INGESTING DATA

  // Load data
  public PdaoLoadStatistics loadToStagingTable(
      Dataset dataset,
      DatasetTable targetTable,
      String stagingTableName,
      IngestRequestModel ingestRequest,
      String path)
      throws InterruptedException {

    BigQueryProject bigQueryProject = BigQueryProject.from(dataset);
    BigQuery bigQuery = bigQueryProject.getBigQuery();
    String bqDatasetId = BigQueryPdao.prefixName(dataset.getName());
    TableId tableId = TableId.of(bqDatasetId, stagingTableName);
    LoadJobConfiguration.Builder loadBuilder =
        LoadJobConfiguration.builder(tableId, path)
            .setFormatOptions(buildFormatOptions(ingestRequest))
            .setMaxBadRecords(
                (ingestRequest.getMaxBadRecords() == null)
                    ? Integer.valueOf(0)
                    : ingestRequest.getMaxBadRecords())
            .setIgnoreUnknownValues(
                (ingestRequest.isIgnoreUnknownValues() == null)
                    ? Boolean.TRUE
                    : ingestRequest.isIgnoreUnknownValues())
            .setCreateDisposition(JobInfo.CreateDisposition.CREATE_IF_NEEDED)
            .setWriteDisposition(JobInfo.WriteDisposition.WRITE_TRUNCATE);

    // This seems like a bug in the BigQuery Java interface.
    // The null marker is CSV-only, but it cannot be set in the format,
    // so we have to special-case here. Grumble...
    if (ingestRequest.getFormat() == IngestRequestModel.FormatEnum.CSV) {
      loadBuilder.setNullMarker(
          (ingestRequest.getCsvNullMarker() == null) ? "" : ingestRequest.getCsvNullMarker());
    }
    Job loadJob;

    Schema schemaWithRowId = buildSchema(targetTable, true, false); // Source does not have row_id
    if (ingestRequest.getFormat() == IngestRequestModel.FormatEnum.CSV
        && ingestRequest.isCsvGenerateRowIds()) {
      // Ingest without the datarepo_row_id column
      Schema noRowId = buildSchema(targetTable, false, false);
      loadBuilder.setSchema(noRowId);
      loadJob = ingestData(bigQuery, path, loadBuilder.build());
      // Then add the datarepo_row_id column to the schema
      updateSchema(bigQuery, bqDatasetId, stagingTableName, schemaWithRowId, true);
    } else {
      loadBuilder.setSchema(
          schemaWithRowId); // docs say this is for target, but CLI provides one for the source
      loadJob = ingestData(bigQuery, path, loadBuilder.build());
    }

    return new PdaoLoadStatistics(loadJob.getStatistics());
  }

  private Job ingestData(BigQuery bigQuery, String path, LoadJobConfiguration configuration)
      throws InterruptedException {
    Job loadJob = bigQuery.create(JobInfo.of(configuration));
    Instant loadJobMaxTime = Instant.now().plusSeconds(TimeUnit.MINUTES.toSeconds(20L));
    while (!loadJob.isDone()) {
      logger.info(
          "Waiting for staging table load job " + loadJob.getJobId().getJob() + " to complete");
      TimeUnit.SECONDS.sleep(5L);

      if (loadJobMaxTime.isBefore(Instant.now())) {
        loadJob.cancel();
        throw new PdaoException("Staging table load failed to complete within timeout - canceled");
      }
    }
    try {
      loadJob = loadJob.reload();
    } catch (BigQueryException ex) {
      logger.info("Staging table load job " + loadJob.getJobId().getJob() + " failed: " + ex);
      if ("notFound".equals(ex.getReason())) {
        throw new ControlFileNotFoundException("Ingest source file not found: " + path);
      }

      List<String> loadErrors = new ArrayList<>();
      List<BigQueryError> bigQueryErrors = ex.getErrors();
      for (BigQueryError bigQueryError : bigQueryErrors) {
        loadErrors.add(
            "BigQueryError: reason="
                + bigQueryError.getReason()
                + " message="
                + bigQueryError.getMessage());
      }
      throw new IngestFailureException(
          "Ingest failed with " + loadErrors.size() + " errors - see error details", loadErrors);
    }
    logger.info("Staging table load job " + loadJob.getJobId().getJob() + " succeeded");
    return loadJob;
  }

  private FormatOptions buildFormatOptions(IngestRequestModel ingestRequest) {
    FormatOptions options;
    switch (ingestRequest.getFormat()) {
      case CSV:
        CsvOptions csvDefaults = FormatOptions.csv();

        options =
            CsvOptions.newBuilder()
                .setFieldDelimiter(
                    ingestRequest.getCsvFieldDelimiter() == null
                        ? csvDefaults.getFieldDelimiter()
                        : ingestRequest.getCsvFieldDelimiter())
                .setQuote(
                    ingestRequest.getCsvQuote() == null
                        ? csvDefaults.getQuote()
                        : ingestRequest.getCsvQuote())
                .setSkipLeadingRows(
                    ingestRequest.getCsvSkipLeadingRows() == null
                        ? csvDefaults.getSkipLeadingRows()
                        : ingestRequest.getCsvSkipLeadingRows())
                .setAllowQuotedNewLines(
                    ingestRequest.isCsvAllowQuotedNewlines() == null
                        ? csvDefaults.allowQuotedNewLines()
                        : ingestRequest.isCsvAllowQuotedNewlines())
                .build();
        break;

      case JSON:
      case ARRAY:
        options = FormatOptions.json();
        break;

      default:
        throw new PdaoException("Invalid format option: " + ingestRequest.getFormat());
    }
    return options;
  }

  public void createStagingLoadHistoryTable(Dataset dataset, String tableName_FlightId)
      throws InterruptedException {
    BigQueryProject bigQueryProject = BigQueryProject.from(dataset);
    try {
      String datasetName = BigQueryPdao.prefixName(dataset.getName());

      if (bigQueryProject.tableExists(
          datasetName, PDAO_LOAD_HISTORY_STAGING_TABLE_PREFIX + tableName_FlightId)) {
        bigQueryProject.deleteTable(
            datasetName, PDAO_LOAD_HISTORY_STAGING_TABLE_PREFIX + tableName_FlightId);
      }

      bigQueryProject.createTable(
          datasetName,
          PDAO_LOAD_HISTORY_STAGING_TABLE_PREFIX + tableName_FlightId,
          buildLoadDatasetSchema());
    } catch (Exception ex) {
      throw new PdaoException(
          "create staging load history table failed for " + dataset.getName(), ex);
    }
  }

  public void deleteStagingLoadHistoryTable(Dataset dataset, String flightId) {
    try {
      deleteDatasetTable(dataset, PDAO_LOAD_HISTORY_STAGING_TABLE_PREFIX + flightId);
    } catch (Exception ex) {
      throw new PdaoException(
          "create staging load history table failed for " + dataset.getName(), ex);
    }
  }

  public static final String insertLoadHistoryToStagingTableTemplate =
      "INSERT INTO `<project>.<dataset>.<stagingTable>`"
          + " (load_tag, load_time, source_name, target_path, state, file_id, checksum_crc32c, checksum_md5, error)"
          + " VALUES <load_history_array:{v|('<load_tag>', '<load_time>', '<v.sourcePath>', '<v.targetPath>',"
          + " '<v.state>', '<v.fileId>', '<v.checksumCRC>', '<v.checksumMD5>', \"\"\"<v.error>\"\"\")};"
          + " separator=\",\">";

  public void loadHistoryToStagingTable(
      Dataset dataset,
      String tableName_FlightId,
      String loadTag,
      Instant loadTime,
      List<BulkLoadHistoryModel> loadHistoryArray)
      throws InterruptedException {
    BigQueryProject bigQueryProject = BigQueryProject.from(dataset);

    ST sqlTemplate = new ST(insertLoadHistoryToStagingTableTemplate);
    sqlTemplate.add("project", bigQueryProject.getProjectId());
    sqlTemplate.add("dataset", BigQueryPdao.prefixName(dataset.getName()));
    sqlTemplate.add("stagingTable", PDAO_LOAD_HISTORY_STAGING_TABLE_PREFIX + tableName_FlightId);

    sqlTemplate.add("load_history_array", loadHistoryArray);
    sqlTemplate.add("load_tag", loadTag);
    sqlTemplate.add("load_time", loadTime);

    bigQueryProject.query(sqlTemplate.render());
  }

  private static final String mergeLoadHistoryStagingTableTemplate =
      "MERGE `<project>.<dataset>.<loadTable>` L"
          + " USING `<project>.<dataset>.<stagingTable>` S"
          + " ON S.load_tag = L.load_tag AND S.load_time = L.load_time"
          + " AND S.file_id = L.file_id"
          + " WHEN NOT MATCHED THEN"
          + " INSERT (load_tag, load_time, source_name, target_path, state, file_id, checksum_crc32c,"
          + " checksum_md5, error)"
          + " VALUES (load_tag, load_time, source_name, target_path, state, file_id, checksum_crc32c,"
          + " checksum_md5, error)";

  public void mergeStagingLoadHistoryTable(Dataset dataset, String flightId)
      throws InterruptedException {
    BigQueryProject bigQueryProject = BigQueryProject.from(dataset);

    String datasetName = BigQueryPdao.prefixName(dataset.getName());

    // Make sure load_history table exists in dataset, if not - add table
    if (!tableExists(dataset, PDAO_LOAD_HISTORY_TABLE)) {
      bigQueryProject.createTable(datasetName, PDAO_LOAD_HISTORY_TABLE, buildLoadDatasetSchema());
    }

    ST sqlTemplate = new ST(mergeLoadHistoryStagingTableTemplate);
    sqlTemplate.add("project", bigQueryProject.getProjectId());
    sqlTemplate.add("dataset", datasetName);
    sqlTemplate.add("stagingTable", PDAO_LOAD_HISTORY_STAGING_TABLE_PREFIX + flightId);
    sqlTemplate.add("loadTable", PDAO_LOAD_HISTORY_TABLE);

    bigQueryProject.query(sqlTemplate.render());
  }

  private static final String getLoadHistoryTemplate =
      "SELECT * "
          + "FROM `<project>.<dataset>.<loadTable>` L "
          + "WHERE L.load_tag = @loadTag "
          + String.format("ORDER BY %s ASC ", LoadHistoryUtil.FILE_ID_FIELD_NAME)
          + "LIMIT <limit> "
          + "OFFSET <offset> ";

  public List<BulkLoadHistoryModel> getLoadHistory(
      Dataset dataset, String loadTag, int offset, int limit) {
    BigQueryProject bigQueryProject = BigQueryProject.from(dataset);
    String datasetName = BigQueryPdao.prefixName(dataset.getName());
    var sqlTemplate = new ST(getLoadHistoryTemplate);
    sqlTemplate.add("project", bigQueryProject.getProjectId());
    sqlTemplate.add("dataset", datasetName);
    sqlTemplate.add("loadTable", PDAO_LOAD_HISTORY_TABLE);
    sqlTemplate.add("limit", limit);
    sqlTemplate.add("offset", offset);

    var query = sqlTemplate.render();
    QueryJobConfiguration queryConfig =
        QueryJobConfiguration.newBuilder(query)
            .addNamedParameter("loadTag", QueryParameterValue.string(loadTag))
            .build();

    try {
      var results = bigQueryProject.getBigQuery().query(queryConfig);
      return StreamSupport.stream(results.iterateAll().spliterator(), true)
          .map(BigQueryDatasetPdao::bigQueryResultToBulkLoadHistoryModel)
          .collect(Collectors.toList());
    } catch (InterruptedException ex) {
      throw new GoogleResourceException("Could not query BigQuery for load history", ex);
    }
  }

  private static BulkLoadHistoryModel bigQueryResultToBulkLoadHistoryModel(
      FieldValueList fieldValue) {
    return new BulkLoadHistoryModel()
        .sourcePath(fieldValue.get(LoadHistoryUtil.SOURCE_NAME_FIELD_NAME).getStringValue())
        .targetPath(fieldValue.get(LoadHistoryUtil.TARGET_PATH_FIELD_NAME).getStringValue())
        .state(
            BulkLoadFileState.fromValue(
                fieldValue.get(LoadHistoryUtil.STATE_FIELD_NAME).getStringValue()))
        .fileId(fieldValue.get(LoadHistoryUtil.FILE_ID_FIELD_NAME).getStringValue())
        .checksumCRC(bqStringValue(fieldValue, LoadHistoryUtil.CHECKSUM_CRC32C_FIELD_NAME))
        .checksumMD5(bqStringValue(fieldValue, LoadHistoryUtil.CHECKSUM_MD5_FIELD_NAME))
        .error(bqStringValue(fieldValue, LoadHistoryUtil.ERROR_FIELD_NAME));
  }

  private static final String addRowIdsToStagingTableTemplate =
      "UPDATE `<project>.<dataset>.<stagingTable>` SET "
          + PDAO_ROW_ID_COLUMN
          + " = GENERATE_UUID() WHERE "
          + PDAO_ROW_ID_COLUMN
          + " IS NULL";

  public void addRowIdsToStagingTable(Dataset dataset, String stagingTableName)
      throws InterruptedException {
    BigQueryProject bigQueryProject = BigQueryProject.from(dataset);

    ST sqlTemplate = new ST(addRowIdsToStagingTableTemplate);
    sqlTemplate.add("project", bigQueryProject.getProjectId());
    sqlTemplate.add("dataset", BigQueryPdao.prefixName(dataset.getName()));
    sqlTemplate.add("stagingTable", stagingTableName);

    bigQueryProject.query(sqlTemplate.render());
  }

  private static final String getRefIdsTemplate =
      "SELECT <refCol> FROM `<project>.<dataset>.<table>`"
          + "<if(array)> CROSS JOIN UNNEST(<refCol>) AS <refCol><endif>";

  public List<String> getRefIds(Dataset dataset, String tableName, Column refColumn)
      throws InterruptedException {
    BigQueryProject bigQueryProject = BigQueryProject.from(dataset);

    ST sqlTemplate = new ST(getRefIdsTemplate);
    sqlTemplate.add("project", bigQueryProject.getProjectId());
    sqlTemplate.add("dataset", BigQueryPdao.prefixName(dataset.getName()));
    sqlTemplate.add("table", tableName);
    sqlTemplate.add("refCol", refColumn.getName());
    sqlTemplate.add("array", refColumn.isArrayOf());

    TableResult result = bigQueryProject.query(sqlTemplate.render());
    List<String> refIdArray = new ArrayList<>();
    for (FieldValueList row : result.iterateAll()) {
      if (!row.get(0).isNull()) {
        String refId = row.get(0).getStringValue();
        refIdArray.add(refId);
      }
    }

    return refIdArray;
  }

  private static final String insertIntoDatasetTableTemplate =
      "INSERT INTO `<project>.<dataset>.<targetTable>` (<transactIdColumn>, <columns; separator=\",\">)"
          + " SELECT @transactId,<columns; separator=\",\"> FROM `<project>.<dataset>.<stagingTable>`";

  public void insertIntoDatasetTable(
      Dataset dataset, DatasetTable targetTable, String stagingTableName, UUID transactId)
      throws InterruptedException {
    BigQueryProject bigQueryProject = BigQueryProject.from(dataset);

    ST sqlTemplate = new ST(insertIntoDatasetTableTemplate);
    sqlTemplate.add("project", bigQueryProject.getProjectId());
    sqlTemplate.add("dataset", BigQueryPdao.prefixName(dataset.getName()));
    sqlTemplate.add("targetTable", targetTable.getRawTableName());
    sqlTemplate.add("stagingTable", stagingTableName);
    sqlTemplate.add("transactIdColumn", PDAO_TRANSACTION_ID_COLUMN);
    sqlTemplate.add("columns", PDAO_ROW_ID_COLUMN);
    targetTable.getColumns().forEach(column -> sqlTemplate.add("columns", column.getName()));

    bigQueryProject.query(
        sqlTemplate.render(),
        Map.of(
            "transactId",
            QueryParameterValue.string(
                Optional.ofNullable(transactId).map(UUID::toString).orElse(null))));
  }

  private static final String insertIntoMetadataTableTemplate =
      "INSERT INTO `<project>.<dataset>.<metadataTable>` (<rowIdColumn>, <ingestByColumn>, "
          + "<ingestTimeColumn>, <loadTagColumn>) "
          + "SELECT <rowIdColumn>, '<ingestEmail>' AS <ingestByColumn>, "
          + "CURRENT_TIMESTAMP() AS <ingestTimeColumn>, '<loadTag>' AS <loadTagColumn> "
          + "FROM `<project>.<dataset>.<stagingTable>`";

  public void insertIntoMetadataTable(
      Dataset dataset,
      String metadataTableName,
      String stagingTableName,
      String email,
      String loadTag)
      throws InterruptedException {
    BigQueryProject bigQueryProject = BigQueryProject.from(dataset);

    ST sqlTemplate = new ST(insertIntoMetadataTableTemplate);
    sqlTemplate.add("project", bigQueryProject.getProjectId());
    sqlTemplate.add("dataset", BigQueryPdao.prefixName(dataset.getName()));
    sqlTemplate.add("metadataTable", metadataTableName);
    sqlTemplate.add("stagingTable", stagingTableName);
    sqlTemplate.add("rowIdColumn", PDAO_ROW_ID_COLUMN);
    sqlTemplate.add("ingestEmail", email);
    sqlTemplate.add("ingestByColumn", PDAO_INGESTED_BY_COLUMN);
    sqlTemplate.add("ingestTimeColumn", PDAO_INGEST_TIME_COLUMN);
    sqlTemplate.add("loadTag", loadTag);
    sqlTemplate.add("loadTagColumn", PDAO_LOAD_TAG_COLUMN);

    bigQueryProject.query(sqlTemplate.render());
  }

  // SOFT DELETES

  /**
   * Construct the sql for inserting existing records data into the soft delete dataset table.
   *
   * @param datasetLiveViewSql The sql to represent what records are active on the target table
   * @return The SQL for a soft deleting existing records for a table
   */
  private static String insertIntoSoftDeleteDatasetTable(String datasetLiveViewSql) {
    return "INSERT INTO `<project>.<dataset>.<softDeleteTable>` "
        + "(<rowIdColumn>,<loadTagColumn>,<flightIdColumn>,<transactIdColumn>,<deleteAtColumn>,"
        + "<deleteByColumn>) "
        + "SELECT"
        + "  T.<rowIdColumn>,"
        + "  @loadTag AS <loadTagColumn>,"
        + "  @flightId AS <flightIdColumn>,"
        + "  @transactId AS <transactIdColumn>,"
        + "  CURRENT_TIMESTAMP() AS <deleteAtColumn>,"
        + "  @deletedBy AS <deleteByColumn> "
        + "FROM ("
        + datasetLiveViewSql
        + ") AS T "
        + " LEFT JOIN `<project>.<dataset>.<stagingTable>` AS S "
        + "  ON <pkColumns:{c|T.<c.name> = S.<c.name>}; separator=\" AND \"> "
        + "WHERE <pkColumns:{c|S.<c.name> IS NOT NULL}; separator=\" AND \">";
  }

  public void insertIntoSoftDeleteDatasetTable(
      AuthenticatedUserRequest authedUser,
      Dataset dataset,
      DatasetTable targetTable,
      String stagingTableName,
      String loadTag,
      String flightId,
      UUID transactId)
      throws InterruptedException {
    BigQueryProject bigQueryProject = BigQueryProject.from(dataset);

    String datasetLiveViewSql =
        renderDatasetLiveViewSql(
            bigQueryProject.getProjectId(),
            BigQueryPdao.prefixName(dataset.getName()),
            targetTable,
            transactId,
            null);
    ST sqlTemplate = new ST(insertIntoSoftDeleteDatasetTable(datasetLiveViewSql));
    sqlTemplate.add("project", bigQueryProject.getProjectId());
    sqlTemplate.add("dataset", BigQueryPdao.prefixName(dataset.getName()));
    sqlTemplate.add("softDeleteTable", targetTable.getSoftDeleteTableName());
    sqlTemplate.add("targetTable", targetTable.getRawTableName());
    sqlTemplate.add("targetTableName", targetTable.getName());
    sqlTemplate.add("stagingTable", stagingTableName);
    sqlTemplate.add("rowIdColumn", PDAO_ROW_ID_COLUMN);
    sqlTemplate.add("deleteAtColumn", PDAO_DELETED_AT_COLUMN);
    sqlTemplate.add("deleteByColumn", PDAO_DELETED_BY_COLUMN);
    sqlTemplate.add("loadTagColumn", PDAO_LOAD_TAG_COLUMN);
    sqlTemplate.add("flightIdColumn", PDAO_FLIGHT_ID_COLUMN);
    sqlTemplate.add("transactIdColumn", PDAO_TRANSACTION_ID_COLUMN);
    sqlTemplate.add("pkColumns", targetTable.getPrimaryKey());

    bigQueryProject.query(
        sqlTemplate.render(),
        Map.of(
            "loadTag",
            QueryParameterValue.string(loadTag),
            "flightId",
            QueryParameterValue.string(flightId),
            "transactId",
            QueryParameterValue.string(
                Optional.ofNullable(transactId).map(UUID::toString).orElse(null)),
            // TODO: make the change for the standalone soft delete flight
            "deletedBy",
            QueryParameterValue.string(authedUser.getEmail())));
  }

  private static final String validateExtTableTemplate =
      "SELECT <rowId> FROM `<project>.<dataset>.<table>` LIMIT 1";

  public void createSoftDeleteExternalTable(
      Dataset dataset, String path, String tableName, String suffix) throws InterruptedException {

    BigQueryProject bigQueryProject = BigQueryProject.from(dataset);
    String extTableName = BigQueryPdao.externalTableName(tableName, suffix);
    TableId tableId = TableId.of(BigQueryPdao.prefixName(dataset.getName()), extTableName);
    Schema schema = Schema.of(Field.of(PDAO_ROW_ID_COLUMN, LegacySQLTypeName.STRING));
    ExternalTableDefinition tableDef =
        ExternalTableDefinition.of(path, schema, FormatOptions.csv());
    TableInfo tableInfo = TableInfo.of(tableId, tableDef);
    bigQueryProject.getBigQuery().create(tableInfo);

    // validate that the external table has data
    String sql =
        new ST(validateExtTableTemplate)
            .add("rowId", PDAO_ROW_ID_COLUMN)
            .add("project", bigQueryProject.getProjectId())
            .add("dataset", tableId.getDataset())
            .add("table", tableId.getTable())
            .render();
    TableResult result = bigQueryProject.query(sql);
    if (result.getTotalRows() == 0L) {
      // either the file at the path is empty or it doesn't exist. error out and let the cleanup
      // begin
      String msg =
          String.format(
              "No rows found at %s. Likely it is from a bad path or empty file(s).", path);
      throw new BadExternalFileException(msg);
    }
  }

  private static final String insertSoftDeleteTemplate =
      "INSERT INTO `<project>.<dataset>.<softDeleteTable>` "
          + "(<rowIdColumn>,"
          + "<flightIdColumn>,<transactIdColumn>,<deleteAtColumn>,"
          + "<deleteByColumn>) "
          + "SELECT DISTINCT E.<rowIdColumn>,"
          + "  @flightId AS <flightIdColumn>,"
          + "  @transactId AS <transactIdColumn>,"
          + "  CURRENT_TIMESTAMP() AS <deleteAtColumn>,"
          + "  @deletedBy AS <deleteByColumn> "
          + "FROM `<project>.<dataset>.<softDeleteExtTable>` E "
          + "LEFT JOIN `<project>.<dataset>.<softDeleteTable>` S USING (<rowIdColumn>) "
          + "WHERE S.<rowIdColumn> IS NULL";

  /**
   * Insert row ids into the corresponding soft delete table for each table provided.
   *
   * @param dataset repo dataset that we are deleting data from
   * @param tableNames list of table names that should have corresponding external tables with row
   *     ids to soft delete
   * @param suffix a bq-safe version of the flight id to prevent different flights from stepping on
   *     each other
   */
  public TableResult applySoftDeletes(
      Dataset dataset,
      List<String> tableNames,
      String suffix,
      String flightId,
      UUID transactionId,
      AuthenticatedUserRequest userRequest)
      throws InterruptedException {
    BigQueryProject bigQueryProject = BigQueryProject.from(dataset);

    // we want this soft delete operation to be one parent job with one child-job per query, we we
    // will combine
    // all of the inserts into one statement that we send to bigquery.
    // the soft delete tables have a random suffix on them, we need to fetch those from the db and
    // pass them in
    Map<String, String> softDeleteTableNameLookup =
        dataset.getTables().stream()
            .collect(Collectors.toMap(DatasetTable::getName, DatasetTable::getSoftDeleteTableName));

    List<String> sqlStatements =
        tableNames.stream()
            .map(
                tableName ->
                    new ST(insertSoftDeleteTemplate)
                        .add("project", bigQueryProject.getProjectId())
                        .add("dataset", BigQueryPdao.prefixName(dataset.getName()))
                        .add("softDeleteTable", softDeleteTableNameLookup.get(tableName))
                        .add("rowIdColumn", PDAO_ROW_ID_COLUMN)
                        .add("loadTagColumn", PDAO_LOAD_TAG_COLUMN)
                        .add("flightIdColumn", PDAO_FLIGHT_ID_COLUMN)
                        .add("transactIdColumn", PDAO_TRANSACTION_ID_COLUMN)
                        .add("deleteAtColumn", PDAO_DELETED_AT_COLUMN)
                        .add("deleteByColumn", PDAO_DELETED_BY_COLUMN)
                        .add(
                            "softDeleteExtTable", BigQueryPdao.externalTableName(tableName, suffix))
                        .render())
            .collect(Collectors.toList());

    return bigQueryProject.query(
        String.join(";", sqlStatements),
        Map.of(
            "flightId", QueryParameterValue.string(flightId),
            "transactId", QueryParameterValue.string(transactionId.toString()),
            "deletedBy", QueryParameterValue.string(userRequest.getEmail())));
  }

  /**
   * This join should pair up every rowId in the external table with a corresponding match in the
   * raw table. If there isn't a match in the raw table, then R.rowId will be null and we count that
   * as a mismatch.
   *
   * <p>Note that since this is joining against the raw table, not the the live view, an attempt to
   * soft delete a rowId that has already been soft deleted will not result in a mismatch.
   */
  private static final String validateSoftDeleteTemplate =
      "SELECT COUNT(E.<rowId>) FROM `<project>.<dataset>.<softDeleteExtTable>` E "
          + "LEFT JOIN `<project>.<dataset>.<rawTable>` R USING (<rowId>) "
          + "WHERE R.<rowId> IS NULL";

  /**
   * Goes through each of the provided tables and checks to see if the proposed row ids to soft
   * delete exist in the raw dataset table. This will error out on the first sign of mismatch.
   *
   * @param dataset dataset repo concept object
   * @param tables list of table specs from the DataDeletionRequest
   * @param suffix a string added onto the end of the external table to prevent collisions
   */
  public void validateDeleteRequest(
      Dataset dataset, List<DataDeletionTableModel> tables, String suffix)
      throws InterruptedException {

    BigQueryProject bigQueryProject = BigQueryProject.from(dataset);
    for (DataDeletionTableModel table : tables) {
      String tableName = table.getTableName();
      String rawTableName = dataset.getTableByName(tableName).get().getRawTableName();
      String sql =
          new ST(validateSoftDeleteTemplate)
              .add("rowId", PDAO_ROW_ID_COLUMN)
              .add("project", bigQueryProject.getProjectId())
              .add("dataset", BigQueryPdao.prefixName(dataset.getName()))
              .add("softDeleteExtTable", BigQueryPdao.externalTableName(tableName, suffix))
              .add("rawTable", rawTableName)
              .render();
      TableResult result = bigQueryProject.query(sql);
      long numMismatched = BigQueryPdao.getSingleLongValue(result);

      // shortcut out early, no use wasting more compute
      if (numMismatched > 0) {
        throw new MismatchedRowIdException(
            String.format("Could not match %s row ids for table %s", numMismatched, tableName));
      }
    }
  }

  // ACCESS

  public void grantReadAccessToDataset(Dataset dataset, Collection<String> policies)
      throws InterruptedException {
    BigQueryPdao.grantReadAccessWorker(
        BigQueryProject.from(dataset), BigQueryPdao.prefixName(dataset.getName()), policies);
  }

  // UTILITY METHODS

  public boolean datasetExists(Dataset dataset) throws InterruptedException {
    BigQueryProject bigQueryProject = BigQueryProject.from(dataset);
    String datasetName = BigQueryPdao.prefixName(dataset.getName());
    // bigQueryProject.datasetExists checks whether the BigQuery dataset by the provided name exists
    return bigQueryProject.datasetExists(datasetName);
  }

  public boolean tableExists(Dataset dataset, String tableName) throws InterruptedException {
    BigQueryProject bigQueryProject = BigQueryProject.from(dataset);
    String datasetName = BigQueryPdao.prefixName(dataset.getName());
    return bigQueryProject.tableExists(datasetName, tableName);
  }

  public boolean deleteDatasetTable(Dataset dataset, String tableName) throws InterruptedException {
    BigQueryProject bigQueryProject = BigQueryProject.from(dataset);
    return bigQueryProject.deleteTable(BigQueryPdao.prefixName(dataset.getName()), tableName);
  }

  public void undoDatasetTableCreate(Dataset dataset, DatasetTable table)
      throws InterruptedException {
    List<String> tableNames =
        List.of(
            table.getRawTableName(),
            table.getSoftDeleteTableName(),
            table.getRowMetadataTableName(),
            table.getName());
    for (String tableName : tableNames) {
      boolean success = deleteDatasetTable(dataset, tableName);
      if (!success) {
        logger.warn(
            "Could not delete table '{}' from dataset '{}' in BigQuery",
            tableName,
            dataset.getName());
      }
    }
  }

  @VisibleForTesting
  static String renderDatasetLiveViewSql(
      String bigQueryProject,
      String datasetName,
      DatasetTable table,
      UUID transactionId,
      Instant filterBefore) {
    ST datasetLiveViewSql = new ST(liveViewTemplate(transactionId, filterBefore));
    datasetLiveViewSql.add("project", bigQueryProject);
    datasetLiveViewSql.add("dataset", datasetName);
    datasetLiveViewSql.add("rawTable", table.getRawTableName());
    datasetLiveViewSql.add("sdTable", table.getSoftDeleteTableName());
    datasetLiveViewSql.add("transactionTable", PDAO_TRANSACTIONS_TABLE);
    datasetLiveViewSql.add("transactIdCol", PDAO_TRANSACTION_ID_COLUMN);
    datasetLiveViewSql.add("transactStatusCol", PDAO_TRANSACTION_STATUS_COLUMN);
    datasetLiveViewSql.add("partitionDateCol", PDAO_INGEST_DATE_COLUMN_ALIAS);
    datasetLiveViewSql.add("transactionTerminatedAtCol", PDAO_TRANSACTION_TERMINATED_AT_COLUMN);
    datasetLiveViewSql.add("transactStatusVal", TransactionModel.StatusEnum.ACTIVE);

    datasetLiveViewSql.add("columns", PDAO_ROW_ID_COLUMN);
    datasetLiveViewSql.add(
        "columns", table.getColumns().stream().map(Column::getName).collect(Collectors.toList()));
    datasetLiveViewSql.add(
        "partitionByDate",
        table.getBigQueryPartitionConfig() != null
            && table.getBigQueryPartitionConfig().getMode()
                == BigQueryPartitionConfigV1.Mode.INGEST_DATE);
    return datasetLiveViewSql.render();
  }

  // Note: this query includes a string substitution since it gets used in a view, which can not
  // support parameterized queries
  private static final String transactionQueryTemplate =
      "SELECT <transactIdCol> "
          + "FROM `<project>.<dataset>.<transactionTable>` "
          + "WHERE <transactStatusCol> = '<transactStatusVal>'";

  /**
   * Construct the live view sql.
   *
   * @param activeTransaction If not null, include values from this transaction. Note, if this has a
   *     non-null value then you MUST set the transactId query parameter. This means that this value
   *     can not be non-null when building a view
   * @param filterBefore If not null, only return values from transactions that were committed
   *     before this time. Note, if this has a non-null value then you MUST set the filterBefore
   *     query parameter. This means that this value can not be non-null when building a view
   * @return The live view SQL for a table
   */
  static String liveViewTemplate(UUID activeTransaction, Instant filterBefore) {
    String activeTransactionFilter = "";
    String committedInTimeFilter = "";
    if (activeTransaction != null) {
      activeTransactionFilter = "OR <transactIdCol> = @transactId";
    }
    if (filterBefore != null) {
      // This will cause ignoring transactions that were committed after the passed in cutoff time
      committedInTimeFilter = " OR <transactionTerminatedAtCol> > @transactionTerminatedAt";
    }
    return "SELECT <columns:{c|R.<c>}; separator=\",\">"
        + "<if(partitionByDate)>,<partitionDateCol><endif>"
        + " FROM (SELECT  <columns:{c|<c>}; separator=\",\">"
        + "<if(partitionByDate)>,_PARTITIONDATE AS <partitionDateCol><endif> "
        + "FROM `<project>.<dataset>.<rawTable>` "
        + "WHERE COALESCE(<transactIdCol>, '') NOT IN ("
        + (transactionQueryTemplate + committedInTimeFilter)
        + ") "
        + activeTransactionFilter
        + ") R "
        + "LEFT OUTER JOIN (SELECT * "
        + "FROM `<project>.<dataset>.<sdTable>` "
        + "WHERE COALESCE(<transactIdCol>, '') NOT IN ("
        + (transactionQueryTemplate + committedInTimeFilter)
        + ")"
        + activeTransactionFilter
        + ") S USING ("
        + PDAO_ROW_ID_COLUMN
        + ") "
        + "WHERE S."
        + PDAO_ROW_ID_COLUMN
        + " IS NULL";
  }

  // SCHEMA BUILDERS

  private Schema buildLoadDatasetSchema() {
    List<Field> fieldList = new ArrayList<>();
    fieldList.add(
        Field.newBuilder("load_tag", LegacySQLTypeName.STRING)
            .setMode(Field.Mode.REQUIRED)
            .build());
    fieldList.add(
        Field.newBuilder("load_time", LegacySQLTypeName.TIMESTAMP)
            .setMode(Field.Mode.REQUIRED)
            .build());
    fieldList.add(
        Field.newBuilder("source_name", LegacySQLTypeName.STRING)
            .setMode(Field.Mode.REQUIRED)
            .build());
    fieldList.add(
        Field.newBuilder("target_path", LegacySQLTypeName.STRING)
            .setMode(Field.Mode.REQUIRED)
            .build());
    fieldList.add(
        Field.newBuilder("state", LegacySQLTypeName.STRING).setMode(Field.Mode.REQUIRED).build());
    fieldList.add(
        Field.newBuilder("file_id", LegacySQLTypeName.STRING).setMode(Field.Mode.NULLABLE).build());
    fieldList.add(
        Field.newBuilder("checksum_crc32c", LegacySQLTypeName.STRING)
            .setMode(Field.Mode.NULLABLE)
            .build());
    fieldList.add(
        Field.newBuilder("checksum_md5", LegacySQLTypeName.STRING)
            .setMode(Field.Mode.NULLABLE)
            .build());
    fieldList.add(
        Field.newBuilder("error", LegacySQLTypeName.STRING).setMode(Field.Mode.NULLABLE).build());

    return Schema.of(fieldList);
  }

  private Schema buildSchema(
      DatasetTable table, boolean addRowIdColumn, boolean addTransactionIdColumn) {
    List<Field> fieldList = new ArrayList<>();
    List<String> primaryKeys =
        table.getPrimaryKey().stream().map(Column::getName).collect(Collectors.toList());

    if (addRowIdColumn) {
      fieldList.add(Field.of(PDAO_ROW_ID_COLUMN, LegacySQLTypeName.STRING));
    }

    if (addTransactionIdColumn) {
      fieldList.add(Field.of(PDAO_TRANSACTION_ID_COLUMN, LegacySQLTypeName.STRING));
    }

    for (Column column : table.getColumns()) {
      Field.Mode mode;
      if (primaryKeys.contains(column.getName()) || column.isRequired()) {
        mode = Field.Mode.REQUIRED;
      } else if (column.isArrayOf()) {
        mode = Field.Mode.REPEATED;
      } else {
        mode = Field.Mode.NULLABLE;
      }
      Field fieldSpec =
          Field.newBuilder(column.getName(), translateType(column.getType())).setMode(mode).build();

      fieldList.add(fieldSpec);
    }

    return Schema.of(fieldList);
  }

  private Schema buildRowMetadataSchema() {
    List<Field> fieldList =
        List.of(
            Field.newBuilder(PDAO_ROW_ID_COLUMN, LegacySQLTypeName.STRING)
                .setMode(Field.Mode.REQUIRED)
                .build(),
            Field.newBuilder(PDAO_INGESTED_BY_COLUMN, LegacySQLTypeName.STRING)
                .setMode(Field.Mode.REQUIRED)
                .build(),
            Field.newBuilder(PDAO_INGEST_TIME_COLUMN, LegacySQLTypeName.TIMESTAMP)
                .setMode(Field.Mode.REQUIRED)
                .build(),
            Field.newBuilder(PDAO_LOAD_TAG_COLUMN, LegacySQLTypeName.STRING)
                .setMode(Field.Mode.REQUIRED)
                .build());
    return Schema.of(fieldList);
  }

  private Schema buildSoftDeletesSchema() {
    return Schema.of(
        List.of(
            Field.of(PDAO_ROW_ID_COLUMN, LegacySQLTypeName.STRING),
            Field.of(PDAO_LOAD_TAG_COLUMN, LegacySQLTypeName.STRING),
            Field.of(PDAO_FLIGHT_ID_COLUMN, LegacySQLTypeName.STRING),
            Field.of(PDAO_TRANSACTION_ID_COLUMN, LegacySQLTypeName.STRING),
            Field.of(PDAO_DELETED_AT_COLUMN, LegacySQLTypeName.TIMESTAMP),
            Field.of(PDAO_DELETED_BY_COLUMN, LegacySQLTypeName.STRING)));
  }

  public static TableInfo buildLiveView(
      String bigQueryProject, String datasetName, DatasetTable table) {
    TableId liveViewId = TableId.of(datasetName, table.getName());
    return TableInfo.of(
        liveViewId,
        ViewDefinition.of(
            renderDatasetLiveViewSql(bigQueryProject, datasetName, table, null, null)));
  }

  private LegacySQLTypeName translateType(TableDataType datatype) {
    switch (datatype) {
      case BOOLEAN:
        return LegacySQLTypeName.BOOLEAN;
      case BYTES:
        return LegacySQLTypeName.BYTES;
      case DATE:
        return LegacySQLTypeName.DATE;
      case DATETIME:
        return LegacySQLTypeName.DATETIME;
      case DIRREF:
        return LegacySQLTypeName.STRING;
      case FILEREF:
        return LegacySQLTypeName.STRING;
      case FLOAT:
        return LegacySQLTypeName.FLOAT;
      case FLOAT64:
        return LegacySQLTypeName.FLOAT; // match the SQL type
      case INTEGER:
        return LegacySQLTypeName.INTEGER;
      case INT64:
        return LegacySQLTypeName.INTEGER; // match the SQL type
      case NUMERIC:
        return LegacySQLTypeName.NUMERIC;
        // case RECORD:    return LegacySQLTypeName.RECORD;
      case STRING:
        return LegacySQLTypeName.STRING;
      case TEXT:
        return LegacySQLTypeName.STRING; // match the Postgres type
      case TIME:
        return LegacySQLTypeName.TIME;
      case TIMESTAMP:
        return LegacySQLTypeName.TIMESTAMP;
      default:
        throw new IllegalArgumentException("Unknown datatype '" + datatype + "'");
    }
  }

  // MIGRATIONS
  public void migrateSchemaForTransactions(Dataset dataset) {
    String datasetName = BigQueryPdao.prefixName(dataset.getName());
    BigQueryProject bigQueryProject = BigQueryProject.from(dataset);
    BigQuery bigQuery = bigQueryProject.getBigQuery();
    String bqDatasetId = BigQueryPdao.prefixName(dataset.getName());
    logger.info("Migrating dataset {}", dataset.toLogString());
    for (DatasetTable datasetTable : dataset.getTables()) {
      try {
        logger.info("...Migrating dataset table {}", datasetTable.toLogString());
        logger.info("......Raw data table");
        updateSchema(
            bigQuery,
            bqDatasetId,
            datasetTable.getRawTableName(),
            buildSchema(datasetTable, true, true),
            false);
        logger.info("......Soft delete table");
        updateSchema(
            bigQuery,
            bqDatasetId,
            datasetTable.getSoftDeleteTableName(),
            buildSoftDeletesSchema(),
            false);
        logger.info("......Adding Transaction table");
        if (!bigQueryProject.tableExists(datasetName, PDAO_TRANSACTIONS_TABLE)) {
          bigQueryProject.createTable(
              datasetName,
              PDAO_TRANSACTIONS_TABLE,
              BigQueryTransactionPdao.buildTransactionsTableSchema());
        }
        logger.info("......Updating live view");
        bigQuery.update(buildLiveView(bigQueryProject.getProjectId(), datasetName, datasetTable));
      } catch (Exception e) {
        logger.warn(
            "Error migrating table {} in dataset {}",
            datasetTable.toLogString(),
            dataset.toLogString(),
            e);
      }
    }
  }

  private void updateSchema(
      BigQuery bigQuery,
      String bqDatasetId,
      String tableToUpdate,
      Schema newSchema,
      boolean strict) {
    com.google.cloud.bigquery.Table table = bigQuery.getTable(bqDatasetId, tableToUpdate);
    if (table == null) {
      return;
    }
    com.google.cloud.bigquery.Table updatedTable =
        table.toBuilder().setDefinition(StandardTableDefinition.of(newSchema)).build();
    try {
      updatedTable.update();
    } catch (BigQueryException e) {
      String message = String.format("Failure updating the table schema for %s", tableToUpdate);
      if (strict) {
        throw new PdaoException(message, e);
      } else {
        logger.warn(message, e);
      }
    }
  }

  private static String bqStringValue(FieldValueList fieldValue, String fieldName) {
    var value = fieldValue.get(fieldName);
    if (value != null) {
      var stringValue = value.getStringValue();
      if (!stringValue.isEmpty()) {
        return stringValue;
      }
    }
    return null;
  }
}
