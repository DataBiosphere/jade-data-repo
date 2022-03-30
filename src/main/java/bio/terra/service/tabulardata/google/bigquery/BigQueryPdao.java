package bio.terra.service.tabulardata.google.bigquery;

import static bio.terra.common.PdaoConstant.PDAO_EXTERNAL_TABLE_PREFIX;
import static bio.terra.common.PdaoConstant.PDAO_FIRESTORE_DUMP_FILE_ID_KEY;
import static bio.terra.common.PdaoConstant.PDAO_FIRESTORE_DUMP_GSPATH_KEY;
import static bio.terra.common.PdaoConstant.PDAO_PREFIX;
import static bio.terra.common.PdaoConstant.PDAO_ROW_ID_COLUMN;
import static bio.terra.common.PdaoConstant.PDAO_TRANSACTIONS_TABLE;
import static bio.terra.common.PdaoConstant.PDAO_TRANSACTION_CREATED_AT_COLUMN;
import static bio.terra.common.PdaoConstant.PDAO_TRANSACTION_CREATED_BY_COLUMN;
import static bio.terra.common.PdaoConstant.PDAO_TRANSACTION_DESCRIPTION_COLUMN;
import static bio.terra.common.PdaoConstant.PDAO_TRANSACTION_ID_COLUMN;
import static bio.terra.common.PdaoConstant.PDAO_TRANSACTION_LOCK_COLUMN;
import static bio.terra.common.PdaoConstant.PDAO_TRANSACTION_STATUS_COLUMN;
import static bio.terra.common.PdaoConstant.PDAO_TRANSACTION_TERMINATED_AT_COLUMN;
import static bio.terra.common.PdaoConstant.PDAO_TRANSACTION_TERMINATED_BY_COLUMN;

import bio.terra.common.CollectionType;
import bio.terra.common.Column;
import bio.terra.common.DateTimeUtils;
import bio.terra.common.exception.NotFoundException;
import bio.terra.common.exception.PdaoException;
import bio.terra.common.iam.AuthenticatedUserRequest;
import bio.terra.model.TransactionModel;
import bio.terra.service.common.gcs.BigQueryUtils;
import bio.terra.service.dataset.Dataset;
import bio.terra.service.dataset.DatasetTable;
import bio.terra.service.dataset.exception.TransactionLockException;
import bio.terra.service.filedata.FSContainerInterface;
import bio.terra.service.resourcemanagement.google.GoogleBucketResource;
import bio.terra.service.snapshot.Snapshot;
import bio.terra.service.snapshot.SnapshotTable;
import bio.terra.service.tabulardata.google.BigQueryProject;
import com.google.cloud.bigquery.Acl;
import com.google.cloud.bigquery.ExternalTableDefinition;
import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.Field.Mode;
import com.google.cloud.bigquery.FieldValueList;
import com.google.cloud.bigquery.FormatOptions;
import com.google.cloud.bigquery.LegacySQLTypeName;
import com.google.cloud.bigquery.QueryParameterValue;
import com.google.cloud.bigquery.Schema;
import com.google.cloud.bigquery.TableId;
import com.google.cloud.bigquery.TableInfo;
import com.google.cloud.bigquery.TableResult;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Component;
import org.stringtemplate.v4.ST;

@Component
@Profile("google")
public class BigQueryPdao {

  static void grantReadAccessWorker(
      BigQueryProject bigQueryProject, String name, Collection<String> policyGroupEmails)
      throws InterruptedException {
    List<Acl> policyGroupAcls =
        policyGroupEmails.stream()
            .map(email -> Acl.of(new Acl.Group(email), Acl.Role.READER))
            .collect(Collectors.toList());
    bigQueryProject.addDatasetAcls(name, policyGroupAcls);
  }

  private static final String insertIntoTransactionTableTemplate =
      "INSERT INTO `<project>.<dataset>.<transactionTable>` "
          + "(<transactIdCol>,<transactStatusCol>,<transactLockCol>,<transactDescriptionCol>,"
          + "<transactCreatedAtCol>,<transactCreatedByCol>)"
          + " VALUES "
          + "(@transactId,@transactStatus,@transactLock,@transactDescription,@transactCreatedAt,"
          + "@transactCreatedBy)";

  public TransactionModel insertIntoTransactionTable(
      AuthenticatedUserRequest authedUser,
      Dataset dataset,
      String flightId,
      String transactionDescription)
      throws InterruptedException {
    BigQueryProject bigQueryProject = BigQueryProject.from(dataset);

    ST sqlTemplate = new ST(insertIntoTransactionTableTemplate);
    sqlTemplate.add("project", bigQueryProject.getProjectId());
    sqlTemplate.add("dataset", prefixName(dataset.getName()));
    sqlTemplate.add("transactionTable", PDAO_TRANSACTIONS_TABLE);

    sqlTemplate.add("transactIdCol", PDAO_TRANSACTION_ID_COLUMN);
    sqlTemplate.add("transactStatusCol", PDAO_TRANSACTION_STATUS_COLUMN);
    sqlTemplate.add("transactLockCol", PDAO_TRANSACTION_LOCK_COLUMN);
    sqlTemplate.add("transactDescriptionCol", PDAO_TRANSACTION_DESCRIPTION_COLUMN);
    sqlTemplate.add("transactCreatedAtCol", PDAO_TRANSACTION_CREATED_AT_COLUMN);
    sqlTemplate.add("transactCreatedByCol", PDAO_TRANSACTION_CREATED_BY_COLUMN);

    Instant filterBefore = Instant.now();
    TransactionModel transaction =
        new TransactionModel()
            .id(UUID.randomUUID())
            .lock(flightId)
            .description(transactionDescription)
            .status(TransactionModel.StatusEnum.ACTIVE)
            .createdAt(filterBefore.toString())
            .createdBy(authedUser.getEmail());

    bigQueryProject.query(
        sqlTemplate.render(),
        Map.of(
            "transactId", QueryParameterValue.string(transaction.getId().toString()),
            "transactLock", QueryParameterValue.string(transaction.getLock()),
            "transactDescription", QueryParameterValue.string(transaction.getDescription()),
            "transactStatus", QueryParameterValue.string(transaction.getStatus().toString()),
            "transactCreatedAt",
                QueryParameterValue.timestamp(DateTimeUtils.toEpochMicros(filterBefore)),
            "transactCreatedBy", QueryParameterValue.string(transaction.getCreatedBy())));

    return transaction;
  }

  private static final String deleteFromTransactionTableTemplate =
      "DELETE FROM `<project>.<dataset>.<transactionTable>` WHERE <transactIdCol>=@transactId";

  public void deleteFromTransactionTable(Dataset dataset, UUID transactionId)
      throws InterruptedException {
    BigQueryProject bigQueryProject = BigQueryProject.from(dataset);

    ST sqlTemplate = new ST(deleteFromTransactionTableTemplate);
    sqlTemplate.add("project", bigQueryProject.getProjectId());
    sqlTemplate.add("dataset", prefixName(dataset.getName()));
    sqlTemplate.add("transactionTable", PDAO_TRANSACTIONS_TABLE);

    sqlTemplate.add("transactIdCol", PDAO_TRANSACTION_ID_COLUMN);

    bigQueryProject.query(
        sqlTemplate.render(),
        Map.of("transactId", QueryParameterValue.string(transactionId.toString())));
  }

  // TODO: once GA, add a transaction block around this. Putting into the same job at least reduces
  // the risk of conflict
  private static final String updateTransactionTableLockTemplate =
      "SELECT IF((SELECT COUNT(*)"
          + " FROM `<project>.<dataset>.<transactionTable>`"
          + " WHERE <transactIdCol>=@transactId"
          + " AND (<createdByCol>!=@createdBy"
          + " OR (<transactLockCol> IS NOT NULL AND <transactLockCol>!=@transactLock))) > 0,"
          + " ERROR('<lockErrorMessage>'), '');"
          + "UPDATE `<project>.<dataset>.<transactionTable>` SET "
          + "<transactLockCol>=@transactLock "
          + "WHERE <transactIdCol>=@transactId;";

  public TransactionModel updateTransactionTableLock(
      Dataset dataset, UUID transactId, String flightId, AuthenticatedUserRequest userRequest)
      throws InterruptedException {
    BigQueryProject bigQueryProject = BigQueryProject.from(dataset);

    ST sqlTemplate = new ST(updateTransactionTableLockTemplate);
    sqlTemplate.add("project", bigQueryProject.getProjectId());
    sqlTemplate.add("dataset", prefixName(dataset.getName()));
    sqlTemplate.add("transactionTable", PDAO_TRANSACTIONS_TABLE);

    sqlTemplate.add("transactIdCol", PDAO_TRANSACTION_ID_COLUMN);
    sqlTemplate.add("transactLockCol", PDAO_TRANSACTION_LOCK_COLUMN);
    sqlTemplate.add("transactTerminatedAtCol", PDAO_TRANSACTION_TERMINATED_AT_COLUMN);
    sqlTemplate.add("createdByCol", PDAO_TRANSACTION_CREATED_BY_COLUMN);

    String lockErrorMessage = "Transaction already locked or was created by another user";
    sqlTemplate.add("lockErrorMessage", lockErrorMessage);
    try {
      bigQueryProject.query(
          sqlTemplate.render(),
          Map.of(
              "transactId", QueryParameterValue.string(transactId.toString()),
              "transactLock", QueryParameterValue.string(flightId),
              "createdBy", QueryParameterValue.string(userRequest.getEmail())));
    } catch (PdaoException e) {
      if (e.getCause() != null && e.getCause().getMessage().contains(lockErrorMessage)) {
        throw new TransactionLockException(
            String.format("Error locking transaction for dataset %s", dataset.toLogString()),
            List.of(lockErrorMessage));
      }
      throw new TransactionLockException(
          String.format("Error locking transaction for dataset %s", dataset.toLogString()), e);
    }
    return retrieveTransaction(dataset, transactId);
  }

  private static final String updateTransactionTableStatusTemplate =
      "UPDATE `<project>.<dataset>.<transactionTable>` SET "
          + "<transactStatusCol>=@transactStatus,"
          + "<transactTerminatedAtCol>=@transactTerminatedAt,"
          + "<transactTerminatedByCol>=@transactTerminatedBy "
          + "WHERE <transactIdCol>=@transactId";

  public TransactionModel updateTransactionTableStatus(
      AuthenticatedUserRequest authedUser,
      Dataset dataset,
      UUID transactId,
      TransactionModel.StatusEnum status)
      throws InterruptedException {
    if (status == TransactionModel.StatusEnum.ACTIVE) {
      throw new IllegalArgumentException("Transactions cannot be re-openned");
    }
    BigQueryProject bigQueryProject = BigQueryProject.from(dataset);

    ST sqlTemplate = new ST(updateTransactionTableStatusTemplate);
    sqlTemplate.add("project", bigQueryProject.getProjectId());
    sqlTemplate.add("dataset", prefixName(dataset.getName()));
    sqlTemplate.add("transactionTable", PDAO_TRANSACTIONS_TABLE);

    sqlTemplate.add("transactIdCol", PDAO_TRANSACTION_ID_COLUMN);
    sqlTemplate.add("transactStatusCol", PDAO_TRANSACTION_STATUS_COLUMN);
    sqlTemplate.add("transactTerminatedAtCol", PDAO_TRANSACTION_TERMINATED_AT_COLUMN);
    sqlTemplate.add("transactTerminatedByCol", PDAO_TRANSACTION_TERMINATED_BY_COLUMN);

    Instant terminatedAt = Instant.now();

    bigQueryProject.query(
        sqlTemplate.render(),
        Map.of(
            "transactId", QueryParameterValue.string(transactId.toString()),
            "transactStatus", QueryParameterValue.string(status.toString()),
            "transactTerminatedAt",
                QueryParameterValue.timestamp(DateTimeUtils.toEpochMicros(terminatedAt)),
            "transactTerminatedBy", QueryParameterValue.string(authedUser.getEmail())));

    return retrieveTransaction(dataset, transactId);
  }

  private static final String baseTransactionTableTemplate =
      "SELECT * FROM `<project>.<dataset>.<transactionTable>`";

  private static final String enumerateTransactionsTemplate =
      baseTransactionTableTemplate
          + " ORDER BY <transactCreatedAtCol> DESC LIMIT <limit> OFFSET <offset>";

  public List<TransactionModel> enumerateTransactions(Dataset dataset, long offset, long limit)
      throws InterruptedException {
    BigQueryProject bigQueryProject = BigQueryProject.from(dataset);

    ST sqlTemplate = new ST(enumerateTransactionsTemplate);
    sqlTemplate.add("project", bigQueryProject.getProjectId());
    sqlTemplate.add("dataset", prefixName(dataset.getName()));
    sqlTemplate.add("transactionTable", PDAO_TRANSACTIONS_TABLE);

    sqlTemplate.add("limit", limit);
    sqlTemplate.add("offset", offset);
    sqlTemplate.add("transactCreatedAtCol", PDAO_TRANSACTION_CREATED_AT_COLUMN);

    return StreamSupport.stream(
            bigQueryProject.query(sqlTemplate.render()).getValues().spliterator(), false)
        .map(this::mapTransactionModel)
        .collect(Collectors.toList());
  }

  private static final String retrieveTransactionTemplate =
      baseTransactionTableTemplate + " WHERE <transactIdCol> = @transactionId ";

  public TransactionModel retrieveTransaction(Dataset dataset, UUID transactionId)
      throws InterruptedException {
    BigQueryProject bigQueryProject = BigQueryProject.from(dataset);

    ST sqlTemplate = new ST(retrieveTransactionTemplate);
    sqlTemplate.add("project", bigQueryProject.getProjectId());
    sqlTemplate.add("dataset", prefixName(dataset.getName()));
    sqlTemplate.add("transactionTable", PDAO_TRANSACTIONS_TABLE);

    sqlTemplate.add("transactIdCol", PDAO_TRANSACTION_ID_COLUMN);

    TableResult result =
        bigQueryProject.query(
            sqlTemplate.render(),
            Map.of("transactionId", QueryParameterValue.string(transactionId.toString())));
    if (result.getTotalRows() == 0) {
      throw new NotFoundException(
          String.format(
              "Transaction %s not found in dataset %s", transactionId, dataset.toLogString()));
    } else if (result.getTotalRows() == 1) {
      return mapTransactionModel(result.getValues().iterator().next());
    } else {
      throw new PdaoException(
          String.format("Found duplicate transactions in dataset %s", dataset.toLogString()));
    }
  }

  private static final String verifyTransactionTemplate =
      "SELECT COUNT(*) cnt "
          + "FROM `<project>.<dataset>.<rawDataTable>` AS RT"
          + "  INNER JOIN `<project>.<dataset>.<targetTable>` AS T "
          + "    ON <pkColumns:{c|RT.<c.name> = T.<c.name>}; separator=\" AND \"> "
          + "        AND RT.<transactIdCol>=@transactId"
          + "  INNER JOIN `<project>.<dataset>.<rawDataTable>` R"
          + "    ON T.<rowIdColumn>=R.<rowIdColumn> "
          + "  INNER JOIN `<project>.<dataset>.<transactionTable>` X"
          + "    ON R.<transactIdCol>=X.<transactIdCol> "
          + "WHERE x.<transactStatusCol>=@transactStatus"
          + "  AND x.<transactTerminatedAtCol> >"
          + "  (SELECT <transactCreatedAtCol>"
          + "   FROM `<project>.<dataset>.<transactionTable>`"
          + "   WHERE <transactIdCol>=@transactId)";

  public long verifyTransaction(Dataset dataset, DatasetTable datasetTable, UUID transactId)
      throws InterruptedException {
    BigQueryProject bigQueryProject = BigQueryProject.from(dataset);

    ST sqlTemplate = new ST(verifyTransactionTemplate);
    sqlTemplate.add("project", bigQueryProject.getProjectId());
    sqlTemplate.add("dataset", prefixName(dataset.getName()));
    sqlTemplate.add("targetTable", datasetTable.getName());
    sqlTemplate.add("metadataTable", datasetTable.getRowMetadataTableName());
    sqlTemplate.add("rawDataTable", datasetTable.getRawTableName());
    sqlTemplate.add("transactionTable", PDAO_TRANSACTIONS_TABLE);

    sqlTemplate.add("rowIdColumn", PDAO_ROW_ID_COLUMN);
    sqlTemplate.add("transactIdCol", PDAO_TRANSACTION_ID_COLUMN);
    sqlTemplate.add("transactStatusCol", PDAO_TRANSACTION_STATUS_COLUMN);
    sqlTemplate.add("transactTerminatedAtCol", PDAO_TRANSACTION_TERMINATED_AT_COLUMN);
    sqlTemplate.add("transactCreatedAtCol", PDAO_TRANSACTION_CREATED_AT_COLUMN);
    sqlTemplate.add("pkColumns", datasetTable.getPrimaryKey());

    TableResult result =
        bigQueryProject.query(
            sqlTemplate.render(),
            Map.of(
                "transactId", QueryParameterValue.string(transactId.toString()),
                "transactStatus",
                    QueryParameterValue.string(TransactionModel.StatusEnum.COMMITTED.toString())));

    try {
      return result.getValues().iterator().next().get("cnt").getLongValue();
    } catch (NoSuchElementException e) {
      // Note: there will always be a row here unless something goes wrong with BigQuery return.
      // It's highly unlikely we would ever reach this point
      throw new PdaoException("Error verifying transaction lock status", e);
    }
  }

  private TransactionModel mapTransactionModel(FieldValueList values) {
    return new TransactionModel()
        .id(UUID.fromString(values.get(PDAO_TRANSACTION_ID_COLUMN).getStringValue()))
        .status(
            TransactionModel.StatusEnum.fromValue(
                values.get(PDAO_TRANSACTION_STATUS_COLUMN).getStringValue()))
        .lock(
            values.get(PDAO_TRANSACTION_LOCK_COLUMN).isNull()
                ? null
                : values.get(PDAO_TRANSACTION_LOCK_COLUMN).getStringValue())
        .description(
            Optional.ofNullable(values.get(PDAO_TRANSACTION_DESCRIPTION_COLUMN).getValue())
                .map(Object::toString)
                .orElse(null))
        .createdAt(
            DateTimeUtils.ofEpicMicros(
                    values.get(PDAO_TRANSACTION_CREATED_AT_COLUMN).getTimestampValue())
                .toString())
        .createdBy(values.get(PDAO_TRANSACTION_CREATED_BY_COLUMN).getStringValue())
        .terminatedAt(
            values.get(PDAO_TRANSACTION_TERMINATED_AT_COLUMN).isNull()
                ? null
                : DateTimeUtils.ofEpicMicros(
                        values.get(PDAO_TRANSACTION_TERMINATED_AT_COLUMN).getTimestampValue())
                    .toString())
        .terminatedBy(
            Optional.ofNullable(values.get(PDAO_TRANSACTION_TERMINATED_BY_COLUMN).getValue())
                .map(Object::toString)
                .orElse(null));
  }

  private static final String rollbackDatasetTableTemplate =
      "DELETE FROM `<project>.<dataset>.<targetTable>` WHERE <transactIdColumn>=@transactId";

  public void rollbackDatasetTable(Dataset dataset, String targetTableName, UUID transactId)
      throws InterruptedException {
    if (transactId == null) {
      throw new PdaoException("Can not roll back a null transaction");
    }
    BigQueryProject bigQueryProject = BigQueryProject.from(dataset);

    ST sqlTemplate = new ST(rollbackDatasetTableTemplate);
    sqlTemplate.add("project", bigQueryProject.getProjectId());
    sqlTemplate.add("dataset", prefixName(dataset.getName()));
    sqlTemplate.add("targetTable", targetTableName);
    sqlTemplate.add("transactIdColumn", PDAO_TRANSACTION_ID_COLUMN);

    bigQueryProject.query(
        sqlTemplate.render(),
        Map.of("transactId", QueryParameterValue.string(transactId.toString())));
  }

  private static final String rollbackDatasetMetadataTableTemplate =
      "DELETE FROM `<project>.<dataset>.<metadataTable>` "
          + "WHERE <datarepoIdCol> IN"
          + "(SELECT <datarepoIdCol>"
          + " FROM `<project>.<dataset>.<targetTable>`"
          + " WHERE <transactIdColumn>=@transactId)";

  public void rollbackDatasetMetadataTable(
      Dataset dataset, DatasetTable targetTable, UUID transactId) throws InterruptedException {
    if (transactId == null) {
      throw new PdaoException("Can not roll back a null transaction");
    }
    BigQueryProject bigQueryProject = BigQueryProject.from(dataset);

    ST sqlTemplate = new ST(rollbackDatasetMetadataTableTemplate);
    sqlTemplate.add("project", bigQueryProject.getProjectId());
    sqlTemplate.add("dataset", prefixName(dataset.getName()));
    sqlTemplate.add("metadataTable", targetTable.getRowMetadataTableName());
    sqlTemplate.add("targetTable", targetTable.getRawTableName());
    sqlTemplate.add("transactIdColumn", PDAO_TRANSACTION_ID_COLUMN);
    sqlTemplate.add("datarepoIdCol", PDAO_ROW_ID_COLUMN);

    bigQueryProject.query(
        sqlTemplate.render(),
        Map.of("transactId", QueryParameterValue.string(transactId.toString())));
  }

  private static final String selectHasDuplicateStagingIdsTemplate =
      "SELECT <pkColumns:{c|<c.name>}; separator=\",\">,COUNT(*) "
          + "FROM `<project>.<dataset>.<tableName>` "
          + "GROUP BY <pkColumns:{c|<c.name>}; separator=\",\"> "
          + "HAVING COUNT(*) > 1";

  /**
   * Returns true is any duplicate IDs are present in a BigQuery table TODO: add support for
   * returning top few instances
   */
  public boolean hasDuplicatePrimaryKeys(
      FSContainerInterface container, List<Column> pkColumns, String tableName)
      throws InterruptedException {
    BigQueryProject bigQueryProject = BigQueryProject.from(container);

    String bqDatasetName = prefixContainerName(container);

    ST sqlTemplate = new ST(selectHasDuplicateStagingIdsTemplate);
    sqlTemplate.add("project", bigQueryProject.getProjectId());
    sqlTemplate.add("dataset", bqDatasetName);
    sqlTemplate.add("tableName", tableName);
    sqlTemplate.add("pkColumns", pkColumns);

    TableResult result = bigQueryProject.query(sqlTemplate.render());
    return result.getTotalRows() > 0;
  }

  public static String prefixName(String name) {
    return PDAO_PREFIX + name;
  }

  static Schema buildTransactionsTableSchema() {
    return Schema.of(
        Field.newBuilder(PDAO_TRANSACTION_ID_COLUMN, LegacySQLTypeName.STRING)
            .setMode(Field.Mode.REQUIRED)
            .build(),
        Field.newBuilder(PDAO_TRANSACTION_STATUS_COLUMN, LegacySQLTypeName.STRING)
            .setMode(Mode.REQUIRED)
            .build(),
        Field.newBuilder(PDAO_TRANSACTION_LOCK_COLUMN, LegacySQLTypeName.STRING)
            .setMode(Mode.NULLABLE)
            .build(),
        Field.newBuilder(PDAO_TRANSACTION_DESCRIPTION_COLUMN, LegacySQLTypeName.STRING)
            .setMode(Mode.NULLABLE)
            .build(),
        Field.newBuilder(PDAO_TRANSACTION_CREATED_AT_COLUMN, LegacySQLTypeName.TIMESTAMP)
            .setMode(Mode.REQUIRED)
            .build(),
        Field.newBuilder(PDAO_TRANSACTION_CREATED_BY_COLUMN, LegacySQLTypeName.STRING)
            .setMode(Mode.REQUIRED)
            .build(),
        Field.newBuilder(PDAO_TRANSACTION_TERMINATED_AT_COLUMN, LegacySQLTypeName.TIMESTAMP)
            .setMode(Mode.NULLABLE)
            .build(),
        Field.newBuilder(PDAO_TRANSACTION_TERMINATED_BY_COLUMN, LegacySQLTypeName.STRING)
            .setMode(Mode.NULLABLE)
            .build());
  }

  static String externalTableName(String tableName, String suffix) {
    return String.format("%s%s_%s", PDAO_EXTERNAL_TABLE_PREFIX, tableName, suffix);
  }

  public void createFirestoreGsPathExternalTable(Snapshot snapshot, String path, String flightId) {
    String gsPathMappingTableName = BigQueryUtils.gsPathMappingTableName(snapshot);
    String extTableName = externalTableName(gsPathMappingTableName, flightId);

    BigQueryProject bigQueryProject = BigQueryProject.from(snapshot);
    TableId tableId = TableId.of(snapshot.getName(), extTableName);
    Schema schema =
        Schema.of(
            Field.of(PDAO_FIRESTORE_DUMP_FILE_ID_KEY, LegacySQLTypeName.STRING),
            Field.of(PDAO_FIRESTORE_DUMP_GSPATH_KEY, LegacySQLTypeName.STRING));
    ExternalTableDefinition tableDef =
        ExternalTableDefinition.of(path, schema, FormatOptions.json());
    TableInfo tableInfo = TableInfo.of(tableId, tableDef);
    bigQueryProject.getBigQuery().create(tableInfo);
  }

  public boolean deleteFirestoreGsPathExternalTable(Snapshot snapshot, String flightId) {
    String gsPathMappingTableName = BigQueryUtils.gsPathMappingTableName(snapshot);
    return deleteExternalTable(snapshot, gsPathMappingTableName, flightId);
  }

  public boolean deleteExternalTable(
      FSContainerInterface container, String tableName, String suffix) {

    BigQueryProject bigQueryProject = BigQueryProject.from(container);
    String bqDatasetName = prefixContainerName(container);
    String extTableName = externalTableName(tableName, suffix);
    return bigQueryProject.deleteTable(bqDatasetName, extTableName);
  }

  private String prefixContainerName(FSContainerInterface container) {
    if (container.getCollectionType() == CollectionType.DATASET) {
      return prefixName(container.getName());
    } else {
      return container.getName();
    }
  }

  private static final String exportToParquetTemplate =
      "export data OPTIONS( "
          + "uri='<exportPath>/<table>-*.parquet', "
          + "format='PARQUET') "
          + "AS (<exportToParquetQuery>)";

  private static final String exportPathTemplate = "gs://<bucket>/<flightId>/<table>";
  private static final String exportToParquetQueryTemplate =
      "select * from `<project>.<snapshot>.<table>`";

  public List<String> exportTableToParquet(
      Snapshot snapshot,
      GoogleBucketResource bucketResource,
      String flightId,
      boolean exportGsPaths)
      throws InterruptedException {
    List<String> paths = new ArrayList<>();
    BigQueryProject bigQueryProject = BigQueryProject.from(snapshot);

    for (var table : snapshot.getTables()) {
      String tableName = table.getName();
      String exportPath =
          new ST(exportPathTemplate)
              .add("bucket", bucketResource.getName())
              .add("flightId", flightId)
              .add("table", tableName)
              .render();

      final String exportToParquetQuery;
      if (exportGsPaths) {
        exportToParquetQuery = createExportToParquetWithGsPathQuery(snapshot, table, flightId);
      } else {
        exportToParquetQuery = creteExportToParquetQuery(snapshot, table, flightId);
      }

      String exportStatement =
          new ST(exportToParquetTemplate)
              .add("exportPath", exportPath)
              .add("exportToParquetQuery", exportToParquetQuery)
              .add("table", tableName)
              .render();

      bigQueryProject.query(exportStatement);
      paths.add(exportPath);
    }

    return paths;
  }

  private String creteExportToParquetQuery(
      Snapshot snapshot, SnapshotTable table, String flightId) {
    String snapshotProject = snapshot.getProjectResource().getGoogleProjectId();
    String snapshotName = snapshot.getName();
    return new ST(exportToParquetQueryTemplate)
        .add("table", table.getName())
        .add("project", snapshotProject)
        .add("snapshot", snapshotName)
        .render();
  }

  private static final String exportToMappingTableTemplate =
      "WITH datarepo_gs_path_mapping AS "
          + "(SELECT <fileIdKey>, <gsPathKey> "
          + "  FROM `<project>.<snapshotDatasetName>.<gsPathMappingTable>`) "
          + "SELECT "
          + "  S.<pdaoRowIdColumn>,"
          + "  <mappedColumns; separator=\",\"> "
          + "FROM "
          + "`<project>.<snapshotDatasetName>.<table>` S";

  private String createExportToParquetWithGsPathQuery(
      Snapshot snapshot, SnapshotTable table, String flightId) {
    String gsPathMappingTableName = BigQueryUtils.gsPathMappingTableName(snapshot);
    String extTableName = externalTableName(gsPathMappingTableName, flightId);
    List<String> mappedColumns =
        table.getColumns().stream().map(this::gsPathMappingSelectSql).collect(Collectors.toList());

    return new ST(exportToMappingTableTemplate)
        .add("project", snapshot.getProjectResource().getGoogleProjectId())
        .add("snapshotDatasetName", snapshot.getName())
        .add("gsPathMappingTable", extTableName)
        .add("pdaoRowIdColumn", PDAO_ROW_ID_COLUMN)
        .add("mappedColumns", mappedColumns)
        .add("table", table.getName())
        .add("gsPathKey", PDAO_FIRESTORE_DUMP_GSPATH_KEY)
        .add("fileIdKey", PDAO_FIRESTORE_DUMP_FILE_ID_KEY)
        .render();
  }

  private static final String fileRefMappingColumnTemplate =
      "(SELECT <gsPathKey> "
          + "FROM datarepo_gs_path_mapping "
          + "WHERE RIGHT(S.<columnName>, 36) = <fileIdKey>) AS <columnName>";

  private static final String fileRefArrayOfMappingColumnTemplate =
      "  ARRAY(SELECT <gsPathKey> "
          + "    FROM UNNEST(<columnName>) AS unnested_drs_path, "
          + "    datarepo_gs_path_mapping "
          + "    WHERE RIGHT(unnested_drs_path, 36) = <fileIdKey>) AS <columnName>";

  private String gsPathMappingSelectSql(Column snapshotColumn) {
    String columnName = snapshotColumn.getName();
    if (snapshotColumn.isFileOrDirRef()) {
      final String columnTemplate;
      if (snapshotColumn.isArrayOf()) {
        columnTemplate = fileRefArrayOfMappingColumnTemplate;
      } else {
        columnTemplate = fileRefMappingColumnTemplate;
      }
      return new ST(columnTemplate)
          .add("columnName", columnName)
          .add("gsPathKey", PDAO_FIRESTORE_DUMP_GSPATH_KEY)
          .add("fileIdKey", PDAO_FIRESTORE_DUMP_FILE_ID_KEY)
          .render();
    } else {
      return "S." + snapshotColumn.getName();
    }
  }

  static long getSingleLongValue(TableResult result) {
    FieldValueList fieldValues = result.getValues().iterator().next();
    return fieldValues.get(0).getLongValue();
  }
}
