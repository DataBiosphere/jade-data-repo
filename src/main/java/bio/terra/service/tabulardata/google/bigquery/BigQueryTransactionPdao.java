package bio.terra.service.tabulardata.google.bigquery;

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

import bio.terra.common.DateTimeUtils;
import bio.terra.common.exception.NotFoundException;
import bio.terra.common.exception.PdaoException;
import bio.terra.common.iam.AuthenticatedUserRequest;
import bio.terra.model.TransactionModel;
import bio.terra.service.dataset.Dataset;
import bio.terra.service.dataset.DatasetTable;
import bio.terra.service.dataset.exception.TransactionLockException;
import bio.terra.service.filedata.google.gcs.GcsConfiguration;
import bio.terra.service.tabulardata.google.BigQueryProject;
import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.FieldValueList;
import com.google.cloud.bigquery.LegacySQLTypeName;
import com.google.cloud.bigquery.QueryParameterValue;
import com.google.cloud.bigquery.Schema;
import com.google.cloud.bigquery.TableResult;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Component;
import org.stringtemplate.v4.ST;

@Component
@Profile("google")
public class BigQueryTransactionPdao {
  private static final Logger logger = LoggerFactory.getLogger(BigQueryTransactionPdao.class);

  private static final String insertIntoTransactionTableTemplate =
      "INSERT INTO `<project>.<dataset>.<transactionTable>` "
          + "(<transactIdCol>,<transactStatusCol>,<transactLockCol>,<transactDescriptionCol>,"
          + "<transactCreatedAtCol>,<transactCreatedByCol>)"
          + " VALUES "
          + "(@transactId,@transactStatus,@transactLock,@transactDescription,@transactCreatedAt,"
          + "@transactCreatedBy)";

  private GcsConfiguration gcsConfiguration;

  @Autowired
  public BigQueryTransactionPdao(GcsConfiguration gcsConfiguration) {
    this.gcsConfiguration = gcsConfiguration;
  }

  public TransactionModel insertIntoTransactionTable(
      AuthenticatedUserRequest authedUser,
      Dataset dataset,
      String flightId,
      String transactionDescription)
      throws InterruptedException {
    BigQueryProject bigQueryProject =
        BigQueryProject.from(
            dataset,
            gcsConfiguration.getConnectTimeoutSeconds(),
            gcsConfiguration.getReadTimeoutSeconds());

    ST sqlTemplate = new ST(insertIntoTransactionTableTemplate);
    sqlTemplate.add("project", bigQueryProject.getProjectId());
    sqlTemplate.add("dataset", BigQueryPdao.prefixName(dataset.getName()));
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
            "transactId",
            QueryParameterValue.string(transaction.getId().toString()),
            "transactLock",
            QueryParameterValue.string(transaction.getLock()),
            "transactDescription",
            QueryParameterValue.string(transaction.getDescription()),
            "transactStatus",
            QueryParameterValue.string(transaction.getStatus().toString()),
            "transactCreatedAt",
            QueryParameterValue.timestamp(DateTimeUtils.toEpochMicros(filterBefore)),
            "transactCreatedBy",
            QueryParameterValue.string(transaction.getCreatedBy())));

    return transaction;
  }

  private static final String deleteFromTransactionTableTemplate =
      "DELETE FROM `<project>.<dataset>.<transactionTable>` WHERE <transactIdCol>=@transactId";

  public void deleteFromTransactionTable(Dataset dataset, UUID transactionId)
      throws InterruptedException {
    BigQueryProject bigQueryProject =
        BigQueryProject.from(
            dataset,
            gcsConfiguration.getConnectTimeoutSeconds(),
            gcsConfiguration.getReadTimeoutSeconds());

    ST sqlTemplate = new ST(deleteFromTransactionTableTemplate);
    sqlTemplate.add("project", bigQueryProject.getProjectId());
    sqlTemplate.add("dataset", BigQueryPdao.prefixName(dataset.getName()));
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
    BigQueryProject bigQueryProject =
        BigQueryProject.from(
            dataset,
            gcsConfiguration.getConnectTimeoutSeconds(),
            gcsConfiguration.getReadTimeoutSeconds());

    ST sqlTemplate = new ST(updateTransactionTableLockTemplate);
    sqlTemplate.add("project", bigQueryProject.getProjectId());
    sqlTemplate.add("dataset", BigQueryPdao.prefixName(dataset.getName()));
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
    BigQueryProject bigQueryProject =
        BigQueryProject.from(
            dataset,
            gcsConfiguration.getConnectTimeoutSeconds(),
            gcsConfiguration.getReadTimeoutSeconds());

    ST sqlTemplate = new ST(updateTransactionTableStatusTemplate);
    sqlTemplate.add("project", bigQueryProject.getProjectId());
    sqlTemplate.add("dataset", BigQueryPdao.prefixName(dataset.getName()));
    sqlTemplate.add("transactionTable", PDAO_TRANSACTIONS_TABLE);

    sqlTemplate.add("transactIdCol", PDAO_TRANSACTION_ID_COLUMN);
    sqlTemplate.add("transactStatusCol", PDAO_TRANSACTION_STATUS_COLUMN);
    sqlTemplate.add("transactTerminatedAtCol", PDAO_TRANSACTION_TERMINATED_AT_COLUMN);
    sqlTemplate.add("transactTerminatedByCol", PDAO_TRANSACTION_TERMINATED_BY_COLUMN);

    Instant terminatedAt = Instant.now();

    bigQueryProject.query(
        sqlTemplate.render(),
        Map.of(
            "transactId",
            QueryParameterValue.string(transactId.toString()),
            "transactStatus",
            QueryParameterValue.string(status.toString()),
            "transactTerminatedAt",
            QueryParameterValue.timestamp(DateTimeUtils.toEpochMicros(terminatedAt)),
            "transactTerminatedBy",
            QueryParameterValue.string(authedUser.getEmail())));

    return retrieveTransaction(dataset, transactId);
  }

  private static final String baseTransactionTableTemplate =
      "SELECT * FROM `<project>.<dataset>.<transactionTable>`";

  private static final String enumerateTransactionsTemplate =
      baseTransactionTableTemplate
          + " ORDER BY <transactCreatedAtCol> DESC LIMIT <limit> OFFSET <offset>";

  public List<TransactionModel> enumerateTransactions(Dataset dataset, long offset, long limit)
      throws InterruptedException {
    BigQueryProject bigQueryProject =
        BigQueryProject.from(
            dataset,
            gcsConfiguration.getConnectTimeoutSeconds(),
            gcsConfiguration.getReadTimeoutSeconds());

    ST sqlTemplate = new ST(enumerateTransactionsTemplate);
    sqlTemplate.add("project", bigQueryProject.getProjectId());
    sqlTemplate.add("dataset", BigQueryPdao.prefixName(dataset.getName()));
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
    BigQueryProject bigQueryProject =
        BigQueryProject.from(
            dataset,
            gcsConfiguration.getConnectTimeoutSeconds(),
            gcsConfiguration.getReadTimeoutSeconds());

    ST sqlTemplate = new ST(retrieveTransactionTemplate);
    sqlTemplate.add("project", bigQueryProject.getProjectId());
    sqlTemplate.add("dataset", BigQueryPdao.prefixName(dataset.getName()));
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
    BigQueryProject bigQueryProject =
        BigQueryProject.from(
            dataset,
            gcsConfiguration.getConnectTimeoutSeconds(),
            gcsConfiguration.getReadTimeoutSeconds());

    ST sqlTemplate = new ST(verifyTransactionTemplate);
    sqlTemplate.add("project", bigQueryProject.getProjectId());
    sqlTemplate.add("dataset", BigQueryPdao.prefixName(dataset.getName()));
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
                "transactId",
                QueryParameterValue.string(transactId.toString()),
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

  private static final String rollbackDatasetTableTemplate =
      "DELETE FROM `<project>.<dataset>.<targetTable>` WHERE <transactIdColumn>=@transactId";

  public void rollbackDatasetTable(Dataset dataset, String targetTableName, UUID transactId)
      throws InterruptedException {
    if (transactId == null) {
      throw new PdaoException("Can not roll back a null transaction");
    }
    BigQueryProject bigQueryProject =
        BigQueryProject.from(
            dataset,
            gcsConfiguration.getConnectTimeoutSeconds(),
            gcsConfiguration.getReadTimeoutSeconds());

    ST sqlTemplate = new ST(rollbackDatasetTableTemplate);
    sqlTemplate.add("project", bigQueryProject.getProjectId());
    sqlTemplate.add("dataset", BigQueryPdao.prefixName(dataset.getName()));
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
    BigQueryProject bigQueryProject =
        BigQueryProject.from(
            dataset,
            gcsConfiguration.getConnectTimeoutSeconds(),
            gcsConfiguration.getReadTimeoutSeconds());

    ST sqlTemplate = new ST(rollbackDatasetMetadataTableTemplate);
    sqlTemplate.add("project", bigQueryProject.getProjectId());
    sqlTemplate.add("dataset", BigQueryPdao.prefixName(dataset.getName()));
    sqlTemplate.add("metadataTable", targetTable.getRowMetadataTableName());
    sqlTemplate.add("targetTable", targetTable.getRawTableName());
    sqlTemplate.add("transactIdColumn", PDAO_TRANSACTION_ID_COLUMN);
    sqlTemplate.add("datarepoIdCol", PDAO_ROW_ID_COLUMN);

    bigQueryProject.query(
        sqlTemplate.render(),
        Map.of("transactId", QueryParameterValue.string(transactId.toString())));
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

  static Schema buildTransactionsTableSchema() {
    return Schema.of(
        Field.newBuilder(PDAO_TRANSACTION_ID_COLUMN, LegacySQLTypeName.STRING)
            .setMode(Field.Mode.REQUIRED)
            .build(),
        Field.newBuilder(PDAO_TRANSACTION_STATUS_COLUMN, LegacySQLTypeName.STRING)
            .setMode(Field.Mode.REQUIRED)
            .build(),
        Field.newBuilder(PDAO_TRANSACTION_LOCK_COLUMN, LegacySQLTypeName.STRING)
            .setMode(Field.Mode.NULLABLE)
            .build(),
        Field.newBuilder(PDAO_TRANSACTION_DESCRIPTION_COLUMN, LegacySQLTypeName.STRING)
            .setMode(Field.Mode.NULLABLE)
            .build(),
        Field.newBuilder(PDAO_TRANSACTION_CREATED_AT_COLUMN, LegacySQLTypeName.TIMESTAMP)
            .setMode(Field.Mode.REQUIRED)
            .build(),
        Field.newBuilder(PDAO_TRANSACTION_CREATED_BY_COLUMN, LegacySQLTypeName.STRING)
            .setMode(Field.Mode.REQUIRED)
            .build(),
        Field.newBuilder(PDAO_TRANSACTION_TERMINATED_AT_COLUMN, LegacySQLTypeName.TIMESTAMP)
            .setMode(Field.Mode.NULLABLE)
            .build(),
        Field.newBuilder(PDAO_TRANSACTION_TERMINATED_BY_COLUMN, LegacySQLTypeName.STRING)
            .setMode(Field.Mode.NULLABLE)
            .build());
  }
}
