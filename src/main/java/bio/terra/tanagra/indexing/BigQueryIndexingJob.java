package bio.terra.tanagra.indexing;

import bio.terra.tanagra.exception.InvalidConfigException;
import bio.terra.tanagra.exception.SystemException;
import bio.terra.tanagra.query.ColumnHeaderSchema;
import bio.terra.tanagra.query.ColumnSchema;
import bio.terra.tanagra.query.FieldPointer;
import bio.terra.tanagra.query.FieldVariable;
import bio.terra.tanagra.query.FilterVariable;
import bio.terra.tanagra.query.Literal;
import bio.terra.tanagra.query.Query;
import bio.terra.tanagra.query.QueryExecutor;
import bio.terra.tanagra.query.QueryRequest;
import bio.terra.tanagra.query.QueryResult;
import bio.terra.tanagra.query.TablePointer;
import bio.terra.tanagra.query.TableVariable;
import bio.terra.tanagra.query.UpdateFromSelect;
import bio.terra.tanagra.query.filtervariable.BinaryFilterVariable;
import bio.terra.tanagra.underlay.DataPointer;
import bio.terra.tanagra.underlay.Entity;
import bio.terra.tanagra.underlay.Underlay;
import bio.terra.tanagra.underlay.datapointer.BigQueryDataset;
import bio.terra.tanagra.utils.GoogleBigQuery;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.auth.oauth2.ServiceAccountCredentials;
import com.google.cloud.bigquery.BigQueryException;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Lists;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;
import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.runners.direct.DirectRunner;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.MoreObjects;
import org.apache.http.HttpStatus;
import org.joda.time.DateTimeUtils;
import org.joda.time.DateTimeZone;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

public abstract class BigQueryIndexingJob implements IndexingJob {
  private static final DateTimeFormatter FORMATTER =
      DateTimeFormat.forPattern("MMddHHmm").withZone(DateTimeZone.UTC);

  protected static final String DEFAULT_REGION = "us-central1";

  private final Entity entity;

  protected BigQueryIndexingJob(Entity entity) {
    this.entity = entity;
  }

  protected Entity getEntity() {
    return entity;
  }

  @VisibleForTesting
  public TablePointer getEntityIndexTable() {
    return getEntity().getMapping(Underlay.MappingType.INDEX).getTablePointer();
  }

  protected BigQueryDataset getBQDataPointer(TablePointer tablePointer) {
    DataPointer outputDataPointer = tablePointer.getDataPointer();
    if (!(outputDataPointer instanceof BigQueryDataset)) {
      throw new InvalidConfigException("Entity indexing job only supports BigQuery");
    }
    return (BigQueryDataset) outputDataPointer;
  }

  protected void deleteTable(TablePointer tablePointer, boolean isDryRun) {
    BigQueryDataset outputBQDataset = getBQDataPointer(tablePointer);
    if (isDryRun) {
      LOGGER.info("Delete table: {}", tablePointer.getPathForIndexing());
    } else {
      getBQDataPointer(tablePointer)
          .getBigQueryService()
          .deleteTable(
              outputBQDataset.getProjectId(),
              outputBQDataset.getDatasetId(),
              tablePointer.getTableName());
    }
  }

  // -----Helper methods for checking whether a job has run already.-------
  protected boolean checkTableExists(TablePointer tablePointer, QueryExecutor executor) {
    BigQueryDataset outputBQDataset = getBQDataPointer(tablePointer);
    LOGGER.info(
        "output BQ table: project={}, dataset={}, table={}",
        outputBQDataset.getProjectId(),
        outputBQDataset.getDatasetId(),
        tablePointer.getTableName());
    GoogleBigQuery googleBigQuery = outputBQDataset.getBigQueryService();
    return googleBigQuery
        .getTable(
            outputBQDataset.getProjectId(),
            outputBQDataset.getDatasetId(),
            tablePointer.getTableName())
        .isPresent();
  }

  protected boolean checkOneNotNullIdRowExists(Entity entity, QueryExecutor executor) {
    // Check if the table has at least 1 id row where id IS NOT NULL
    FieldPointer idField =
        getEntity().getIdAttribute().getMapping(Underlay.MappingType.INDEX).getValue();
    ColumnSchema idColumnSchema =
        getEntity()
            .getIdAttribute()
            .getMapping(Underlay.MappingType.INDEX)
            .buildValueColumnSchema();
    return checkOneNotNullRowExists(idField, idColumnSchema, executor);
  }

  protected boolean checkOneNotNullRowExists(
      FieldPointer fieldPointer, ColumnSchema columnSchema, QueryExecutor executor) {
    // Check if the table has at least 1 row with a non-null field value.
    TableVariable outputTableVar = TableVariable.forPrimary(fieldPointer.getTablePointer());
    List<TableVariable> tableVars = Lists.newArrayList(outputTableVar);

    FieldVariable fieldVar = fieldPointer.buildVariable(outputTableVar, tableVars);
    FilterVariable fieldNotNull =
        new BinaryFilterVariable(
            fieldVar, BinaryFilterVariable.BinaryOperator.IS_NOT, new Literal(null));
    Query query =
        new Query.Builder()
            .select(List.of(fieldVar))
            .tables(tableVars)
            .where(fieldNotNull)
            .limit(1)
            .build();

    ColumnHeaderSchema columnHeaderSchema = new ColumnHeaderSchema(List.of(columnSchema));
    QueryRequest queryRequest = new QueryRequest(query, columnHeaderSchema);
    QueryResult queryResult = executor.execute(queryRequest);

    return queryResult.getRowResults().iterator().hasNext();
  }

  // -----Helper methods for running insert/update jobs in BigQuery directly (i.e. not via
  // Dataflow).-------
  protected void updateEntityTableFromSelect(
      Query selectQuery,
      Map<String, FieldVariable> updateFieldMap,
      String selectIdFieldName,
      boolean isDryRun,
      QueryExecutor executor) {
    // Build a TableVariable for the (output) entity table that we want to update.
    List<TableVariable> outputTables = new ArrayList<>();
    TableVariable entityTable =
        TableVariable.forPrimary(
            getEntity().getMapping(Underlay.MappingType.INDEX).getTablePointer());
    outputTables.add(entityTable);

    // Build a map of (output) update FieldVariable -> (input) selected FieldVariable.
    Map<FieldVariable, FieldVariable> updateFields = new HashMap<>();
    for (Map.Entry<String, FieldVariable> updateToSelect : updateFieldMap.entrySet()) {
      String updateFieldName = updateToSelect.getKey();
      FieldVariable selectField = updateToSelect.getValue();
      FieldVariable updateField =
          new FieldPointer.Builder()
              .tablePointer(entityTable.getTablePointer())
              .columnName(updateFieldName)
              .build()
              .buildVariable(entityTable, outputTables);
      updateFields.put(updateField, selectField);
    }

    // Build a FieldVariable for the id field in the (output) entity table.
    FieldVariable updateIdField =
        getEntity()
            .getIdAttribute()
            .getMapping(Underlay.MappingType.INDEX)
            .getValue()
            .buildVariable(entityTable, outputTables);

    // Get the FieldVariable for the id field in the input table.
    FieldVariable selectIdField =
        selectQuery.getSelect().stream()
            .filter(fv -> fv.getAliasOrColumnName().equals(selectIdFieldName))
            .findFirst()
            .get();

    UpdateFromSelect updateQuery =
        new UpdateFromSelect(entityTable, updateFields, selectQuery, updateIdField, selectIdField);
    LOGGER.info("Generated SQL: {}", updateQuery);
    try {
      insertUpdateTableFromSelect(executor.renderSQL(updateQuery), isDryRun);
    } catch (BigQueryException bqEx) {
      if (bqEx.getCode() == HttpStatus.SC_NOT_FOUND) {
        LOGGER.info(
            "Query dry run failed because table has not been created yet: {}",
            bqEx.getError().getMessage());
      } else {
        throw bqEx;
      }
    }
  }

  protected void insertUpdateTableFromSelect(String sql, boolean isDryRun) {
    getBQDataPointer(getEntityIndexTable())
        .getBigQueryService()
        .runInsertUpdateQuery(sql, isDryRun);
  }

  // -----Helper methods for running Dataflow jobs that read/write to BigQuery.-------
  protected BigQueryOptions buildDataflowPipelineOptions(BigQueryDataset outputBQDataset) {
    // If the BQ dataset defines a service account, then specify that.
    // Otherwise, try to get the service account email for the application default credentials.
    String serviceAccountEmail = outputBQDataset.getDataflowServiceAccountEmail();
    if (serviceAccountEmail == null) {
      serviceAccountEmail = getAppDefaultSAEmail();
    }
    LOGGER.info("Dataflow service account: {}", serviceAccountEmail);

    DataflowPipelineOptions dataflowOptions =
        PipelineOptionsFactory.create().as(DataflowPipelineOptions.class);
    dataflowOptions.setRunner(DirectRunner.class);
    dataflowOptions.setProject(outputBQDataset.getProjectId());
    // TODO: Allow overriding the default region.
    dataflowOptions.setRegion(DEFAULT_REGION);
    dataflowOptions.setServiceAccount(serviceAccountEmail);
    dataflowOptions.setJobName(getDataflowJobName());

    if (outputBQDataset.getDataflowTempLocation() != null) {
      dataflowOptions.setTempLocation(outputBQDataset.getDataflowTempLocation());
      LOGGER.info("Dataflow temp location: {}", dataflowOptions.getTempLocation());
    }

    return dataflowOptions;
  }

  /** Build a name for the Dataflow job that will be visible in the Cloud Console. */
  private String getDataflowJobName() {
    String underlayName = entity.getUnderlay().getName();
    String normalizedUnderlayName = underlayName.toLowerCase().replaceAll("[^a-z0-9]", "-");
    String jobDisplayName = getName();
    String normalizedJobDisplayName =
        jobDisplayName == null || jobDisplayName.length() == 0
            ? "t-BQIndexingJob"
            : "t-" + jobDisplayName.toLowerCase().replaceAll("[^a-z0-9]", "-");
    String userName = MoreObjects.firstNonNull(System.getProperty("user.name"), "");
    String normalizedUserName = userName.toLowerCase().replaceAll("[^a-z0-9]", "0");
    String datePart = FORMATTER.print(DateTimeUtils.currentTimeMillis());

    String randomPart = Integer.toHexString(ThreadLocalRandom.current().nextInt());
    return String.format(
        "%s-%s-%s-%s-%s",
        normalizedUnderlayName, normalizedJobDisplayName, normalizedUserName, datePart, randomPart);
  }

  protected String getAppDefaultSAEmail() {
    GoogleCredentials appDefaultSACredentials;
    try {
      appDefaultSACredentials = GoogleCredentials.getApplicationDefault();
    } catch (IOException ioEx) {
      throw new SystemException("Error reading application default credentials.", ioEx);
    }

    // Get email if this is a service account.
    try {
      String serviceAccountEmail =
          ((ServiceAccountCredentials) appDefaultSACredentials).getClientEmail();
      LOGGER.info("Service account email: {}", serviceAccountEmail);
      return serviceAccountEmail;
    } catch (ClassCastException ccEx) {
      LOGGER.debug("Application default credentials are not a service account.", ccEx);
    }

    return "";
  }

  protected String getTempTableName(String suffix) {
    return getEntity().getName() + "_" + suffix;
  }
}
