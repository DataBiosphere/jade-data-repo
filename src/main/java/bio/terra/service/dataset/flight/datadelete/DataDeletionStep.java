package bio.terra.service.dataset.flight.datadelete;

import static bio.terra.service.dataset.flight.datadelete.DataDeletionUtils.getRequest;

import bio.terra.common.FlightUtils;
import bio.terra.common.iam.AuthenticatedUserRequest;
import bio.terra.model.DataDeletionRequest;
import bio.terra.model.DataDeletionTableModel;
import bio.terra.model.DeleteResponseModel;
import bio.terra.service.common.gcs.BigQueryUtils;
import bio.terra.service.configuration.ConfigEnum;
import bio.terra.service.configuration.ConfigurationService;
import bio.terra.service.dataset.Dataset;
import bio.terra.service.dataset.DatasetService;
import bio.terra.service.dataset.flight.ingest.IngestUtils;
import bio.terra.service.dataset.flight.transactions.TransactionUtils;
import bio.terra.service.tabulardata.google.bigquery.BigQueryDatasetPdao;
import bio.terra.service.tabulardata.google.bigquery.BigQueryTransactionPdao;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.Step;
import bio.terra.stairway.StepResult;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;

public class DataDeletionStep extends BaseStep {
  private static Logger logger = LoggerFactory.getLogger(DataDeletionStep.class);

  private final BigQueryTransactionPdao bigQueryTransactionPdao;
  private final BigQueryDatasetPdao bigQueryDatasetPdao;
  private final DatasetService datasetService;
  private final ConfigurationService configService;
  private final AuthenticatedUserRequest userRequest;
  private final boolean autocommit;

  @StepInput private UUID datasetId;
  @StepInput private DataDeletionRequest request;

  public DataDeletionStep(
      BigQueryTransactionPdao bigQueryTransactionPdao,
      BigQueryDatasetPdao bigQueryDatasetPdao,
      DatasetService datasetService,
      ConfigurationService configService,
      AuthenticatedUserRequest userRequest,
      boolean autocommit) {
    this.bigQueryTransactionPdao = bigQueryTransactionPdao;
    this.bigQueryDatasetPdao = bigQueryDatasetPdao;
    this.datasetService = datasetService;
    this.configService = configService;
    this.userRequest = userRequest;
    this.autocommit = autocommit;
  }

  @Override
  public StepResult perform() throws InterruptedException {
    Dataset dataset = datasetService.retrieve(datasetId);
    String suffix = BigQueryUtils.getSuffix(???);
    List<String> tableNames =
        request.getTables().stream()
            .map(DataDeletionTableModel::getTableName)
            .collect(Collectors.toList());
    UUID transactionId = TransactionUtils.getTransactionId(???);

    bigQueryDatasetPdao.validateDeleteRequest(dataset, dataDeletionRequest.getTables(), suffix);

    if (configService.testInsertFault(ConfigEnum.SOFT_DELETE_LOCK_CONFLICT_STOP_FAULT)) {
      logger.info("SOFT_DELETE_LOCK_CONFLICT_STOP_FAULT");
      while (!configService.testInsertFault(ConfigEnum.SOFT_DELETE_LOCK_CONFLICT_CONTINUE_FAULT)) {
        logger.info("Sleeping for CONTINUE FAULT");
        TimeUnit.SECONDS.sleep(5);
      }
      logger.info("SOFT_DELETE_LOCK_CONFLICT_CONTINUE_FAULT");
    }

    bigQueryDatasetPdao.applySoftDeletes(
        dataset, tableNames, suffix, ???, transactionId, userRequest);

    // TODO<DR-2407>: this can be more informative, something like # rows deleted per table, or
    // mismatched
    // row ids
    DeleteResponseModel deleteResponseModel =
        new DeleteResponseModel().objectState(DeleteResponseModel.ObjectStateEnum.DELETED);
    setResponse(deleteResponseModel, HttpStatus.OK);

    return StepResult.getStepResultSuccess();
  }

  @Override
  public StepResult undo() {
    if (autocommit) {
      Dataset dataset = datasetService.retrieve(datasetId);
      UUID transactionId = TransactionUtils.getTransactionId(???);
      List<String> tableNames =
          request.getTables().stream()
              .map(DataDeletionTableModel::getTableName)
              .collect(Collectors.toList());
      dataset.getTables().stream()
          .filter(t -> tableNames.contains(t.getName()))
          .forEach(
              t -> {
                try {
                  bigQueryTransactionPdao.rollbackDatasetTable(
                      dataset, t.getSoftDeleteTableName(), transactionId);
                } catch (InterruptedException e) {
                  logger.warn(
                      String.format(
                          "Could not rollback soft delete data for table %s in transaction %s",
                          dataset.toLogString(), transactionId),
                      e);
                }
              });
    }
    return StepResult.getStepResultSuccess();
  }
}
