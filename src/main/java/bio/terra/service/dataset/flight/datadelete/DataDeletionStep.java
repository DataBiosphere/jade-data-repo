package bio.terra.service.dataset.flight.datadelete;

import static bio.terra.service.dataset.flight.datadelete.DataDeletionUtils.getDataset;
import static bio.terra.service.dataset.flight.datadelete.DataDeletionUtils.getRequest;
import static bio.terra.service.dataset.flight.datadelete.DataDeletionUtils.getSuffix;

import bio.terra.common.FlightUtils;
import bio.terra.common.iam.AuthenticatedUserRequest;
import bio.terra.model.DataDeletionRequest;
import bio.terra.model.DataDeletionTableModel;
import bio.terra.model.DeleteResponseModel;
import bio.terra.service.configuration.ConfigEnum;
import bio.terra.service.configuration.ConfigurationService;
import bio.terra.service.dataset.Dataset;
import bio.terra.service.dataset.DatasetService;
import bio.terra.service.dataset.flight.xactions.TransactionUtils;
import bio.terra.service.tabulardata.google.BigQueryPdao;
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

public class DataDeletionStep implements Step {

  private final BigQueryPdao bigQueryPdao;
  private final DatasetService datasetService;
  private final ConfigurationService configService;
  private final AuthenticatedUserRequest userRequest;

  private static Logger logger = LoggerFactory.getLogger(DataDeletionStep.class);

  public DataDeletionStep(
      BigQueryPdao bigQueryPdao,
      DatasetService datasetService,
      ConfigurationService configService,
      AuthenticatedUserRequest userRequest) {
    this.bigQueryPdao = bigQueryPdao;
    this.datasetService = datasetService;
    this.configService = configService;
    this.userRequest = userRequest;
  }

  @Override
  public StepResult doStep(FlightContext context) throws InterruptedException {
    Dataset dataset = getDataset(context, datasetService);
    String suffix = getSuffix(context);
    DataDeletionRequest dataDeletionRequest = getRequest(context);
    List<String> tableNames =
        dataDeletionRequest.getTables().stream()
            .map(DataDeletionTableModel::getTableName)
            .collect(Collectors.toList());
    UUID transactionId = TransactionUtils.getTransactionId(context);

    bigQueryPdao.validateDeleteRequest(dataset, dataDeletionRequest.getTables(), suffix);

    if (configService.testInsertFault(ConfigEnum.SOFT_DELETE_LOCK_CONFLICT_STOP_FAULT)) {
      logger.info("SOFT_DELETE_LOCK_CONFLICT_STOP_FAULT");
      while (!configService.testInsertFault(ConfigEnum.SOFT_DELETE_LOCK_CONFLICT_CONTINUE_FAULT)) {
        logger.info("Sleeping for CONTINUE FAULT");
        TimeUnit.SECONDS.sleep(5);
      }
      logger.info("SOFT_DELETE_LOCK_CONFLICT_CONTINUE_FAULT");
    }

    bigQueryPdao.applySoftDeletes(
        dataset, tableNames, suffix, context.getFlightId(), transactionId, userRequest);

    // TODO<DR-2407>: this can be more informative, something like # rows deleted per table, or
    // mismatched
    // row ids
    DeleteResponseModel deleteResponseModel =
        new DeleteResponseModel().objectState(DeleteResponseModel.ObjectStateEnum.DELETED);
    FlightUtils.setResponse(context, deleteResponseModel, HttpStatus.OK);

    return StepResult.getStepResultSuccess();
  }

  @Override
  public StepResult undoStep(FlightContext context) {
    // The do step is atomic, either it worked or it didn't. There is no undo.
    return StepResult.getStepResultSuccess();
  }
}
