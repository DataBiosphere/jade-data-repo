package bio.terra.service.dataset.flight.datadelete;

import bio.terra.common.BaseStep;
import bio.terra.common.StepInput;
import bio.terra.model.DataDeletionRequest;
import bio.terra.model.DataDeletionTableModel;
import bio.terra.model.DeleteResponseModel;
import bio.terra.service.configuration.ConfigEnum;
import bio.terra.service.configuration.ConfigurationService;
import bio.terra.service.dataset.Dataset;
import bio.terra.service.dataset.DatasetService;
import bio.terra.service.tabulardata.google.BigQueryPdao;
import bio.terra.stairway.StepResult;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;

public class DataDeletionStep extends BaseStep {

  private final BigQueryPdao bigQueryPdao;
  private final DatasetService datasetService;
  private final ConfigurationService configService;

  @StepInput private UUID datasetId;
  @StepInput private DataDeletionRequest request;

  private static Logger logger = LoggerFactory.getLogger(DataDeletionStep.class);

  public DataDeletionStep(
      BigQueryPdao bigQueryPdao,
      DatasetService datasetService,
      ConfigurationService configService) {
    this.bigQueryPdao = bigQueryPdao;
    this.datasetService = datasetService;
    this.configService = configService;
  }

  @Override
  public StepResult perform() throws InterruptedException {
    Dataset dataset = datasetService.retrieve(datasetId);
    String suffix = DataDeletionUtils.getSuffix(getContext());
    List<String> tableNames =
        request.getTables().stream()
            .map(DataDeletionTableModel::getTableName)
            .collect(Collectors.toList());

    bigQueryPdao.validateDeleteRequest(dataset, request.getTables(), suffix);

    if (configService.testInsertFault(ConfigEnum.SOFT_DELETE_LOCK_CONFLICT_STOP_FAULT)) {
      logger.info("SOFT_DELETE_LOCK_CONFLICT_STOP_FAULT");
      while (!configService.testInsertFault(ConfigEnum.SOFT_DELETE_LOCK_CONFLICT_CONTINUE_FAULT)) {
        logger.info("Sleeping for CONTINUE FAULT");
        TimeUnit.SECONDS.sleep(5);
      }
      logger.info("SOFT_DELETE_LOCK_CONFLICT_CONTINUE_FAULT");
    }

    bigQueryPdao.applySoftDeletes(dataset, tableNames, suffix);

    // TODO: this can be more informative, something like # rows deleted per table, or mismatched
    // row ids
    DeleteResponseModel deleteResponseModel =
        new DeleteResponseModel().objectState(DeleteResponseModel.ObjectStateEnum.DELETED);
    setResponse(deleteResponseModel, HttpStatus.OK);

    return StepResult.getStepResultSuccess();
  }
}
