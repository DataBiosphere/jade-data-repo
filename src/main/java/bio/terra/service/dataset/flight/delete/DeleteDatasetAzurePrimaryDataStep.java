package bio.terra.service.dataset.flight.delete;

import bio.terra.service.configuration.ConfigEnum;
import bio.terra.service.configuration.ConfigurationService;
import bio.terra.service.dataset.Dataset;
import bio.terra.service.dataset.DatasetService;
import bio.terra.service.filedata.azure.blobstore.AzureBlobStorePdao;
import bio.terra.service.filedata.google.firestore.FireStoreDao;
import bio.terra.service.job.JobMapKeys;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.FlightMap;
import bio.terra.stairway.Step;
import bio.terra.stairway.StepResult;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;

public class DeleteDatasetAzurePrimaryDataStep implements Step {

  private static final Logger logger =
      LoggerFactory.getLogger(DeleteDatasetAzurePrimaryDataStep.class);

  private final AzureBlobStorePdao azureBlobStorePdao;
  private final FireStoreDao fileDao;
  private final DatasetService datasetService;
  private final UUID datasetId;
  private final ConfigurationService configService;

  public DeleteDatasetAzurePrimaryDataStep(
      AzureBlobStorePdao azureBlobStorePdao,
      FireStoreDao fileDao,
      DatasetService datasetService,
      UUID datasetId,
      ConfigurationService configService) {
    this.azureBlobStorePdao = azureBlobStorePdao;
    this.fileDao = fileDao;
    this.datasetService = datasetService;
    this.datasetId = datasetId;
    this.configService = configService;
  }

  @Override
  public StepResult doStep(FlightContext context) throws InterruptedException {
    Dataset dataset = datasetService.retrieve(datasetId);
    fileDao.deleteFilesFromDataset(dataset, azureBlobStorePdao::deleteFile);

    // this fault is used by the DatasetConnectedTest > testOverlappingDeletes
    if (configService.testInsertFault(ConfigEnum.DATASET_DELETE_LOCK_CONFLICT_STOP_FAULT)) {
      logger.info("DATASET_DELETE_LOCK_CONFLICT_STOP_FAULT");
      while (!configService.testInsertFault(
          ConfigEnum.DATASET_DELETE_LOCK_CONFLICT_CONTINUE_FAULT)) {
        logger.info("Sleeping for CONTINUE FAULT");
        TimeUnit.SECONDS.sleep(5);
      }
      logger.info("DATASET_DELETE_LOCK_CONFLICT_CONTINUE_FAULT");
    }

    FlightMap map = context.getWorkingMap();
    map.put(JobMapKeys.STATUS_CODE.getKeyName(), HttpStatus.NO_CONTENT);
    return StepResult.getStepResultSuccess();
  }

  @Override
  public StepResult undoStep(FlightContext context) {
    // can't undo delete
    return StepResult.getStepResultSuccess();
  }
}
