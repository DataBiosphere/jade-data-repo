package bio.terra.service.dataset.flight.delete;

import bio.terra.service.configuration.ConfigEnum;
import bio.terra.service.configuration.ConfigurationService;
import bio.terra.service.dataset.Dataset;
import bio.terra.service.dataset.DatasetService;
import bio.terra.service.filedata.google.firestore.FireStoreDao;
import bio.terra.service.filedata.google.gcs.GcsPdao;
import bio.terra.service.job.JobMapKeys;
import bio.terra.service.tabulardata.google.bigquery.BigQueryDatasetPdao;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.FlightMap;
import bio.terra.stairway.Step;
import bio.terra.stairway.StepResult;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;

public class DeleteDatasetPrimaryDataStep implements Step {

  private static Logger logger = LoggerFactory.getLogger(DeleteDatasetPrimaryDataStep.class);

  private BigQueryDatasetPdao bigQueryDatasetPdao;
  private GcsPdao gcsPdao;
  private FireStoreDao fileDao;
  private DatasetService datasetService;
  private UUID datasetId;
  private ConfigurationService configService;

  public DeleteDatasetPrimaryDataStep(
      BigQueryDatasetPdao bigQueryDatasetPdao,
      GcsPdao gcsPdao,
      FireStoreDao fileDao,
      DatasetService datasetService,
      UUID datasetId,
      ConfigurationService configService) {
    this.bigQueryDatasetPdao = bigQueryDatasetPdao;
    this.gcsPdao = gcsPdao;
    this.fileDao = fileDao;
    this.datasetService = datasetService;
    this.datasetId = datasetId;
    this.configService = configService;
  }

  @Override
  public StepResult doStep(FlightContext context) throws InterruptedException {
    Dataset dataset = datasetService.retrieve(datasetId);
    bigQueryDatasetPdao.deleteDataset(dataset);
    fileDao.deleteFilesFromDataset(dataset, fireStoreFile -> gcsPdao.deleteFile(fireStoreFile));

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
