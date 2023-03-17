package bio.terra.service.dataset.flight.delete;

import bio.terra.common.iam.AuthenticatedUserRequest;
import bio.terra.service.common.CommonMapKeys;
import bio.terra.service.configuration.ConfigEnum;
import bio.terra.service.configuration.ConfigurationService;
import bio.terra.service.dataset.DatasetService;
import bio.terra.service.filedata.azure.blobstore.AzureBlobStorePdao;
import bio.terra.service.filedata.azure.tables.TableDao;
import bio.terra.service.job.JobMapKeys;
import bio.terra.service.profile.ProfileDao;
import bio.terra.service.resourcemanagement.ResourceService;
import bio.terra.service.resourcemanagement.azure.AzureStorageAuthInfo;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.FlightMap;
import bio.terra.stairway.Step;
import bio.terra.stairway.StepResult;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;

public class DeleteDatasetAzurePrimaryDataStep implements Step {

  private static final Logger logger =
      LoggerFactory.getLogger(DeleteDatasetAzurePrimaryDataStep.class);

  private final AzureBlobStorePdao azureBlobStorePdao;
  private final TableDao tableDao;
  private final DatasetService datasetService;
  private final UUID datasetId;
  private final ConfigurationService configService;
  private final ResourceService resourceService;
  private final ProfileDao profileDao;
  private final AuthenticatedUserRequest userRequest;

  public DeleteDatasetAzurePrimaryDataStep(
      AzureBlobStorePdao azureBlobStorePdao,
      TableDao tableDao,
      DatasetService datasetService,
      UUID datasetId,
      ConfigurationService configService,
      ResourceService resourceService,
      ProfileDao profileDao,
      AuthenticatedUserRequest userRequest) {
    this.azureBlobStorePdao = azureBlobStorePdao;
    this.tableDao = tableDao;
    this.datasetService = datasetService;
    this.datasetId = datasetId;
    this.configService = configService;
    this.resourceService = resourceService;
    this.profileDao = profileDao;
    this.userRequest = userRequest;
  }

  @Override
  public StepResult doStep(FlightContext context) throws InterruptedException {
    FlightMap map = context.getWorkingMap();
    AzureStorageAuthInfo storageAuthInfo =
        Objects.requireNonNull(
            map.get(CommonMapKeys.DATASET_STORAGE_AUTH_INFO, AzureStorageAuthInfo.class),
            "No Azure storage auth info found");

    tableDao.deleteFilesFromDataset(
        storageAuthInfo, datasetId, f -> azureBlobStorePdao.deleteFile(f, userRequest));

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

    map.put(JobMapKeys.STATUS_CODE.getKeyName(), HttpStatus.NO_CONTENT);
    return StepResult.getStepResultSuccess();
  }

  @Override
  public StepResult undoStep(FlightContext context) {
    // can't undo delete
    return StepResult.getStepResultSuccess();
  }
}
