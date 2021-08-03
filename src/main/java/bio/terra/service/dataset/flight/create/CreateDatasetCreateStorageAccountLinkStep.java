package bio.terra.service.dataset.flight.create;

import bio.terra.service.dataset.DatasetStorageAccountDao;
import bio.terra.service.dataset.flight.DatasetWorkingMapKeys;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.FlightMap;
import bio.terra.stairway.Step;
import bio.terra.stairway.StepResult;
import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CreateDatasetCreateStorageAccountLinkStep implements Step {
  private static final Logger logger =
      LoggerFactory.getLogger(CreateDatasetCreateStorageAccountLinkStep.class);
  private final DatasetStorageAccountDao datasetStorageAccountDao;

  public CreateDatasetCreateStorageAccountLinkStep(
      DatasetStorageAccountDao datasetStorageAccountDao) {
    this.datasetStorageAccountDao = datasetStorageAccountDao;
  }

  @Override
  public StepResult doStep(FlightContext context) throws InterruptedException {
    logger.info("Creating a storage account link for Azure backed dataset");
    FlightMap workingMap = context.getWorkingMap();
    UUID datasetId = workingMap.get(DatasetWorkingMapKeys.DATASET_ID, UUID.class);
    UUID storageAccountId =
        workingMap.get(DatasetWorkingMapKeys.STORAGE_ACCOUNT_RESOURCE_ID, UUID.class);

    datasetStorageAccountDao.createDatasetStorageAccountLink(datasetId, storageAccountId, false);
    return StepResult.getStepResultSuccess();
  }

  @Override
  public StepResult undoStep(FlightContext context) throws InterruptedException {
    logger.debug("Dataset / Storage account link creation failed. Deleting link metadata.");
    FlightMap workingMap = context.getWorkingMap();
    UUID datasetId = workingMap.get(DatasetWorkingMapKeys.DATASET_ID, UUID.class);
    UUID storageAccountId =
        workingMap.get(DatasetWorkingMapKeys.STORAGE_ACCOUNT_RESOURCE_ID, UUID.class);
    datasetStorageAccountDao.deleteDatasetStorageAccountLink(datasetId, storageAccountId);
    return StepResult.getStepResultSuccess();
  }
}
