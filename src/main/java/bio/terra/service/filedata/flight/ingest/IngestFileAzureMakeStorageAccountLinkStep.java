package bio.terra.service.filedata.flight.ingest;

import bio.terra.common.exception.RetryQueryException;
import bio.terra.service.common.CommonMapKeys;
import bio.terra.service.dataset.Dataset;
import bio.terra.service.dataset.DatasetStorageAccountDao;
import bio.terra.service.filedata.flight.FileMapKeys;
import bio.terra.service.resourcemanagement.azure.AzureStorageAccountResource;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.FlightMap;
import bio.terra.stairway.Step;
import bio.terra.stairway.StepResult;
import bio.terra.stairway.StepStatus;

public class IngestFileAzureMakeStorageAccountLinkStep implements Step {
  private final DatasetStorageAccountDao datasetStorageAccountDao;
  private final Dataset dataset;

  public IngestFileAzureMakeStorageAccountLinkStep(
      DatasetStorageAccountDao datasetStorageAccountDao, Dataset dataset) {
    this.datasetStorageAccountDao = datasetStorageAccountDao;
    this.dataset = dataset;
  }

  @Override
  public StepResult doStep(FlightContext context) throws InterruptedException {
    FlightMap workingMap = context.getWorkingMap();
    Boolean loadComplete = workingMap.get(FileMapKeys.LOAD_COMPLETED, Boolean.class);
    if (loadComplete == null || !loadComplete) {
      AzureStorageAccountResource storageAccountForFile =
          workingMap.get(
              CommonMapKeys.DATASET_STORAGE_ACCOUNT_RESOURCE, AzureStorageAccountResource.class);
      try {
        datasetStorageAccountDao.createDatasetStorageAccountLink(
            dataset.getId(), storageAccountForFile.getResourceId(), true);
      } catch (RetryQueryException ex) {
        return new StepResult(StepStatus.STEP_RESULT_FAILURE_RETRY);
      }
    }
    return StepResult.getStepResultSuccess();
  }

  @Override
  public StepResult undoStep(FlightContext context) {
    // Parallel threads can try create the storage account link. We do not error on a
    // duplicate create attempt.
    // Therefore, we do not delete the link during undo. Instead, we use a counter on the storage
    // account link that counts successful ingests. Since this ingest is failing, we decrement
    // the counter.
    FlightMap workingMap = context.getWorkingMap();
    Boolean loadComplete = workingMap.get(FileMapKeys.LOAD_COMPLETED, Boolean.class);
    if (loadComplete == null || !loadComplete) {
      AzureStorageAccountResource storageAccountForFile =
          workingMap.get(
              CommonMapKeys.DATASET_STORAGE_ACCOUNT_RESOURCE, AzureStorageAccountResource.class);
      try {
        datasetStorageAccountDao.decrementDatasetStorageAccountLink(
            dataset.getId(), storageAccountForFile.getResourceId());
      } catch (RetryQueryException ex) {
        return new StepResult(StepStatus.STEP_RESULT_FAILURE_RETRY);
      }
    }
    return StepResult.getStepResultSuccess();
  }
}
