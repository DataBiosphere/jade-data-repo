package bio.terra.service.dataset.flight.create;

import bio.terra.common.exception.RetryQueryException;
import bio.terra.service.dataset.DatasetBucketDao;
import bio.terra.service.dataset.flight.DatasetWorkingMapKeys;
import bio.terra.service.filedata.flight.FileMapKeys;
import bio.terra.service.resourcemanagement.google.GoogleBucketResource;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.FlightMap;
import bio.terra.stairway.Step;
import bio.terra.stairway.StepResult;
import bio.terra.stairway.StepStatus;
import java.util.UUID;

public class DatasetCreateMakeBucketLinkStep implements Step {
  private final DatasetBucketDao datasetBucketDao;

  public DatasetCreateMakeBucketLinkStep(DatasetBucketDao datasetBucketDao) {
    this.datasetBucketDao = datasetBucketDao;
  }

  @Override
  public StepResult doStep(FlightContext context) throws InterruptedException {
    FlightMap workingMap = context.getWorkingMap();
    UUID datasetId = workingMap.get(DatasetWorkingMapKeys.DATASET_ID, UUID.class);
    GoogleBucketResource bucketForFile =
        workingMap.get(FileMapKeys.BUCKET_INFO, GoogleBucketResource.class);
    try {
      datasetBucketDao.createDatasetBucketLink(datasetId, bucketForFile.getResourceId(), false);
    } catch (RetryQueryException ex) {
      return new StepResult(StepStatus.STEP_RESULT_FAILURE_RETRY);
    }
    return StepResult.getStepResultSuccess();
  }

  @Override
  public StepResult undoStep(FlightContext context) {
    FlightMap workingMap = context.getWorkingMap();
    UUID datasetId = workingMap.get(DatasetWorkingMapKeys.DATASET_ID, UUID.class);
    GoogleBucketResource bucketForFile =
        workingMap.get(FileMapKeys.BUCKET_INFO, GoogleBucketResource.class);
    try {
      datasetBucketDao.deleteDatasetBucketLink(datasetId, bucketForFile.getResourceId());
    } catch (RetryQueryException ex) {
      return new StepResult(StepStatus.STEP_RESULT_FAILURE_RETRY);
    }
    return StepResult.getStepResultSuccess();
  }
}
