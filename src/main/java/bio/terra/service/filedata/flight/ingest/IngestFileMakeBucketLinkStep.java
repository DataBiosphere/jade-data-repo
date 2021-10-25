package bio.terra.service.filedata.flight.ingest;

import bio.terra.common.exception.RetryQueryException;
import bio.terra.service.dataset.Dataset;
import bio.terra.service.dataset.DatasetBucketDao;
import bio.terra.service.dataset.flight.ingest.OptionalStep;
import bio.terra.service.filedata.flight.FileMapKeys;
import bio.terra.service.resourcemanagement.google.GoogleBucketResource;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.FlightMap;
import bio.terra.stairway.StepResult;
import bio.terra.stairway.StepStatus;
import java.util.function.Predicate;

public class IngestFileMakeBucketLinkStep extends OptionalStep {
  private final DatasetBucketDao datasetBucketDao;
  private final Dataset dataset;

  public IngestFileMakeBucketLinkStep(
      DatasetBucketDao datasetBucketDao, Dataset dataset, Predicate<FlightContext> doCondition) {
    super(doCondition);
    this.datasetBucketDao = datasetBucketDao;
    this.dataset = dataset;
  }

  public IngestFileMakeBucketLinkStep(DatasetBucketDao datasetBucketDao, Dataset dataset) {
    this(datasetBucketDao, dataset, OptionalStep::alwaysDo);
  }

  @Override
  public StepResult doOptionalStep(FlightContext context) throws InterruptedException {
    FlightMap workingMap = context.getWorkingMap();
    Boolean loadComplete = workingMap.get(FileMapKeys.LOAD_COMPLETED, Boolean.class);
    if (loadComplete == null || !loadComplete) {
      GoogleBucketResource bucketForFile =
          workingMap.get(FileMapKeys.BUCKET_INFO, GoogleBucketResource.class);
      try {
        datasetBucketDao.createDatasetBucketLink(dataset.getId(), bucketForFile.getResourceId());
      } catch (RetryQueryException ex) {
        return new StepResult(StepStatus.STEP_RESULT_FAILURE_RETRY);
      }
    }
    return StepResult.getStepResultSuccess();
  }

  @Override
  public StepResult undoOptionalStep(FlightContext context) {
    // Parallel threads can try create the bucket link. We do not error on a duplicate create
    // attempt.
    // Therefore, we do not delete the link during undo. Instead, we use a counter on the bucket
    // link
    // that counts successful ingests. Since this ingest is failing, we decrement the counter.
    FlightMap workingMap = context.getWorkingMap();
    Boolean loadComplete = workingMap.get(FileMapKeys.LOAD_COMPLETED, Boolean.class);
    if (loadComplete == null || !loadComplete) {
      GoogleBucketResource bucketForFile =
          workingMap.get(FileMapKeys.BUCKET_INFO, GoogleBucketResource.class);
      try {
        datasetBucketDao.decrementDatasetBucketLink(dataset.getId(), bucketForFile.getResourceId());
      } catch (RetryQueryException ex) {
        return new StepResult(StepStatus.STEP_RESULT_FAILURE_RETRY);
      }
    }
    return StepResult.getStepResultSuccess();
  }
}
