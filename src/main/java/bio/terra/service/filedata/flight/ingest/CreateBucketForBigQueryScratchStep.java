package bio.terra.service.filedata.flight.ingest;

import bio.terra.service.common.gcs.CommonFlightKeys;
import bio.terra.service.dataset.Dataset;
import bio.terra.service.dataset.DatasetService;
import bio.terra.service.job.DefaultUndoStep;
import bio.terra.service.job.JobMapKeys;
import bio.terra.service.resourcemanagement.ResourceService;
import bio.terra.service.resourcemanagement.exception.BucketLockException;
import bio.terra.service.resourcemanagement.exception.GoogleResourceNamingException;
import bio.terra.service.resourcemanagement.google.GoogleBucketResource;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.FlightMap;
import bio.terra.stairway.StepResult;
import bio.terra.stairway.StepStatus;
import java.util.UUID;

public record CreateBucketForBigQueryScratchStep(
    ResourceService resourceService, DatasetService datasetService) implements DefaultUndoStep {

  @Override
  public StepResult doStep(FlightContext context) throws InterruptedException {
    FlightMap workingMap = context.getWorkingMap();
    FlightMap inputParameters = context.getInputParameters();
    UUID datasetId =
        UUID.fromString(inputParameters.get(JobMapKeys.DATASET_ID.getKeyName(), String.class));
    Dataset dataset = datasetService.retrieve(datasetId);
    try {
      GoogleBucketResource bucketForFile =
          resourceService.getOrCreateBucketForBigQueryScratchFile(dataset, context.getFlightId());
      workingMap.put(CommonFlightKeys.SCRATCH_BUCKET_INFO, bucketForFile);
    } catch (BucketLockException e) {
      return new StepResult(StepStatus.STEP_RESULT_FAILURE_RETRY, e);
    } catch (GoogleResourceNamingException e) {
      return new StepResult(StepStatus.STEP_RESULT_FAILURE_FATAL, e);
    }
    return StepResult.getStepResultSuccess();
  }
}
