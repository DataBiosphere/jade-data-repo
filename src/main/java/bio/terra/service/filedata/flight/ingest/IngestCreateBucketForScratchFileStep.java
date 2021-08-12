package bio.terra.service.filedata.flight.ingest;

import bio.terra.service.dataset.Dataset;
import bio.terra.service.dataset.flight.ingest.SkippableStep;
import bio.terra.service.filedata.flight.FileMapKeys;
import bio.terra.service.resourcemanagement.ResourceService;
import bio.terra.service.resourcemanagement.exception.BucketLockException;
import bio.terra.service.resourcemanagement.exception.GoogleResourceNamingException;
import bio.terra.service.resourcemanagement.google.GoogleBucketResource;
import bio.terra.service.resourcemanagement.google.GoogleProjectResource;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.FlightMap;
import bio.terra.stairway.StepResult;
import bio.terra.stairway.StepStatus;
import java.util.function.Predicate;

public class IngestCreateBucketForScratchFileStep extends SkippableStep {

  private final ResourceService resourceService;
  private final Dataset dataset;

  public IngestCreateBucketForScratchFileStep(
      ResourceService resourceService, Dataset dataset, Predicate<FlightContext> skipCondition) {
    super(skipCondition);
    this.resourceService = resourceService;
    this.dataset = dataset;
  }

  public IngestCreateBucketForScratchFileStep(ResourceService resourceService, Dataset dataset) {
    this.resourceService = resourceService;
    this.dataset = dataset;
  }

  @Override
  public StepResult doSkippableStep(FlightContext context) throws InterruptedException {
    FlightMap workingMap = context.getWorkingMap();
    GoogleProjectResource googleProjectResource =
        workingMap.get(FileMapKeys.PROJECT_RESOURCE, GoogleProjectResource.class);

    try {
      GoogleBucketResource bucketForFile =
          resourceService.getOrCreateBucketForIngestScratchFile(
              dataset, googleProjectResource, context.getFlightId());
      workingMap.put(FileMapKeys.INGEST_FILE_BUCKET_INFO, bucketForFile);
    } catch (BucketLockException blEx) {
      return new StepResult(StepStatus.STEP_RESULT_FAILURE_RETRY, blEx);
    } catch (GoogleResourceNamingException ex) {
      return new StepResult(StepStatus.STEP_RESULT_FAILURE_FATAL, ex);
    }
    return StepResult.getStepResultSuccess();
  }
}
