package bio.terra.service.filedata.flight.ingest;

import bio.terra.service.common.CreateAzureStorageAccountStep;
import bio.terra.service.dataset.DatasetService;
import bio.terra.service.dataset.flight.ingest.SkippableStep;
import bio.terra.service.filedata.flight.FileMapKeys;
import bio.terra.service.resourcemanagement.ResourceService;
import bio.terra.service.resourcemanagement.exception.BucketLockException;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.FlightMap;
import bio.terra.stairway.StepResult;
import bio.terra.stairway.StepStatus;
import java.util.function.Predicate;

public class IngestFileAzurePrimaryDataLocationStep extends CreateAzureStorageAccountStep {

  private final ResourceService resourceService;
  private final DatasetService datasetService;

  public IngestFileAzurePrimaryDataLocationStep(
      DatasetService datasetService,
      ResourceService resourceService,
      Predicate<FlightContext> skipCondition) {
    super(datasetService, resourceService, skipCondition);
    this.resourceService = resourceService;
    this.datasetService = datasetService;
  }

  public IngestFileAzurePrimaryDataLocationStep(
      DatasetService datasetService, ResourceService resourceService) {
    this(datasetService, resourceService, SkippableStep::neverSkip);
  }

  @Override
  public StepResult doSkippableStep(FlightContext context) throws InterruptedException {
    FlightMap workingMap = context.getWorkingMap();
    Boolean loadComplete = workingMap.get(FileMapKeys.LOAD_COMPLETED, Boolean.class);
    if (loadComplete == null || !loadComplete) {
      try {
        // Retrieve the already authorized billing profile from the working map and retrieve
        // or create a storage account in the context of that profile and the dataset.
        getOrCreateDatasetStorageAccount(context);

        // TODO - is this just left over from GCP? I don't expect a bucket lock exception here
      } catch (BucketLockException blEx) {
        return new StepResult(StepStatus.STEP_RESULT_FAILURE_RETRY, blEx);
      }
    }
    return StepResult.getStepResultSuccess();
  }
}
