package bio.terra.service.filedata.flight.ingest;

import bio.terra.service.common.CreateAzureStorageAccountStep;
import bio.terra.service.dataset.Dataset;
import bio.terra.service.dataset.flight.ingest.SkippableStep;
import bio.terra.service.filedata.flight.FileMapKeys;
import bio.terra.service.resourcemanagement.ResourceService;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.FlightMap;
import bio.terra.stairway.StepResult;
import java.util.function.Predicate;

public class IngestFileAzurePrimaryDataLocationStep extends CreateAzureStorageAccountStep {

  private final ResourceService resourceService;
  private final Dataset dataset;

  public IngestFileAzurePrimaryDataLocationStep(
      ResourceService resourceService, Dataset dataset, Predicate<FlightContext> skipCondition) {
    super(resourceService, dataset, skipCondition);
    this.resourceService = resourceService;
    this.dataset = dataset;
  }

  public IngestFileAzurePrimaryDataLocationStep(ResourceService resourceService, Dataset dataset) {
    this(resourceService, dataset, SkippableStep::neverSkip);
  }

  @Override
  public StepResult doSkippableStep(FlightContext context) throws InterruptedException {
    FlightMap workingMap = context.getWorkingMap();
    Boolean loadComplete = workingMap.get(FileMapKeys.LOAD_COMPLETED, Boolean.class);
    if (loadComplete == null || !loadComplete) {
      // Retrieve the already authorized billing profile from the working map and retrieve
      // or create a storage account in the context of that profile and the dataset.
      getOrCreateDatasetStorageAccount(context);
    }
    return StepResult.getStepResultSuccess();
  }
}
