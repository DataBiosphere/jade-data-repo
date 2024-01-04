package bio.terra.service.filedata.flight.ingest;

import bio.terra.service.common.CreateAzureStorageAccountStep;
import bio.terra.service.dataset.Dataset;
import bio.terra.service.resourcemanagement.ResourceService;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.StepResult;

public class IngestCreateAzureStorageAccountStep extends CreateAzureStorageAccountStep {

  public IngestCreateAzureStorageAccountStep(ResourceService resourceService, Dataset dataset) {
    super(resourceService, dataset);
  }

  @Override
  public StepResult doStep(FlightContext flightContext) throws InterruptedException {
    getOrCreateDatasetStorageAccount(flightContext);
    return StepResult.getStepResultSuccess();
  }
}
