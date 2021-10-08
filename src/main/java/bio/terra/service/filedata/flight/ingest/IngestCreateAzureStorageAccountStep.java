package bio.terra.service.filedata.flight.ingest;

import bio.terra.service.common.CreateAzureStorageAccountStep;
import bio.terra.service.dataset.DatasetService;
import bio.terra.service.resourcemanagement.ResourceService;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.StepResult;
import bio.terra.stairway.exception.RetryException;

public class IngestCreateAzureStorageAccountStep extends CreateAzureStorageAccountStep {

  public IngestCreateAzureStorageAccountStep(
      DatasetService datasetService, ResourceService resourceService) {
    super(datasetService, resourceService);
  }

  @Override
  public StepResult doStep(FlightContext context) throws InterruptedException, RetryException {
    getOrCreateDatasetStorageAccount(context);
    return StepResult.getStepResultSuccess();
  }

  @Override
  public StepResult undoStep(FlightContext context) throws InterruptedException {
    return StepResult.getStepResultSuccess();
  }
}
