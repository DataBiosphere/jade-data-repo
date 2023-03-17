package bio.terra.service.dataset.flight.delete;

import bio.terra.service.dataset.Dataset;
import bio.terra.service.dataset.DatasetService;
import bio.terra.service.resourcemanagement.ResourceService;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.Step;
import bio.terra.stairway.StepResult;
import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DeleteDatasetDeleteStorageAccountsStep implements Step {
  private static final Logger logger =
      LoggerFactory.getLogger(DeleteDatasetDeleteStorageAccountsStep.class);
  private final ResourceService resourceService;
  private final DatasetService datasetService;
  private final UUID datasetId;

  public DeleteDatasetDeleteStorageAccountsStep(
      ResourceService resourceService, DatasetService datasetService, UUID datasetId) {
    this.resourceService = resourceService;
    this.datasetService = datasetService;
    this.datasetId = datasetId;
  }

  @Override
  public StepResult doStep(FlightContext context) throws InterruptedException {
    Dataset dataset = datasetService.retrieve(datasetId);
    logger.info("Deleting a storage account for Azure backed dataset");
    resourceService.deleteStorageContainer(dataset, context.getFlightId());
    return StepResult.getStepResultSuccess();
  }

  @Override
  public StepResult undoStep(FlightContext context) throws InterruptedException {
    // Leaving artifacts on undo
    return StepResult.getStepResultSuccess();
  }
}
