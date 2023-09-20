package bio.terra.service.dataset.flight.delete;

import bio.terra.service.dataset.Dataset;
import bio.terra.service.dataset.DatasetService;
import bio.terra.service.job.DefaultUndoStep;
import bio.terra.service.tabulardata.azure.StorageTableService;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.StepResult;
import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DeleteDatasetLoadHistoryStorageTableStep extends DefaultUndoStep {

  private static final Logger logger =
      LoggerFactory.getLogger(DeleteDatasetLoadHistoryStorageTableStep.class);

  private final StorageTableService storageTableService;
  private final DatasetService datasetService;
  private final UUID datasetId;

  public DeleteDatasetLoadHistoryStorageTableStep(
      StorageTableService storageTableService, DatasetService datasetService, UUID datasetId) {
    this.storageTableService = storageTableService;
    this.datasetService = datasetService;
    this.datasetId = datasetId;
  }

  @Override
  public StepResult doStep(FlightContext context) throws InterruptedException {
    Dataset dataset = datasetService.retrieve(datasetId);
    storageTableService.dropLoadHistoryTable(dataset);
    return StepResult.getStepResultSuccess();
  }
}
