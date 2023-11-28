package bio.terra.service.dataset.flight.lock;

import bio.terra.common.FlightUtils;
import bio.terra.model.DatasetSummaryModel;
import bio.terra.model.ResourceLocks;
import bio.terra.service.dataset.DatasetService;
import bio.terra.service.job.DefaultUndoStep;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.StepResult;
import java.util.UUID;
import org.springframework.http.HttpStatus;

public class DatasetLockSetResponseStep extends DefaultUndoStep {
  private final DatasetService datasetService;
  private final UUID datasetId;

  public DatasetLockSetResponseStep(DatasetService datasetService, UUID datasetId) {
    this.datasetService = datasetService;
    this.datasetId = datasetId;
  }

  @Override
  public StepResult doStep(FlightContext context) {
    DatasetSummaryModel response = datasetService.retrieveDatasetSummary(datasetId);
    ResourceLocks locks = response.getResourceLocks();
    FlightUtils.setResponse(context, locks, HttpStatus.OK);
    return StepResult.getStepResultSuccess();
  }
}
