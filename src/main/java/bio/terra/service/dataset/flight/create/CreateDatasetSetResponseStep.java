package bio.terra.service.dataset.flight.create;

import bio.terra.common.FlightUtils;
import bio.terra.model.DatasetSummaryModel;
import bio.terra.service.dataset.DatasetService;
import bio.terra.service.dataset.flight.DatasetWorkingMapKeys;
import bio.terra.service.job.DefaultUndoStep;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.StepResult;
import java.util.UUID;
import org.springframework.http.HttpStatus;

public class CreateDatasetSetResponseStep extends DefaultUndoStep {

  private final DatasetService datasetService;

  public CreateDatasetSetResponseStep(DatasetService datasetService) {
    this.datasetService = datasetService;
  }

  @Override
  public StepResult doStep(FlightContext context) {
    UUID datasetId = context.getWorkingMap().get(DatasetWorkingMapKeys.DATASET_ID, UUID.class);
    DatasetSummaryModel response = datasetService.retrieveDatasetSummary(datasetId);
    FlightUtils.setResponse(context, response, HttpStatus.CREATED);
    return StepResult.getStepResultSuccess();
  }
}
