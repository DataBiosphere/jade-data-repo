package bio.terra.service.dataset.flight.create;

import bio.terra.common.BaseStep;
import bio.terra.common.StepInput;
import bio.terra.service.dataset.DatasetService;
import bio.terra.stairway.StepResult;
import java.util.UUID;
import org.springframework.http.HttpStatus;

public class CreateDatasetSetResponseStep extends BaseStep {

  private final DatasetService datasetService;

  @StepInput private UUID datasetId;

  public CreateDatasetSetResponseStep(DatasetService datasetService) {
    this.datasetService = datasetService;
  }

  @Override
  public StepResult perform() {
    setResponse(datasetService.retrieveDatasetSummary(datasetId), HttpStatus.CREATED);
    return StepResult.getStepResultSuccess();
  }
}
