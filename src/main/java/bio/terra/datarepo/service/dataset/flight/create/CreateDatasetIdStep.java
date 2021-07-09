package bio.terra.datarepo.service.dataset.flight.create;

import bio.terra.datarepo.service.dataset.flight.DatasetWorkingMapKeys;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.FlightMap;
import bio.terra.stairway.Step;
import bio.terra.stairway.StepResult;
import java.util.UUID;

public class CreateDatasetIdStep implements Step {

  public CreateDatasetIdStep() {}

  @Override
  public StepResult doStep(FlightContext context) {
    // creates the dataset id and puts it in the working map

    FlightMap workingMap = context.getWorkingMap();
    UUID datasetId = UUID.randomUUID();
    workingMap.put(DatasetWorkingMapKeys.DATASET_ID, datasetId);
    return StepResult.getStepResultSuccess();
  }

  @Override
  public StepResult undoStep(FlightContext context) {
    return StepResult.getStepResultSuccess();
  }
}
