package bio.terra.service.filedata.flight.ingest;

import bio.terra.stairway.FlightContext;
import bio.terra.stairway.Step;
import bio.terra.stairway.StepResult;

public class IngestCreateBucketForScratchFileStep implements Step {

  public IngestCreateBucketForScratchFileStep() {}

  @Override
  public StepResult doStep(FlightContext context) {
    return StepResult.getStepResultSuccess();
  }

  @Override
  public StepResult undoStep(FlightContext context) {
    return StepResult.getStepResultSuccess();
  }
}
