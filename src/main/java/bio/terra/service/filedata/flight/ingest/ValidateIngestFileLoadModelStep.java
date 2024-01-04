package bio.terra.service.filedata.flight.ingest;

import bio.terra.model.FileLoadModel;
import bio.terra.service.job.JobMapKeys;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.FlightMap;
import bio.terra.stairway.Step;
import bio.terra.stairway.StepResult;
import bio.terra.stairway.StepStatus;
import bio.terra.stairway.exception.RetryException;

public class ValidateIngestFileLoadModelStep implements Step {
  @Override
  public StepResult doStep(FlightContext context) throws InterruptedException, RetryException {
    FlightMap inputParameters = context.getInputParameters();
    FileLoadModel loadModel =
        inputParameters.get(JobMapKeys.REQUEST.getKeyName(), FileLoadModel.class);

    if (!loadModel.getTargetPath().startsWith("/")) {
      return new StepResult(
          StepStatus.STEP_RESULT_FAILURE_FATAL,
          new IllegalArgumentException("A target path must start with '/'"));
    }
    return StepResult.getStepResultSuccess();
  }

  @Override
  public StepResult undoStep(FlightContext context) throws InterruptedException {
    return StepResult.getStepResultSuccess();
  }
}
