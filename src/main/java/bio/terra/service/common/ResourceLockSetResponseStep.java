package bio.terra.service.common;

import bio.terra.common.FlightUtils;
import bio.terra.model.ResourceLocks;
import bio.terra.service.job.DefaultUndoStep;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.StepResult;
import org.springframework.http.HttpStatus;

public abstract class ResourceLockSetResponseStep extends DefaultUndoStep {

  protected ResourceLockSetResponseStep() {}

  @Override
  public StepResult doStep(FlightContext context) {
    ResourceLocks locks = getResourceLocks();
    FlightUtils.setResponse(context, locks, HttpStatus.OK);
    return StepResult.getStepResultSuccess();
  }

  protected abstract ResourceLocks getResourceLocks();
}
