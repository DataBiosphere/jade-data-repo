package bio.terra.service.common;

import bio.terra.common.BaseStep;
import bio.terra.model.ResourceLocks;
import bio.terra.stairway.StepResult;

public abstract class ResourceLockSetResponseStep extends BaseStep {

  protected ResourceLockSetResponseStep() {}

  @Override
  public StepResult perform() {
    setResponse(getResourceLocks());
    return StepResult.getStepResultSuccess();
  }

  protected abstract ResourceLocks getResourceLocks();
}
