package bio.terra.service.common;

import bio.terra.service.job.DefaultUndoStep;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.StepResult;
import bio.terra.stairway.StepStatus;
import bio.terra.stairway.exception.RetryException;

public abstract class UnlockResourceCheckLockNameStep extends DefaultUndoStep {
  private final String lockName;

  public UnlockResourceCheckLockNameStep(String lockName) {
    this.lockName = lockName;
  }

  @Override
  public StepResult doStep(FlightContext context) throws InterruptedException, RetryException {
    // Is the dataset actually locked by this flight?
    String exclusiveLock = "";
    // TODO - But, it could still be locked by a shared lock.
    try {
      exclusiveLock = getExclusiveLock();
    } catch (Exception ex) {
      return new StepResult(StepStatus.STEP_RESULT_FAILURE_FATAL, ex);
    }
    if (exclusiveLock == null || !exclusiveLock.equals(lockName)) {
      var message =
          exclusiveLock == null
              ? "Resource is not locked by an exclusive lock (But, if this is a dataset, it could be locked by a shared lock. Please contact the TDR team for help removing the shared lock.)"
              : "Resource is not locked by lock "
                  + lockName
                  + ". Resource is locked by "
                  + exclusiveLock
                  + ".";
      return new StepResult(
          StepStatus.STEP_RESULT_FAILURE_FATAL, new ResourceLockConflict(message));
    }
    return StepResult.getStepResultSuccess();
  }

  protected abstract String getExclusiveLock();
}
