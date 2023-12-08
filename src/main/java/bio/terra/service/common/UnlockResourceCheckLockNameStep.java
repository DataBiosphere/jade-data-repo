package bio.terra.service.common;

import bio.terra.service.dataset.flight.DatasetWorkingMapKeys;
import bio.terra.service.job.DefaultUndoStep;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.FlightMap;
import bio.terra.stairway.StepResult;
import bio.terra.stairway.StepStatus;
import bio.terra.stairway.exception.RetryException;
import java.util.List;

public abstract class UnlockResourceCheckLockNameStep extends DefaultUndoStep {
  private final String lockName;

  public UnlockResourceCheckLockNameStep(String lockName) {
    this.lockName = lockName;
  }

  @Override
  public StepResult doStep(FlightContext context) throws InterruptedException, RetryException {
    FlightMap workingMap = context.getWorkingMap();
    List<String> locks;
    try {
      locks = getLocks();
    } catch (Exception ex) {
      return new StepResult(StepStatus.STEP_RESULT_FAILURE_FATAL, ex);
    }
    if (locks.isEmpty()) {
      return new StepResult(
          StepStatus.STEP_RESULT_FAILURE_FATAL,
          new ResourceLockConflict("Resource is not locked."));
    }
    if (!locks.contains(lockName)) {
      return new StepResult(
          StepStatus.STEP_RESULT_FAILURE_FATAL,
          new ResourceLockConflict(
              "Resource not locked by "
                  + lockName
                  + ". It is locked by flight(s) "
                  + String.join(", ", locks)
                  + "."));
    }
    workingMap.put(DatasetWorkingMapKeys.IS_SHARED_LOCK, isSharedLock(lockName));
    return StepResult.getStepResultSuccess();
  }

  protected abstract List<String> getLocks();

  protected abstract boolean isSharedLock(String lockName);
}
