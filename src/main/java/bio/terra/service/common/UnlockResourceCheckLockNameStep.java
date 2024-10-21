package bio.terra.service.common;

import bio.terra.service.auth.iam.IamResourceType;
import bio.terra.service.dataset.flight.DatasetWorkingMapKeys;
import bio.terra.service.job.DefaultUndoStep;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.FlightMap;
import bio.terra.stairway.StepResult;
import bio.terra.stairway.StepStatus;
import bio.terra.stairway.exception.RetryException;
import java.util.List;
import java.util.UUID;
import org.apache.commons.lang3.StringUtils;

public abstract class UnlockResourceCheckLockNameStep extends DefaultUndoStep {
  private final IamResourceType iamResourceType;
  protected final UUID resourceId;
  private final String lockName;

  protected UnlockResourceCheckLockNameStep(
      IamResourceType iamResourceType, UUID resourceId, String lockName) {
    this.iamResourceType = iamResourceType;
    this.resourceId = resourceId;
    this.lockName = lockName;
  }

  private String resourceToString() {
    return StringUtils.capitalize(iamResourceType.getSamResourceName()) + " " + resourceId;
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
      String message = "%s is not locked.".formatted(resourceToString());
      return new StepResult(
          StepStatus.STEP_RESULT_FAILURE_FATAL, new ResourceLockConflict(message));
    }
    if (!locks.contains(lockName)) {
      String message =
          """
            %s has no lock named '%s'.
            Do you mean to remove one of these existing locks instead?
            """
              .formatted(resourceToString(), lockName);
      return new StepResult(
          StepStatus.STEP_RESULT_FAILURE_FATAL, new ResourceLockConflict(message, locks));
    }
    workingMap.put(DatasetWorkingMapKeys.IS_SHARED_LOCK, isSharedLock(lockName));
    return StepResult.getStepResultSuccess();
  }

  protected abstract List<String> getLocks();

  protected abstract boolean isSharedLock(String lockName);
}
