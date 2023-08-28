package bio.terra.service.dataset.flight.upgrade.enableSecureMonitoring;

import bio.terra.common.iam.AuthenticatedUserRequest;
import bio.terra.service.dataset.DatasetDao;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.Step;
import bio.terra.stairway.StepResult;
import bio.terra.stairway.StepStatus;
import java.util.UUID;

public class EnableSecureMonitoringFlipFlagStep implements Step {
  private final UUID datasetId;
  private final DatasetDao datasetDao;
  private final AuthenticatedUserRequest userRequest;

  public EnableSecureMonitoringFlipFlagStep(
      UUID datasetId, DatasetDao datasetDao, AuthenticatedUserRequest userRequest) {
    this.datasetId = datasetId;
    this.datasetDao = datasetDao;
    this.userRequest = userRequest;
  }

  @Override
  public StepResult doStep(FlightContext context) throws InterruptedException {
    boolean patchSucceeded = datasetDao.setSecureMonitoring(datasetId, true, userRequest);
    if (!patchSucceeded) {
      return new StepResult(
          StepStatus.STEP_RESULT_FAILURE_FATAL,
          new Exception("Unable to update secure monitoring flag"));
    }
    return StepResult.getStepResultSuccess();
  }

  /**
   * Undo the flag until we have a successful run of entire flight
   *
   * @param context
   * @return
   * @throws InterruptedException
   */
  @Override
  public StepResult undoStep(FlightContext context) throws InterruptedException {
    boolean patchSucceeded = datasetDao.setSecureMonitoring(datasetId, false, userRequest);
    if (!patchSucceeded) {
      return new StepResult(
          StepStatus.STEP_RESULT_FAILURE_FATAL,
          new Exception("Unable to update secure monitoring flag"));
    }
    return StepResult.getStepResultSuccess();
  }
}
