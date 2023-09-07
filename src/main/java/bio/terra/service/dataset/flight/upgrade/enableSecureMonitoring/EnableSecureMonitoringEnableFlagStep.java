package bio.terra.service.dataset.flight.upgrade.enableSecureMonitoring;

import bio.terra.common.iam.AuthenticatedUserRequest;
import bio.terra.service.dataset.DatasetDao;
import bio.terra.service.dataset.flight.DatasetWorkingMapKeys;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.FlightMap;
import bio.terra.stairway.Step;
import bio.terra.stairway.StepResult;
import bio.terra.stairway.StepStatus;
import java.util.UUID;

public class EnableSecureMonitoringEnableFlagStep implements Step {
  private final DatasetDao datasetDao;
  private final AuthenticatedUserRequest userRequest;

  public EnableSecureMonitoringEnableFlagStep(
      DatasetDao datasetDao, AuthenticatedUserRequest userRequest) {
    this.datasetDao = datasetDao;
    this.userRequest = userRequest;
  }

  @Override
  public StepResult doStep(FlightContext context) throws InterruptedException {
    FlightMap workingMap = context.getWorkingMap();
    UUID datasetId = workingMap.get(DatasetWorkingMapKeys.DATASET_ID, UUID.class);
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
    FlightMap workingMap = context.getWorkingMap();
    UUID datasetId = workingMap.get(DatasetWorkingMapKeys.DATASET_ID, UUID.class);
    boolean originalFlagValue =
        workingMap.get(DatasetWorkingMapKeys.SECURE_MONITORING_ENABLED, Boolean.class);
    boolean patchSucceeded =
        datasetDao.setSecureMonitoring(datasetId, originalFlagValue, userRequest);
    if (!patchSucceeded) {
      return new StepResult(
          StepStatus.STEP_RESULT_FAILURE_FATAL,
          new Exception("Unable to update secure monitoring flag"));
    }
    return StepResult.getStepResultSuccess();
  }
}