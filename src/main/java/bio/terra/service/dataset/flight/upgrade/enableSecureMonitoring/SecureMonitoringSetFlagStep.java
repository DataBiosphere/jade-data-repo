package bio.terra.service.dataset.flight.upgrade.enableSecureMonitoring;

import bio.terra.common.iam.AuthenticatedUserRequest;
import bio.terra.service.dataset.DatasetDao;
import bio.terra.service.dataset.flight.DatasetWorkingMapKeys;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.FlightMap;
import bio.terra.stairway.Step;
import bio.terra.stairway.StepResult;
import java.util.UUID;

public class SecureMonitoringSetFlagStep implements Step {
  private final DatasetDao datasetDao;
  private final AuthenticatedUserRequest userRequest;

  private final boolean enableSecureMonitoring;

  public SecureMonitoringSetFlagStep(
      DatasetDao datasetDao, AuthenticatedUserRequest userRequest, boolean enableSecureMonitoring) {
    this.datasetDao = datasetDao;
    this.userRequest = userRequest;
    this.enableSecureMonitoring = enableSecureMonitoring;
  }

  @Override
  public StepResult doStep(FlightContext context) throws InterruptedException {
    FlightMap workingMap = context.getWorkingMap();
    UUID datasetId = workingMap.get(DatasetWorkingMapKeys.DATASET_ID, UUID.class);
    datasetDao.setSecureMonitoring(datasetId, enableSecureMonitoring, userRequest);
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
    datasetDao.setSecureMonitoring(datasetId, originalFlagValue, userRequest);
    return StepResult.getStepResultSuccess();
  }
}
