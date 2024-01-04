package bio.terra.service.dataset.flight.upgrade.enableSecureMonitoring;

import bio.terra.service.dataset.Dataset;
import bio.terra.service.dataset.flight.DatasetWorkingMapKeys;
import bio.terra.service.job.DefaultUndoStep;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.FlightMap;
import bio.terra.stairway.StepResult;

public class SecureMonitoringRecordInFlightMapStep extends DefaultUndoStep {
  private Dataset dataset;

  public SecureMonitoringRecordInFlightMapStep(Dataset dataset) {
    this.dataset = dataset;
  }

  @Override
  public StepResult doStep(FlightContext context) throws InterruptedException {
    FlightMap workingMap = context.getWorkingMap();
    workingMap.put(DatasetWorkingMapKeys.DATASET_ID, dataset.getId());
    workingMap.put(
        DatasetWorkingMapKeys.SECURE_MONITORING_ENABLED, dataset.isSecureMonitoringEnabled());
    return StepResult.getStepResultSuccess();
  }
}
