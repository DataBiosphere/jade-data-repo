package bio.terra.service.snapshot.flight.delete;

import bio.terra.service.job.OptionalStep;
import bio.terra.service.snapshot.flight.SnapshotWorkingMapKeys;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.Step;

public class PerformDatasetStep extends OptionalStep {
  public PerformDatasetStep(Step step) {
    super(step);
  }

  @Override
  public boolean isEnabled(FlightContext context) {
    return context.getWorkingMap().get(SnapshotWorkingMapKeys.DATASET_EXISTS, boolean.class);
  }
}
