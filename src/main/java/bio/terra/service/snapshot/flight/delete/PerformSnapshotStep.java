package bio.terra.service.snapshot.flight.delete;

import bio.terra.service.job.OptionalStep;
import bio.terra.service.snapshot.flight.SnapshotWorkingMapKeys;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.Step;

public record PerformSnapshotStep(Step step) implements OptionalStep {
  @Override
  public boolean isEnabled(FlightContext context) {
    return context.getWorkingMap().get(SnapshotWorkingMapKeys.SNAPSHOT_EXISTS, boolean.class);
  }
}
