package bio.terra.service.snapshot.flight.duos;

import bio.terra.service.job.OptionalStep;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.Step;

public class IfGroupDoesNotExistStep extends OptionalStep {
  public IfGroupDoesNotExistStep(Step step) {
    super(step);
  }

  @Override
  public boolean isEnabled(FlightContext context) {
    return !context.getWorkingMap().get(SnapshotDuosMapKeys.FIRECLOUD_GROUP_EXISTS, boolean.class);
  }

  @Override
  public String getSkipReason() {
    return "a Firecloud group already exists for the DUOS ID";
  }

  @Override
  public String getRunReason(FlightContext context) {
    return "a Firecloud group does not yet exist for the DUOS ID";
  }
}
