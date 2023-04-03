package bio.terra.service.snapshot.flight.duos;

import bio.terra.service.job.OptionalStep;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.FlightMap;
import bio.terra.stairway.Step;

public record IfNoGroupRetrievedStep(Step step) implements OptionalStep {

  @Override
  public boolean isEnabled(FlightContext context) {
    FlightMap workingMap = context.getWorkingMap();
    return !workingMap.get(SnapshotDuosMapKeys.FIRECLOUD_GROUP_RETRIEVED, boolean.class);
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
