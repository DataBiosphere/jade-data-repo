package bio.terra.service.snapshot.flight.create;

import bio.terra.service.job.OptionalStep;
import bio.terra.service.snapshot.flight.SnapshotWorkingMapKeys;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.Step;
import com.fasterxml.jackson.core.type.TypeReference;
import java.util.List;

public class IfDataAccessControlGroupStep extends OptionalStep {

  public IfDataAccessControlGroupStep(Step step) {
    super(step);
  }

  @Override
  public boolean isEnabled(FlightContext context) {
    List<String> groups =
        context
            .getWorkingMap()
            .get(
                SnapshotWorkingMapKeys.SNAPSHOT_DATA_ACCESS_CONTROL_GROUPS,
                new TypeReference<>() {});
    return groups != null && !groups.isEmpty();
  }

  @Override
  public String getSkipReason() {
    return "there are no data access groups to set.";
  }

  @Override
  public String getRunReason(FlightContext context) {
    return "a data access group exists and needs to be set on the snapshot.";
  }
}
