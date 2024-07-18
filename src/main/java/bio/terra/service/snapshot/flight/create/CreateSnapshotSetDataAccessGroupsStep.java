package bio.terra.service.snapshot.flight.create;

import bio.terra.service.job.DefaultUndoStep;
import bio.terra.service.snapshot.flight.SnapshotWorkingMapKeys;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.StepResult;
import bio.terra.stairway.exception.RetryException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;

public class CreateSnapshotSetDataAccessGroupsStep extends DefaultUndoStep {
  List<String> dataAccessControlGroups;

  public CreateSnapshotSetDataAccessGroupsStep(List<String> dataAccessControlGroups) {
    this.dataAccessControlGroups = dataAccessControlGroups;
  }

  @Override
  public StepResult doStep(FlightContext flightContext)
      throws InterruptedException, RetryException {
    var workingMap = flightContext.getWorkingMap();
    List<String> finalGroups = new ArrayList<>();
    if (Objects.nonNull(dataAccessControlGroups)) {
      finalGroups.addAll(dataAccessControlGroups);
    }
    finalGroups.add(
        workingMap.get(SnapshotWorkingMapKeys.SNAPSHOT_FIRECLOUD_GROUP_NAME, String.class));
    List<String> uniqueUserGroups =
        new ArrayList<>(new HashSet<>(finalGroups).stream().filter(Objects::nonNull).toList());
    workingMap.put(SnapshotWorkingMapKeys.SNAPSHOT_DATA_ACCESS_CONTROL_GROUPS, uniqueUserGroups);
    return StepResult.getStepResultSuccess();
  }
}
