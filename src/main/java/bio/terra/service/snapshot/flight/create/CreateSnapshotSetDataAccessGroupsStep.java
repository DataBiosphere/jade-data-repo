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
    // Confirm we one of 3 valid states:
    // (1) There are no groups set
    // (2) There is a group created for snapshot byRequestId
    // (3) There is a group set on the request
    boolean hasGroupSetOnRequest =
        Objects.nonNull(dataAccessControlGroups) && !dataAccessControlGroups.isEmpty();
    String snapshotCreatedGroupName =
        workingMap.get(SnapshotWorkingMapKeys.SNAPSHOT_FIRECLOUD_GROUP_NAME, String.class);
    boolean hasSnapshotCreatedGroup = Objects.nonNull(snapshotCreatedGroupName);
    if (hasGroupSetOnRequest && hasSnapshotCreatedGroup) {
      throw new IllegalArgumentException(
          "Both a data access group was set on the request and a group was created.");
    }
    // Set the group list on the flight map
    List<String> finalGroups = new ArrayList<>();
    if (hasSnapshotCreatedGroup) {
      finalGroups.add(snapshotCreatedGroupName);
    } else if (hasGroupSetOnRequest) {
      List<String> uniqueUserGroups = new HashSet<>(dataAccessControlGroups).stream().toList();
      finalGroups.addAll(uniqueUserGroups);
    }
    workingMap.put(SnapshotWorkingMapKeys.SNAPSHOT_DATA_ACCESS_CONTROL_GROUPS, finalGroups);
    return StepResult.getStepResultSuccess();
  }
}
