package bio.terra.service.dataset.flight.inheritsteward;

import bio.terra.service.dataset.flight.DatasetWorkingMapKeys;
import bio.terra.service.resourcemanagement.ResourceService;
import bio.terra.service.snapshot.SnapshotService;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.FlightMap;
import bio.terra.stairway.Step;
import bio.terra.stairway.StepResult;
import bio.terra.stairway.exception.RetryException;
import java.util.List;
import java.util.Objects;
import java.util.UUID;

public record SetAuthGcpUserRolesStep(
    ResourceService resourceService,
    SnapshotService snapshotService,
    String custodianEmail,
    boolean inheritSteward)
    implements Step {

  private StepResult setAuth(FlightContext flightContext, boolean inheritSteward)
      throws InterruptedException {
    FlightMap workingMap = flightContext.getWorkingMap();
    List<UUID> snapshotIds = workingMap.get(DatasetWorkingMapKeys.SNAPSHOT_IDS, List.class);
    for (var snapshotId : Objects.requireNonNull(snapshotIds)) {
      String projectId =
          snapshotService.retrieve(snapshotId).getProjectResource().getGoogleProjectId();
      if (inheritSteward) {
        resourceService.assignRolesForSnapshot(projectId, List.of(custodianEmail));
      } else {
        resourceService.revokeRolesForSnapshot(projectId, List.of(custodianEmail));
      }
    }
    return StepResult.getStepResultSuccess();
  }

  @Override
  public StepResult doStep(FlightContext flightContext)
      throws InterruptedException, RetryException {
    return setAuth(flightContext, inheritSteward);
  }

  @Override
  public StepResult undoStep(FlightContext flightContext) throws InterruptedException {
    return setAuth(flightContext, !inheritSteward);
  }
}
