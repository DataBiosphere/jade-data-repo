package bio.terra.datarepo.service.snapshot.flight.create;

import bio.terra.datarepo.service.iam.IamRole;
import bio.terra.datarepo.service.resourcemanagement.ResourceService;
import bio.terra.datarepo.service.snapshot.Snapshot;
import bio.terra.datarepo.service.snapshot.SnapshotService;
import bio.terra.datarepo.service.snapshot.flight.SnapshotWorkingMapKeys;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.FlightMap;
import bio.terra.stairway.Step;
import bio.terra.stairway.StepResult;
import java.util.Collections;
import java.util.Map;

public class SnapshotAuthzBqJobUserStep implements Step {
  private final SnapshotService snapshotService;
  private final ResourceService resourceService;
  private final String snapshotName;

  public SnapshotAuthzBqJobUserStep(
      SnapshotService snapshotService, ResourceService resourceService, String snapshotName) {
    this.snapshotService = snapshotService;
    this.resourceService = resourceService;
    this.snapshotName = snapshotName;
  }

  @Override
  public StepResult doStep(FlightContext context) throws InterruptedException {
    FlightMap workingMap = context.getWorkingMap();
    Map<IamRole, String> policyMap = workingMap.get(SnapshotWorkingMapKeys.POLICY_MAP, Map.class);

    Snapshot snapshot = snapshotService.retrieveByName(snapshotName);

    // Allow the steward and reader to make queries in this project.
    // The underlying service provides retries so we do not need to retry this operation
    resourceService.grantPoliciesBqJobUser(
        snapshot.getProjectResource().getGoogleProjectId(),
        Collections.singletonList(policyMap.get(IamRole.STEWARD)));
    resourceService.grantPoliciesBqJobUser(
        snapshot.getProjectResource().getGoogleProjectId(),
        Collections.singletonList(policyMap.get(IamRole.READER)));

    return StepResult.getStepResultSuccess();
  }

  @Override
  public StepResult undoStep(FlightContext context) throws InterruptedException {
    return StepResult.getStepResultSuccess();
  }
}
