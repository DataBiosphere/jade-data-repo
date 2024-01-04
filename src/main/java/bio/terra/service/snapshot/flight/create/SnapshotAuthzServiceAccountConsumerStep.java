package bio.terra.service.snapshot.flight.create;

import bio.terra.service.auth.iam.IamRole;
import bio.terra.service.resourcemanagement.ResourceService;
import bio.terra.service.snapshot.Snapshot;
import bio.terra.service.snapshot.SnapshotService;
import bio.terra.service.snapshot.flight.SnapshotWorkingMapKeys;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.FlightMap;
import bio.terra.stairway.Step;
import bio.terra.stairway.StepResult;
import com.fasterxml.jackson.core.type.TypeReference;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class SnapshotAuthzServiceAccountConsumerStep implements Step {
  private final SnapshotService snapshotService;
  private final ResourceService resourceService;
  private final String snapshotName;
  private final String tdrServiceAccountEmail;

  public SnapshotAuthzServiceAccountConsumerStep(
      SnapshotService snapshotService,
      ResourceService resourceService,
      String snapshotName,
      String tdrServiceAccountEmail) {
    this.snapshotService = snapshotService;
    this.resourceService = resourceService;
    this.snapshotName = snapshotName;
    this.tdrServiceAccountEmail = tdrServiceAccountEmail;
  }

  @Override
  public StepResult doStep(FlightContext context) throws InterruptedException {
    FlightMap workingMap = context.getWorkingMap();
    Map<IamRole, String> policyMap =
        workingMap.get(SnapshotWorkingMapKeys.POLICY_MAP, new TypeReference<>() {});

    Snapshot snapshot = snapshotService.retrieveByName(snapshotName);

    // Allow the steward and reader to bill this project to read requester pays bucket data.
    // The underlying service provides retries so we do not need to retry this operation
    // Also grant the dataset's service account the permission to also access source buckets
    List<String> principalsToAdd =
        new ArrayList<>(List.of(policyMap.get(IamRole.STEWARD), policyMap.get(IamRole.READER)));
    if (!snapshot
        .getSourceDataset()
        .getProjectResource()
        .getServiceAccount()
        .equals(tdrServiceAccountEmail)) {
      principalsToAdd.add(snapshot.getSourceDataset().getProjectResource().getServiceAccount());
    }
    resourceService.grantPoliciesServiceUsageConsumer(
        snapshot.getProjectResource().getGoogleProjectId(), principalsToAdd);

    return StepResult.getStepResultSuccess();
  }

  @Override
  public StepResult undoStep(FlightContext context) throws InterruptedException {
    return StepResult.getStepResultSuccess();
  }
}
