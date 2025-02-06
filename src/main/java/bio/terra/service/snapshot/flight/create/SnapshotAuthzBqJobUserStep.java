package bio.terra.service.snapshot.flight.create;

import bio.terra.common.iam.AuthenticatedUserRequest;
import bio.terra.service.auth.iam.IamResourceType;
import bio.terra.service.auth.iam.IamRole;
import bio.terra.service.auth.iam.IamService;
import bio.terra.service.resourcemanagement.ResourceService;
import bio.terra.service.snapshot.Snapshot;
import bio.terra.service.snapshot.SnapshotService;
import bio.terra.service.snapshot.flight.SnapshotWorkingMapKeys;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.FlightMap;
import bio.terra.stairway.Step;
import bio.terra.stairway.StepResult;
import com.fasterxml.jackson.core.type.TypeReference;
import java.util.List;
import java.util.Map;
import java.util.UUID;

public class SnapshotAuthzBqJobUserStep implements Step {
  private final SnapshotService snapshotService;
  private final ResourceService resourceService;
  private final IamService sam;
  private final AuthenticatedUserRequest request;
  private final String snapshotName;

  public SnapshotAuthzBqJobUserStep(
      SnapshotService snapshotService,
      ResourceService resourceService,
      IamService sam,
      AuthenticatedUserRequest request,
      String snapshotName) {
    this.snapshotService = snapshotService;
    this.resourceService = resourceService;
    this.sam = sam;
    this.request = request;
    this.snapshotName = snapshotName;
  }

  @Override
  public StepResult doStep(FlightContext context) throws InterruptedException {
    FlightMap workingMap = context.getWorkingMap();
    Map<IamRole, String> policyMap =
        workingMap.get(SnapshotWorkingMapKeys.POLICY_MAP, new TypeReference<>() {});

    Snapshot snapshot = snapshotService.retrieveByName(snapshotName);

    // Allow the steward and reader to make queries in this project.
    // The underlying service provides retries so we do not need to retry this operation
    resourceService.grantPoliciesBqJobUser(
        snapshot.getProjectResource().getGoogleProjectId(),
        List.of(policyMap.get(IamRole.STEWARD)));
    resourceService.grantPoliciesBqJobUser(
        snapshot.getProjectResource().getGoogleProjectId(), List.of(policyMap.get(IamRole.READER)));

    UUID parentDatasetId =
        workingMap.get(SnapshotWorkingMapKeys.SNAPSHOT_PARENT_DATASET_ID, UUID.class);
    if (parentDatasetId != null) {
      var datasetPolicyMap =
          sam.retrievePolicyEmails(request, IamResourceType.DATASET, parentDatasetId);
      // Allow the custodian to make queries in this project.
      // FIXME: Is this necessary? The dataset custodian should already BQ job access to the
      // snapshot's project.
      resourceService.grantPoliciesBqJobUser(
          snapshot.getProjectResource().getGoogleProjectId(),
          List.of(datasetPolicyMap.get(IamRole.CUSTODIAN)));
    }

    return StepResult.getStepResultSuccess();
  }

  @Override
  public StepResult undoStep(FlightContext context) throws InterruptedException {
    return StepResult.getStepResultSuccess();
  }
}
