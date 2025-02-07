package bio.terra.service.snapshot.flight.delete;

import bio.terra.common.iam.AuthenticatedUserRequest;
import bio.terra.service.auth.iam.IamResourceType;
import bio.terra.service.auth.iam.IamRole;
import bio.terra.service.auth.iam.IamService;
import bio.terra.service.resourcemanagement.ResourceService;
import bio.terra.service.snapshot.Snapshot;
import bio.terra.service.snapshot.SnapshotService;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.Step;
import bio.terra.stairway.StepResult;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DeleteSnapshotAuthzBqAclsStep implements Step {
  private final IamService sam;
  private final ResourceService resourceService;
  private final SnapshotService snapshotService;
  private final UUID snapshotId;
  private final AuthenticatedUserRequest userReq;

  private static final Logger logger = LoggerFactory.getLogger(DeleteSnapshotAuthzBqAclsStep.class);

  public DeleteSnapshotAuthzBqAclsStep(
      IamService sam,
      ResourceService resourceService,
      SnapshotService snapshotService,
      UUID snapshotId,
      AuthenticatedUserRequest userReq) {
    this.sam = sam;
    this.resourceService = resourceService;
    this.snapshotService = snapshotService;
    this.snapshotId = snapshotId;
    this.userReq = userReq;
  }

  @Override
  public StepResult doStep(FlightContext context) throws InterruptedException {
    Snapshot snapshot = snapshotService.retrieve(snapshotId);

    // These policy emails should not change since the snapshot is locked by the flight
    Map<IamRole, String> policyEmails =
        sam.retrievePolicyEmails(userReq, IamResourceType.DATASNAPSHOT, snapshotId);

    // If the dataset custodian inherited permissions, remove them now.
    var datasetPolicyEmails =
        sam.retrievePolicyEmails(
            userReq, IamResourceType.DATASET, snapshot.getSourceDataset().getId());

    // Remove access added by SnapshotAuthzBqJobUserStep.
    // The underlying service provides retries so we do not need to retry this operation
    resourceService.revokePoliciesBqJobUser(
        snapshot.getProjectResource().getGoogleProjectId(),
        List.of(
            policyEmails.get(IamRole.STEWARD),
            policyEmails.get(IamRole.READER),
            datasetPolicyEmails.get(IamRole.CUSTODIAN)));

    return StepResult.getStepResultSuccess();
  }

  @Override
  public StepResult undoStep(FlightContext context) {
    // can't undo delete
    logger.warn("Trying to undo clear ACLs for snapshot {}", snapshotId);
    return StepResult.getStepResultSuccess();
  }
}
