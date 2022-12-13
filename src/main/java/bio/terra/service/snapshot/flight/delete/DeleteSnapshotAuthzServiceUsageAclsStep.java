package bio.terra.service.snapshot.flight.delete;

import bio.terra.common.iam.AuthenticatedUserRequest;
import bio.terra.service.auth.iam.IamResourceType;
import bio.terra.service.auth.iam.IamRole;
import bio.terra.service.auth.iam.IamService;
import bio.terra.service.job.DefaultUndoStep;
import bio.terra.service.resourcemanagement.ResourceService;
import bio.terra.service.snapshot.Snapshot;
import bio.terra.service.snapshot.SnapshotService;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.StepResult;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DeleteSnapshotAuthzServiceUsageAclsStep extends DefaultUndoStep {
  private final IamService sam;
  private final ResourceService resourceService;
  private final SnapshotService snapshotService;
  private final UUID snapshotId;
  private final AuthenticatedUserRequest userReq;
  private final String tdrServiceAccountEmail;

  private static final Logger logger =
      LoggerFactory.getLogger(DeleteSnapshotAuthzServiceUsageAclsStep.class);

  public DeleteSnapshotAuthzServiceUsageAclsStep(
      IamService sam,
      ResourceService resourceService,
      SnapshotService snapshotService,
      UUID snapshotId,
      AuthenticatedUserRequest userReq,
      String tdrServiceAccountEmail) {
    this.sam = sam;
    this.resourceService = resourceService;
    this.snapshotService = snapshotService;
    this.snapshotId = snapshotId;
    this.userReq = userReq;
    this.tdrServiceAccountEmail = tdrServiceAccountEmail;
  }

  @Override
  public StepResult doStep(FlightContext context) throws InterruptedException {
    Snapshot snapshot = snapshotService.retrieve(snapshotId);

    // These policy emails should not change since the snapshot is locked by the flight
    Map<IamRole, String> policyEmails =
        sam.retrievePolicyEmails(userReq, IamResourceType.DATASNAPSHOT, snapshotId);

    // Remove the custodian's access to bill this project to read requester pays bucket data.
    // The underlying service provides retries so we do not need to retry this operation
    List<String> principalsToRemove = new ArrayList<>();
    if (policyEmails.get(IamRole.STEWARD) != null) {
      principalsToRemove.add(policyEmails.get(IamRole.STEWARD));
    }
    if (policyEmails.get(IamRole.READER) != null) {
      principalsToRemove.add(policyEmails.get(IamRole.READER));
    }
    if (!snapshot
        .getSourceDataset()
        .getProjectResource()
        .getServiceAccount()
        .equals(tdrServiceAccountEmail)) {
      principalsToRemove.add(snapshot.getSourceDataset().getProjectResource().getServiceAccount());
    }
    resourceService.revokePoliciesServiceUsageConsumer(
        snapshot.getProjectResource().getGoogleProjectId(), principalsToRemove);

    return StepResult.getStepResultSuccess();
  }
}
