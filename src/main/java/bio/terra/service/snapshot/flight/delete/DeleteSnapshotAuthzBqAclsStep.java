package bio.terra.service.snapshot.flight.delete;

import bio.terra.service.iam.AuthenticatedUserRequest;
import bio.terra.service.iam.IamResourceType;
import bio.terra.service.iam.IamRole;
import bio.terra.service.iam.IamService;
import bio.terra.service.resourcemanagement.ResourceService;
import bio.terra.service.snapshot.Snapshot;
import bio.terra.service.snapshot.SnapshotService;
import bio.terra.service.snapshot.exception.SnapshotNotFoundException;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.Step;
import bio.terra.stairway.StepResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.Map;
import java.util.UUID;

public class DeleteSnapshotAuthzBqAclsStep implements Step {
    private final IamService sam;
    private final ResourceService resourceService;
    private final SnapshotService snapshotService;
    private final UUID snapshotId;
    private final AuthenticatedUserRequest userReq;

    private static final Logger logger = LoggerFactory.getLogger(DeleteSnapshotAuthzBqAclsStep.class);

    public DeleteSnapshotAuthzBqAclsStep(IamService sam,
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
        //TODO: this probably should fail with a 404 before the flight is even attempted
        final Snapshot snapshot;
        try {
            snapshot = snapshotService.retrieve(snapshotId);
        } catch (SnapshotNotFoundException e) {
            logger.warn("Snapshot {} metadata was not found.  Ignoring explicit ACL clear.", snapshotId);
            return StepResult.getStepResultSuccess();
        }

        // These policy emails should not change since the snapshot is locked by the flight
        Map<IamRole, String> policyEmails = sam.retrievePolicyEmails(userReq, IamResourceType.DATASNAPSHOT, snapshotId);

        // Remove the custodian's access to make queries in this project.
        // The underlying service provides retries so we do not need to retry this operation
        resourceService.revokePoliciesBqJobUser(
            snapshot.getProjectResource().getGoogleProjectId(),
            Collections.singletonList(policyEmails.get(IamRole.CUSTODIAN)));

        return StepResult.getStepResultSuccess();
    }

    @Override
    public StepResult undoStep(FlightContext context) {
        // can't undo delete
        logger.warn("Trying to undo clear ACLs for snapshot {}", snapshotId);
        return StepResult.getStepResultSuccess();
    }

}
