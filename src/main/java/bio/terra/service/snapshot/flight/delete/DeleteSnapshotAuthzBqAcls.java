package bio.terra.service.snapshot.flight.delete;

import bio.terra.service.iam.AuthenticatedUserRequest;
import bio.terra.service.iam.IamResourceType;
import bio.terra.service.iam.IamRole;
import bio.terra.service.iam.IamService;
import bio.terra.service.resourcemanagement.DataLocationService;
import bio.terra.service.resourcemanagement.google.GoogleResourceService;
import bio.terra.service.snapshot.Snapshot;
import bio.terra.service.snapshot.SnapshotDataProject;
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

public class DeleteSnapshotAuthzBqAcls implements Step {
    private final IamService sam;
    private final GoogleResourceService resourceService;
    private final DataLocationService dataLocationService;
    private final SnapshotService snapshotService;
    private final UUID snapshotId;
    private final AuthenticatedUserRequest userReq;

    private static Logger logger = LoggerFactory.getLogger(DeleteSnapshotAuthzBqAcls.class);

    public DeleteSnapshotAuthzBqAcls(IamService sam,
                                     GoogleResourceService resourceService,
                                     DataLocationService dataLocationService,
                                     SnapshotService snapshotService,
                                     UUID snapshotId,
                                     AuthenticatedUserRequest userReq) {
        this.sam = sam;
        this.resourceService = resourceService;
        this.dataLocationService = dataLocationService;
        this.snapshotService = snapshotService;
        this.snapshotId = snapshotId;
        this.userReq = userReq;
    }

    @Override
    public StepResult doStep(FlightContext context) throws InterruptedException {
        Snapshot snapshot = snapshotService.retrieve(snapshotId);

        SnapshotDataProject projectForSnapshot = dataLocationService.getProject(snapshot)
            .orElseThrow(() -> new SnapshotNotFoundException("Snapshot was not found"));

        Map<IamRole, String> policyEmails = sam.retrievePolicyEmails(userReq, IamResourceType.DATASNAPSHOT, snapshotId);

        // Remove the custodian's access to make queries in this project.
        // The underlying service provides retries so we do not need to retry this operation
        resourceService.revokePoliciesBqJobUser(
            projectForSnapshot.getGoogleProjectId(),
            Collections.singletonList(policyEmails.get(IamRole.CUSTODIAN)));

        return StepResult.getStepResultSuccess();
    }

    @Override
    public StepResult undoStep(FlightContext context) {
        // can't undo delete
        logger.warn("Trying to undo clear ACLs for snapshot " + snapshotId.toString());
        return StepResult.getStepResultSuccess();
    }

}
