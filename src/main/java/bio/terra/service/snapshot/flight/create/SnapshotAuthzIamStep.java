package bio.terra.service.snapshot.flight.create;

import bio.terra.common.exception.UnauthorizedException;
import bio.terra.model.SnapshotRequestModel;
import bio.terra.service.iam.AuthenticatedUserRequest;
import bio.terra.service.iam.IamService;
import bio.terra.service.snapshot.SnapshotService;
import bio.terra.service.snapshot.flight.SnapshotWorkingMapKeys;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.FlightMap;
import bio.terra.stairway.Step;
import bio.terra.stairway.StepResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.UUID;

public class SnapshotAuthzIamStep implements Step {
    private final IamService sam;
    private final SnapshotService snapshotService;
    private final SnapshotRequestModel snapshotRequestModel;
    private final AuthenticatedUserRequest userReq;
    private static final Logger logger = LoggerFactory.getLogger(SnapshotAuthzIamStep.class);

    public SnapshotAuthzIamStep(IamService sam,
                                SnapshotService snapshotService,
                                SnapshotRequestModel snapshotRequestModel,
                                AuthenticatedUserRequest userReq) {
        this.sam = sam;
        this.snapshotService = snapshotService;
        this.snapshotRequestModel = snapshotRequestModel;
        this.userReq = userReq;
    }

    @Override
    public StepResult doStep(FlightContext context) throws InterruptedException {
        FlightMap workingMap = context.getWorkingMap();
        UUID snapshotId = workingMap.get(SnapshotWorkingMapKeys.SNAPSHOT_ID, UUID.class);

        // This returns the policy email created by Google to correspond to the readers list in SAM
        String readersPolicyEmail = sam.createSnapshotResource(userReq, snapshotId, snapshotRequestModel.getReaders());
        workingMap.put(SnapshotWorkingMapKeys.POLICY_EMAIL, readersPolicyEmail);
        return StepResult.getStepResultSuccess();
    }

    @Override
    public StepResult undoStep(FlightContext context) {
        FlightMap workingMap = context.getWorkingMap();
        UUID snapshotId = workingMap.get(SnapshotWorkingMapKeys.SNAPSHOT_ID, UUID.class);
        try {
            sam.deleteSnapshotResource(userReq, snapshotId);
            // We do not need to remove the ACL from the files or BigQuery. It disappears
            // when SAM deletes the ACL. How 'bout that!
        } catch (UnauthorizedException ex) {
            // suppress exception
            logger.error("NEEDS CLEANUP: delete sam resource for snapshot " + snapshotId.toString());
            logger.warn(ex.getMessage());
        }
        return StepResult.getStepResultSuccess();
    }
}
