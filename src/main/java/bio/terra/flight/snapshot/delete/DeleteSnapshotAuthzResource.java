package bio.terra.flight.snapshot.delete;

import bio.terra.controller.AuthenticatedUser;
import bio.terra.exception.InternalServerErrorException;
import bio.terra.flight.dataset.create.CreateDatasetAuthzResource;
import bio.terra.service.JobMapKeys;
import bio.terra.service.SamClientService;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.FlightMap;
import bio.terra.stairway.Step;
import bio.terra.stairway.StepResult;
import org.broadinstitute.dsde.workbench.client.sam.ApiException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.UUID;

public class DeleteSnapshotAuthzResource implements Step {
    private SamClientService sam;
    private UUID snapshotId;
    private static Logger logger = LoggerFactory.getLogger(CreateDatasetAuthzResource.class);

    public DeleteSnapshotAuthzResource(SamClientService sam, UUID snapshotId) {
        this.sam = sam;
        this.snapshotId = snapshotId;
    }

    @Override
    public StepResult doStep(FlightContext context) {
        FlightMap inputParameters = context.getInputParameters();
        AuthenticatedUser userReq = inputParameters.get(
            JobMapKeys.USER_INFO.getKeyName(), AuthenticatedUser.class);
        try {
            sam.deleteSnapshotResource(userReq, snapshotId);
        } catch (ApiException ex) {
            // If we can't find it consider the delete successful.
            if (ex.getCode() != 404) {
                throw new InternalServerErrorException(ex);
            }
        }
        return StepResult.getStepResultSuccess();
    }

    @Override
    public StepResult undoStep(FlightContext context) {
        // can't undo delete
        logger.warn("Trying to undo delete resource for snapshot " + snapshotId.toString());
        return StepResult.getStepResultSuccess();
    }
}
