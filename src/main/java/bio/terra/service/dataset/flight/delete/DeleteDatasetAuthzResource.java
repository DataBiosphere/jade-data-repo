package bio.terra.service.dataset.flight.delete;

import bio.terra.service.iam.AuthenticatedUserRequest;
import bio.terra.common.exception.InternalServerErrorException;
import bio.terra.service.dataset.flight.create.CreateDatasetAuthzResource;
import bio.terra.service.iam.SamClientService;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.Step;
import bio.terra.stairway.StepResult;
import org.broadinstitute.dsde.workbench.client.sam.ApiException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.UUID;

public class DeleteDatasetAuthzResource implements Step {
    private SamClientService sam;
    private UUID datasetId;
    private AuthenticatedUserRequest userReq;

    public DeleteDatasetAuthzResource(SamClientService sam, UUID datasetId, AuthenticatedUserRequest userReq) {
        this.sam = sam;
        this.datasetId = datasetId;
        this.userReq = userReq;
    }

    private static Logger logger = LoggerFactory.getLogger(CreateDatasetAuthzResource.class);

    @Override
    public StepResult doStep(FlightContext context) {
        try {
            sam.deleteDatasetResource(userReq, datasetId);
        } catch (ApiException ex) {
            throw new InternalServerErrorException(ex);
        }
        return StepResult.getStepResultSuccess();
    }

    @Override
    public StepResult undoStep(FlightContext context) {
        // can't undo delete
        logger.warn("Trying to undo delete resource for dataset " + datasetId.toString());
        return StepResult.getStepResultSuccess();
    }
}
