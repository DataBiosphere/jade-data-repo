package bio.terra.flight.dataset.delete;

import bio.terra.controller.AuthenticatedUserRequest;
import bio.terra.exception.InternalServerErrorException;
import bio.terra.flight.study.create.CreateStudyAuthzResource;
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

public class DeleteDatasetAuthzResource implements Step {
    private SamClientService sam;
    private UUID datasetId;
    private static Logger logger = LoggerFactory.getLogger(CreateStudyAuthzResource.class);

    public DeleteDatasetAuthzResource(SamClientService sam, UUID datasetId) {
        this.sam = sam;
        this.datasetId = datasetId;
    }

    @Override
    public StepResult doStep(FlightContext context) {
        FlightMap inputParameters = context.getInputParameters();
        AuthenticatedUserRequest userReq = inputParameters.get(
            JobMapKeys.USER_INFO.getKeyName(), AuthenticatedUserRequest.class);
        try {
            sam.deleteDatasetResource(userReq, datasetId);
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
        logger.warn("Trying to undo delete resource for dataset " + datasetId.toString());
        return StepResult.getStepResultSuccess();
    }
}
