package bio.terra.flight.dataset.delete;

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

public class DeleteDatasetAuthzResource implements Step {
    private SamClientService sam;
    public DeleteDatasetAuthzResource(SamClientService sam) {
        this.sam = sam;
    }

    private static Logger logger = LoggerFactory.getLogger(CreateDatasetAuthzResource.class);

    @Override
    public StepResult doStep(FlightContext context) {
        FlightMap inputParameters = context.getInputParameters();
        AuthenticatedUser userReq = inputParameters.get(
            JobMapKeys.USER_INFO.getKeyName(), AuthenticatedUser.class);
        UUID datasetId = inputParameters.get(JobMapKeys.REQUEST.getKeyName(), UUID.class);
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
        FlightMap inputParameters = context.getInputParameters();
        UUID datasetId = inputParameters.get(JobMapKeys.REQUEST.getKeyName(), UUID.class);
        logger.warn("Trying to undo delete resource for dataset " + datasetId.toString());
        return StepResult.getStepResultSuccess();
    }
}
