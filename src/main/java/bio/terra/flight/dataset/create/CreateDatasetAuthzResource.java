package bio.terra.flight.dataset.create;

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

public class CreateDatasetAuthzResource implements Step {
    private SamClientService sam;
    public CreateDatasetAuthzResource(SamClientService sam) {
        this.sam = sam;
    }

    private static Logger logger = LoggerFactory.getLogger(CreateStudyAuthzResource.class);

    @Override
    public StepResult doStep(FlightContext context) {
        FlightMap inputParameters = context.getInputParameters();
        AuthenticatedUserRequest userReq = inputParameters.get(
            JobMapKeys.USER_INFO.getKeyName(), AuthenticatedUserRequest.class);
        FlightMap workingMap = context.getWorkingMap();
        UUID datasetId = workingMap.get("datasetId", UUID.class);
        try {
            sam.createDatasetResource(userReq, datasetId);
        } catch (ApiException ex) {
            logger.warn(ex.getMessage());
            throw new InternalServerErrorException(ex);
        }
        return StepResult.getStepResultSuccess();
    }

    @Override
    public StepResult undoStep(FlightContext context) {
        FlightMap inputParameters = context.getInputParameters();
        AuthenticatedUserRequest userReq = inputParameters.get(
            JobMapKeys.USER_INFO.getKeyName(), AuthenticatedUserRequest.class);
        FlightMap workingMap = context.getWorkingMap();
        UUID datasetId = workingMap.get("datasetId", UUID.class);
        System.out.println("im in undo ds resource");
        try {
            sam.deleteDatasetResource(userReq, datasetId);
        } catch (ApiException ex) {
            throw new InternalServerErrorException(ex);
        }
        return StepResult.getStepResultSuccess();
    }
}
