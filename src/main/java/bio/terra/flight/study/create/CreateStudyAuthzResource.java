package bio.terra.flight.study.create;

import bio.terra.controller.AuthenticatedUserRequest;
import bio.terra.exception.InternalServerErrorException;
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

public class CreateStudyAuthzResource implements Step {

    private SamClientService sam;
    public CreateStudyAuthzResource(SamClientService sam) {
        this.sam = sam;
    }

    private static Logger logger = LoggerFactory.getLogger(CreateStudyAuthzResource.class);

    @Override
    public StepResult doStep(FlightContext context) {
        FlightMap inputParameters = context.getInputParameters();
        AuthenticatedUserRequest userReq = inputParameters.get(
            JobMapKeys.USER_INFO.getKeyName(), AuthenticatedUserRequest.class);
        FlightMap workingMap = context.getWorkingMap();
        UUID studyId = workingMap.get("studyId", UUID.class);
        try {
            sam.createStudyResource(userReq, studyId);
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
        UUID studyId = workingMap.get("studyId", UUID.class);
        try {
            sam.deleteStudyResource(userReq, studyId);
        } catch (ApiException ex) {
            throw new InternalServerErrorException(ex);
        }
        return StepResult.getStepResultSuccess();
    }
}
