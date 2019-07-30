package bio.terra.flight.study.create;

import bio.terra.controller.AuthenticatedUserRequest;
import bio.terra.exception.InternalServerErrorException;
import bio.terra.metadata.Study;
import bio.terra.pdao.bigquery.BigQueryPdao;
import bio.terra.service.JobMapKeys;
import bio.terra.service.SamClientService;
import bio.terra.service.StudyService;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.FlightMap;
import bio.terra.stairway.Step;
import bio.terra.stairway.StepResult;
import com.google.api.client.http.HttpStatusCodes;
import org.broadinstitute.dsde.workbench.client.sam.ApiException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.UUID;

public class CreateStudyAuthzResource implements Step {
    private static Logger logger = LoggerFactory.getLogger(CreateStudyAuthzResource.class);

    private SamClientService sam;
    private BigQueryPdao bigQueryPdao;
    private StudyService studyService;

    public CreateStudyAuthzResource(
        SamClientService sam,
        BigQueryPdao bigQueryPdao,
        StudyService studyService) {
        this.sam = sam;
        this.bigQueryPdao = bigQueryPdao;
        this.studyService = studyService;
    }

    @Override
    public StepResult doStep(FlightContext context) {
        FlightMap inputParameters = context.getInputParameters();
        AuthenticatedUserRequest userReq = inputParameters.get(
            JobMapKeys.USER_INFO.getKeyName(), AuthenticatedUserRequest.class);
        FlightMap workingMap = context.getWorkingMap();
        UUID studyId = workingMap.get("studyId", UUID.class);
        Study study = studyService.retrieve(studyId);
        try {
            List<String> policyEmails = sam.createStudyResource(userReq, studyId);
            bigQueryPdao.grantReadAccessToStudy(study, policyEmails);
            // TODO: on file ingest these policies also need to be added as readers
        } catch (ApiException ex) {
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
            if (ex.getCode() == HttpStatusCodes.STATUS_CODE_UNAUTHORIZED) {
                // suppress exception
                logger.error("NEEDS CLEANUP: delete sam resource for study " + studyId.toString());
                logger.warn(ex.getMessage());
            } else {
                throw new InternalServerErrorException(ex);
            }
        }
        return StepResult.getStepResultSuccess();
    }
}
