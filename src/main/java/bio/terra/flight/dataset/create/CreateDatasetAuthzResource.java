package bio.terra.flight.dataset.create;

import bio.terra.controller.AuthenticatedUser;
import bio.terra.exception.InternalServerErrorException;
import bio.terra.metadata.Dataset;
import bio.terra.pdao.bigquery.BigQueryPdao;
import bio.terra.service.JobMapKeys;
import bio.terra.service.SamClientService;
import bio.terra.service.DatasetService;
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

public class CreateDatasetAuthzResource implements Step {
    private static Logger logger = LoggerFactory.getLogger(CreateDatasetAuthzResource.class);

    private SamClientService sam;
    private BigQueryPdao bigQueryPdao;
    private DatasetService datasetService;

    public CreateDatasetAuthzResource(
        SamClientService sam,
        BigQueryPdao bigQueryPdao,
        DatasetService datasetService) {
        this.sam = sam;
        this.bigQueryPdao = bigQueryPdao;
        this.datasetService = datasetService;
    }

    @Override
    public StepResult doStep(FlightContext context) {
        FlightMap inputParameters = context.getInputParameters();
        AuthenticatedUser userReq = inputParameters.get(
            JobMapKeys.USER_INFO.getKeyName(), AuthenticatedUser.class);
        FlightMap workingMap = context.getWorkingMap();
        UUID datasetId = workingMap.get("datasetId", UUID.class);
        Dataset dataset = datasetService.retrieve(datasetId);
        try {
            List<String> policyEmails = sam.createDatasetResource(userReq, datasetId);
            bigQueryPdao.grantReadAccessToDataset(dataset, policyEmails);
            // TODO: on file ingest these policies also need to be added as readers
        } catch (ApiException ex) {
            throw new InternalServerErrorException(ex);
        }
        return StepResult.getStepResultSuccess();
    }

    @Override
    public StepResult undoStep(FlightContext context) {
        FlightMap inputParameters = context.getInputParameters();
        AuthenticatedUser userReq = inputParameters.get(
            JobMapKeys.USER_INFO.getKeyName(), AuthenticatedUser.class);
        FlightMap workingMap = context.getWorkingMap();
        UUID datasetId = workingMap.get("datasetId", UUID.class);
        try {
            sam.deleteDatasetResource(userReq, datasetId);
        } catch (ApiException ex) {
            if (ex.getCode() == HttpStatusCodes.STATUS_CODE_UNAUTHORIZED) {
                // suppress exception
                logger.error("NEEDS CLEANUP: delete sam resource for dataset " + datasetId.toString(), ex);
            } else if (ex.getCode() != HttpStatusCodes.STATUS_CODE_NOT_FOUND) {
                // if the SAM resource is not found, then it was likely not created -- continue undoing
                throw new InternalServerErrorException(ex);
            }
        }
        return StepResult.getStepResultSuccess();
    }
}
