package bio.terra.flight.dataset.create;

import bio.terra.controller.AuthenticatedUserRequest;
import bio.terra.dao.DatasetDao;
import bio.terra.exception.InternalServerErrorException;
import bio.terra.flight.study.create.CreateStudyAuthzResource;
import bio.terra.metadata.Dataset;
import bio.terra.model.DatasetRequestModel;
import bio.terra.pdao.bigquery.BigQueryPdao;
import bio.terra.service.JobMapKeys;
import bio.terra.service.SamClientService;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.FlightMap;
import bio.terra.stairway.Step;
import bio.terra.stairway.StepResult;
import com.google.api.client.http.HttpStatusCodes;
import org.broadinstitute.dsde.workbench.client.sam.ApiException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Optional;
import java.util.UUID;

public class AuthorizeDataset implements Step {
    private static Logger logger = LoggerFactory.getLogger(CreateStudyAuthzResource.class);

    private final SamClientService sam;
    private final BigQueryPdao bigQueryPdao;
    private final DatasetDao datasetDao;

    public AuthorizeDataset(BigQueryPdao bigQueryPdao, SamClientService sam, DatasetDao datasetDao) {
        this.bigQueryPdao = bigQueryPdao;
        this.sam = sam;
        this.datasetDao = datasetDao;
    }

    DatasetRequestModel getRequestModel(FlightContext context) {
        FlightMap inputParameters = context.getInputParameters();
        return inputParameters.get(JobMapKeys.REQUEST.getKeyName(), DatasetRequestModel.class);
    }

    @Override
    public StepResult doStep(FlightContext context) {
        FlightMap inputParameters = context.getInputParameters();
        AuthenticatedUserRequest userReq = inputParameters.get(
            JobMapKeys.USER_INFO.getKeyName(), AuthenticatedUserRequest.class);
        DatasetRequestModel datasetReq = getRequestModel(context);
        FlightMap workingMap = context.getWorkingMap();
        UUID datasetId = workingMap.get("datasetId", UUID.class);
        Dataset dataset = datasetDao.retrieveDataset(datasetId);
        Optional<List<String>> readersList = Optional.ofNullable(datasetReq.getReaders());
        try {
            // This returns the policy email created by Google to correspond to the readers list in SAM
            String readersPolicyEmail = sam.createDatasetResource(userReq, datasetId, readersList);
            bigQueryPdao.addReaderGroupToDataset(dataset, readersPolicyEmail);
        } catch (ApiException ex) {
            throw new InternalServerErrorException("Couldn't add readers", ex);
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
        try {
            sam.deleteDatasetResource(userReq, datasetId);
        } catch (ApiException ex) {
            if (ex.getCode() == HttpStatusCodes.STATUS_CODE_UNAUTHORIZED) {
                // suppress exception
                logger.error("NEEDS CLEANUP: delete sam resource for dataset " + datasetId.toString());
                logger.warn(ex.getMessage());
            } else {
                throw new InternalServerErrorException(ex);
            }

        }
        return StepResult.getStepResultSuccess();
    }
}
