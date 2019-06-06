package bio.terra.flight.dataset.create;

import bio.terra.controller.AuthenticatedUserRequest;
import bio.terra.dao.DatasetDao;
import bio.terra.exception.InternalServerErrorException;
import bio.terra.filesystem.FireStoreDependencyDao;
import bio.terra.flight.study.create.CreateStudyAuthzResource;
import bio.terra.metadata.Dataset;
import bio.terra.metadata.DatasetSource;
import bio.terra.model.DatasetRequestModel;
import bio.terra.pdao.bigquery.BigQueryPdao;
import bio.terra.pdao.gcs.GcsPdao;
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
    private SamClientService sam;
    private BigQueryPdao bigQueryPdao;
    private FireStoreDependencyDao fireStoreDao;
    private DatasetDao datasetDao;
    private GcsPdao gcsPdao;
    private static Logger logger = LoggerFactory.getLogger(CreateStudyAuthzResource.class);

    public AuthorizeDataset(BigQueryPdao bigQueryPdao,
                            SamClientService sam,
                            FireStoreDependencyDao fireStoreDao,
                            DatasetDao datasetDao,
                            GcsPdao gcsPdao) {
        this.bigQueryPdao = bigQueryPdao;
        this.sam = sam;
        this.fireStoreDao = fireStoreDao;
        this.datasetDao = datasetDao;
        this.gcsPdao = gcsPdao;
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
        String datasetName = datasetReq.getName();
        FlightMap workingMap = context.getWorkingMap();
        UUID datasetId = workingMap.get("datasetId", UUID.class);
        Dataset dataset = datasetDao.retrieveDataset(datasetId);
        Optional<List<String>> readersList = Optional.ofNullable(datasetReq.getReaders());
        try {
            // This returns the policy email created by Google to correspond to the readers list in SAM
            String readersPolicyEmail = sam.createDatasetResource(userReq, datasetId, readersList);
            bigQueryPdao.addReaderGroupToDataset(dataset, readersPolicyEmail);

            // Each study may keep its dependencies in its own scope. Therefore,
            // we have to iterate through the studies in the dataset and ask each one
            // to give us its list of file ids. Then we set acls on the files for that
            // study used by the dataset.
            for (DatasetSource datasetSource : dataset.getDatasetSources()) {
                String studyId = datasetSource.getStudy().getId().toString();
                List<String> fileIds = fireStoreDao.getStudyDatasetFileIds(studyId, datasetId.toString());
                gcsPdao.setAclOnFiles(studyId, fileIds, readersPolicyEmail);
            }
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
            // We do not need to remove the ACL from the files or BigQuery. It disappears
            // when SAM deletes the ACL. How 'bout that!
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
