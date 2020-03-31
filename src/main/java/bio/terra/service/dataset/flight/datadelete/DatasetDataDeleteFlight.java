package bio.terra.service.dataset.flight.datadelete;

import bio.terra.app.configuration.ApplicationConfiguration;
import bio.terra.model.DataDeletionRequest;
import bio.terra.service.dataset.DatasetDao;
import bio.terra.service.dataset.DatasetService;
import bio.terra.service.dataset.flight.FetchDatasetStep;
import bio.terra.service.dataset.flight.LockDatasetStep;
import bio.terra.service.dataset.flight.UnlockDatasetStep;
import bio.terra.service.iam.AuthenticatedUserRequest;
import bio.terra.service.iam.IamAction;
import bio.terra.service.iam.IamResourceType;
import bio.terra.service.iam.IamService;
import bio.terra.service.iam.flight.VerifyAuthorizationStep;
import bio.terra.service.job.JobMapKeys;
import bio.terra.service.resourcemanagement.DataLocationService;
import bio.terra.service.tabulardata.google.BigQueryPdao;
import bio.terra.stairway.Flight;
import bio.terra.stairway.FlightMap;
import org.springframework.context.ApplicationContext;

public class DatasetDataDeleteFlight extends Flight {

    public DatasetDataDeleteFlight(FlightMap inputParameters, Object applicationContext) {
        super(inputParameters, applicationContext);

        // get the required daos and services to pass into the steps
        ApplicationContext appContext = (ApplicationContext) applicationContext;
        DatasetDao datasetDao = (DatasetDao) appContext.getBean("datasetDao");
        DatasetService datasetService = (DatasetService) appContext.getBean("datasetService");
        DataLocationService dataLocationService = (DataLocationService) appContext.getBean("dataLocationService");
        BigQueryPdao bigQueryPdao = (BigQueryPdao) appContext.getBean("bigQueryPdao");
        IamService iamClient = (IamService) appContext.getBean("iamService");
        ApplicationConfiguration appConfig = (ApplicationConfiguration) appContext.getBean("applicationConfiguration");

        // get data from inputs that steps need
        AuthenticatedUserRequest userReq = inputParameters.get(
            JobMapKeys.AUTH_USER_INFO.getKeyName(), AuthenticatedUserRequest.class);
        DataDeletionRequest dataDeletionRequest =
            inputParameters.get(JobMapKeys.REQUEST.getKeyName(), DataDeletionRequest.class);
        String datasetId = inputParameters.get(JobMapKeys.DATASET_ID.getKeyName(), String.class);

        addStep(new VerifyAuthorizationStep(
            iamClient,
            IamResourceType.DATASET,
            datasetId,
            IamAction.UPDATE_DATA));
        // need to lock, need dataset name and flight id
        //addStep(new CreateDatasetMetadataStep(datasetDao, datasetRequest));
        addStep(new FetchDatasetStep(datasetDao));
        // TODO: why name here?
        //addStep(new LockDatasetStep(datasetDao));
        addStep(new CreateExternalTableStep(bigQueryPdao, datasetService));
        /*
        - check to see access to file
        - create external temp table
        - validate row ids match
        - insert into soft delete table
        - unlock
        - clean up
         */

        // TODO: create dataset data project step
        // right now the cloud project is created as part of the PrimaryDataStep below
        //addStep(new UnlockDatasetStep(datasetDao));
    }

}
