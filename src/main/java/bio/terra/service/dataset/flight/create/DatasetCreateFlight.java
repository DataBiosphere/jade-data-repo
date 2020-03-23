package bio.terra.service.dataset.flight.create;

import bio.terra.app.configuration.ApplicationConfiguration;
import bio.terra.model.DatasetRequestModel;
import bio.terra.service.dataset.DatasetDao;
import bio.terra.service.dataset.DatasetService;
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

public class DatasetCreateFlight extends Flight {

    public DatasetCreateFlight(FlightMap inputParameters, Object applicationContext) {
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
        DatasetRequestModel datasetRequest =
            inputParameters.get(JobMapKeys.REQUEST.getKeyName(), DatasetRequestModel.class);

        addStep(new VerifyAuthorizationStep(
            iamClient,
            IamResourceType.DATAREPO,
            appConfig.getResourceId(),
            IamAction.CREATE_DATASET));
        addStep(new CreateDatasetMetadataStep(datasetDao, datasetRequest));
        // TODO: create dataset data project step
        // right now the cloud project is created as part of the PrimaryDataStep below
        addStep(new CreateDatasetPrimaryDataStep(bigQueryPdao, datasetDao, dataLocationService));
        addStep(new CreateDatasetAuthzResource(iamClient, bigQueryPdao, datasetService, userReq));
        addStep(new UnlockDatasetStep(datasetDao));
    }

}
