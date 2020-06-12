package bio.terra.service.dataset.flight.create;

import bio.terra.app.configuration.ApplicationConfiguration;
import bio.terra.model.DatasetRequestModel;
import bio.terra.service.configuration.ConfigurationService;
import bio.terra.service.dataset.DatasetDao;
import bio.terra.service.dataset.DatasetService;
import bio.terra.service.dataset.flight.UnlockDatasetStep;
import bio.terra.service.iam.AuthenticatedUserRequest;
import bio.terra.service.iam.IamAction;
import bio.terra.service.iam.IamProviderInterface;
import bio.terra.service.iam.IamResourceType;
import bio.terra.service.iam.flight.VerifyAuthorizationStep;
import bio.terra.service.job.JobMapKeys;
import bio.terra.service.resourcemanagement.DataLocationService;
import bio.terra.service.resourcemanagement.google.GoogleResourceService;
import bio.terra.service.tabulardata.google.BigQueryPdao;
import bio.terra.stairway.Flight;
import bio.terra.stairway.FlightMap;
import bio.terra.stairway.RetryRuleExponentialBackoff;
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
        IamProviderInterface iamClient = (IamProviderInterface) appContext.getBean("iamProvider");
        ApplicationConfiguration appConfig = (ApplicationConfiguration) appContext.getBean("applicationConfiguration");
        GoogleResourceService resourceService = (GoogleResourceService) appContext.getBean("googleResourceService");
        ConfigurationService configService = (ConfigurationService) appContext.getBean("configurationService");

        addStep(new VerifyAuthorizationStep(
            iamClient,
            IamResourceType.DATAREPO,
            appConfig.getResourceId(),
            IamAction.CREATE_DATASET));

        DatasetRequestModel datasetRequest =
            inputParameters.get(JobMapKeys.REQUEST.getKeyName(), DatasetRequestModel.class);
        addStep(new CreateDatasetMetadataStep(datasetDao, datasetRequest));

        // TODO: create dataset data project step
        //  right now the cloud project is created as part of the PrimaryDataStep below
        addStep(new CreateDatasetPrimaryDataStep(bigQueryPdao, datasetDao, dataLocationService));

        // The underlying service provides retries so we do not need to retry for IAM step
        AuthenticatedUserRequest userReq = inputParameters.get(
            JobMapKeys.AUTH_USER_INFO.getKeyName(), AuthenticatedUserRequest.class);
        addStep(new CreateDatasetAuthzIamStep(iamClient, userReq));

        // Google says that ACL change propagation happens in a few seconds, but can take 5-7 minutes. The max
        // operation timeout is generous.
        RetryRuleExponentialBackoff pdaoAclRetryRule =
            new RetryRuleExponentialBackoff(2, 30, 600);
        addStep(new CreateDatasetAuthzPrimaryDataStep(bigQueryPdao, datasetService, configService), pdaoAclRetryRule);

        // The underlying service provides retries so we do not need to retry for BQ Job User step at this time
        addStep(new CreateDatasetAuthzBqJobUserStep(iamClient, bigQueryPdao, datasetService, userReq, resourceService));

        addStep(new UnlockDatasetStep(datasetDao, false));
    }

}
