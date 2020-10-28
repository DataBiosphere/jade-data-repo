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
import bio.terra.service.profile.ProfileService;
import bio.terra.service.profile.flight.AuthorizeBillingProfileUseStep;
import bio.terra.service.resourcemanagement.ResourceService;
import bio.terra.service.tabulardata.google.BigQueryPdao;
import bio.terra.stairway.Flight;
import bio.terra.stairway.FlightMap;
import bio.terra.stairway.RetryRule;
import org.springframework.context.ApplicationContext;

import static bio.terra.common.FlightUtils.getDefaultExponentialBackoffRetryRule;

public class DatasetCreateFlight extends Flight {

    public DatasetCreateFlight(FlightMap inputParameters, Object applicationContext) {
        super(inputParameters, applicationContext);

        // get the required daos and services to pass into the steps
        ApplicationContext appContext = (ApplicationContext) applicationContext;
        DatasetDao datasetDao = (DatasetDao) appContext.getBean("datasetDao");
        DatasetService datasetService = (DatasetService) appContext.getBean("datasetService");
        ResourceService resourceService = (ResourceService) appContext.getBean("resourceService");
        BigQueryPdao bigQueryPdao = (BigQueryPdao) appContext.getBean("bigQueryPdao");
        IamProviderInterface iamClient = (IamProviderInterface) appContext.getBean("iamProvider");
        ApplicationConfiguration appConfig = (ApplicationConfiguration) appContext.getBean("applicationConfiguration");
        ConfigurationService configService = (ConfigurationService) appContext.getBean("configurationService");
        ProfileService profileService = (ProfileService) appContext.getBean("profileService");

        addStep(new VerifyAuthorizationStep(
            iamClient,
            IamResourceType.DATAREPO,
            appConfig.getResourceId(),
            IamAction.CREATE_DATASET));

        DatasetRequestModel datasetRequest =
            inputParameters.get(JobMapKeys.REQUEST.getKeyName(), DatasetRequestModel.class);

        AuthenticatedUserRequest userReq = inputParameters.get(
            JobMapKeys.AUTH_USER_INFO.getKeyName(), AuthenticatedUserRequest.class);

        // Make sure this user is allowed to use the billing profile and that the underlying
        // billing information remains valid.
        addStep(new AuthorizeBillingProfileUseStep(profileService, datasetRequest.getDefaultProfileId(), userReq));

        // Get or create the project where the dataset resources will be created
        addStep(new CreateDatasetGetOrCreateProjectStep(resourceService, datasetRequest));

        // Generate the dateset id and stored it in the working map
        addStep(new CreateDatasetIdStep());

        // Create dataset metadata objects in postgres and lock the dataset
        addStep(new CreateDatasetMetadataStep(datasetDao, datasetRequest));

        addStep(new CreateDatasetPrimaryDataStep(bigQueryPdao, datasetDao));

        // The underlying service provides retries so we do not need to retry for IAM step
        addStep(new CreateDatasetAuthzIamStep(iamClient, userReq));

        // Google says that ACL change propagation happens in a few seconds, but can take 5-7 minutes. The max
        // operation timeout is generous.
        RetryRule pdaoAclRetryRule = getDefaultExponentialBackoffRetryRule();
        addStep(new CreateDatasetAuthzPrimaryDataStep(bigQueryPdao, datasetService, configService), pdaoAclRetryRule);

        // The underlying service provides retries so we do not need to retry for BQ Job User step at this time
        addStep(new CreateDatasetAuthzBqJobUserStep(datasetService, resourceService));

        addStep(new UnlockDatasetStep(datasetDao, false));
    }

}
