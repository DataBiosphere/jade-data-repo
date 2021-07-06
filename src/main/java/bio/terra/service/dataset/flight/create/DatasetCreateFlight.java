package bio.terra.service.dataset.flight.create;

import static bio.terra.common.FlightUtils.getDefaultExponentialBackoffRetryRule;

import bio.terra.common.CloudPlatformWrapper;
import bio.terra.model.DatasetRequestModel;
import bio.terra.service.configuration.ConfigurationService;
import bio.terra.service.dataset.DatasetDao;
import bio.terra.service.dataset.DatasetService;
import bio.terra.service.dataset.DatasetStorageAccountDao;
import bio.terra.service.dataset.flight.UnlockDatasetStep;
import bio.terra.service.iam.AuthenticatedUserRequest;
import bio.terra.service.iam.IamProviderInterface;
import bio.terra.service.job.JobMapKeys;
import bio.terra.service.profile.ProfileService;
import bio.terra.service.profile.flight.AuthorizeBillingProfileUseStep;
import bio.terra.service.resourcemanagement.AzureDataLocationSelector;
import bio.terra.service.resourcemanagement.BufferService;
import bio.terra.service.resourcemanagement.ResourceService;
import bio.terra.service.tabulardata.google.BigQueryPdao;
import bio.terra.stairway.Flight;
import bio.terra.stairway.FlightMap;
import bio.terra.stairway.RetryRule;
import org.springframework.context.ApplicationContext;

public class DatasetCreateFlight extends Flight {

  public DatasetCreateFlight(FlightMap inputParameters, Object applicationContext) {
    super(inputParameters, applicationContext);

    // get the required daos and services to pass into the steps
    ApplicationContext appContext = (ApplicationContext) applicationContext;
    BufferService bufferService = appContext.getBean(BufferService.class);
    DatasetDao datasetDao = appContext.getBean(DatasetDao.class);
    DatasetService datasetService = appContext.getBean(DatasetService.class);
    ResourceService resourceService = appContext.getBean(ResourceService.class);
    BigQueryPdao bigQueryPdao = appContext.getBean(BigQueryPdao.class);
    IamProviderInterface iamClient = appContext.getBean("iamProvider", IamProviderInterface.class);
    ConfigurationService configService = appContext.getBean(ConfigurationService.class);
    ProfileService profileService = appContext.getBean(ProfileService.class);
    AzureDataLocationSelector azureDataLocationSelector =
        appContext.getBean(AzureDataLocationSelector.class);
    DatasetStorageAccountDao datasetStorageAccountDao =
        appContext.getBean(DatasetStorageAccountDao.class);

    DatasetRequestModel datasetRequest =
        inputParameters.get(JobMapKeys.REQUEST.getKeyName(), DatasetRequestModel.class);

    var platform = CloudPlatformWrapper.of(datasetRequest.getCloudPlatform());

    AuthenticatedUserRequest userReq =
        inputParameters.get(JobMapKeys.AUTH_USER_INFO.getKeyName(), AuthenticatedUserRequest.class);

    // Make sure this user is allowed to use the billing profile and that the underlying
    // billing information remains valid.
    addStep(
        new AuthorizeBillingProfileUseStep(
            profileService, datasetRequest.getDefaultProfileId(), userReq));

    // Get a new google project from RBS and store it in the working map
    addStep(new CreateDatasetGetProjectStep(bufferService));

    // Get or initialize the project where the dataset resources will be created
    addStep(new CreateDatasetGetOrCreateProjectStep(resourceService, datasetRequest));

    // Get or create the storage account where the dataset resources will be created for Azure
    if (platform.isAzure()) {
      addStep(
          new CreateDatasetGetOrCreateStorageAccountStep(
              resourceService, datasetRequest, azureDataLocationSelector));
    }

    // Generate the dateset id and stored it in the working map
    addStep(new CreateDatasetIdStep());

    // Create dataset metadata objects in postgres and lock the dataset
    addStep(new CreateDatasetMetadataStep(datasetDao, datasetRequest));

    // For azure backed datasets, add a link co connect the storage account to the dataset
    if (platform.isAzure()) {
      addStep(
          new CreateDatasetCreateStorageAccountLinkStep(datasetStorageAccountDao, datasetRequest));
    }

    addStep(new CreateDatasetPrimaryDataStep(bigQueryPdao, datasetDao));

    // The underlying service provides retries so we do not need to retry for IAM step
    addStep(new CreateDatasetAuthzIamStep(iamClient, userReq));

    // Google says that ACL change propagation happens in a few seconds, but can take 5-7 minutes.
    // The max
    // operation timeout is generous.
    RetryRule pdaoAclRetryRule = getDefaultExponentialBackoffRetryRule();
    addStep(
        new CreateDatasetAuthzPrimaryDataStep(bigQueryPdao, datasetService, configService),
        pdaoAclRetryRule);

    // The underlying service provides retries so we do not need to retry for BQ Job User step at
    // this time
    addStep(new CreateDatasetAuthzBqJobUserStep(datasetService, resourceService));

    addStep(new UnlockDatasetStep(datasetDao, false));
  }
}
