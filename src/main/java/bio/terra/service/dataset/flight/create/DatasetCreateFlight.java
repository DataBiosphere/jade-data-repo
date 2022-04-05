package bio.terra.service.dataset.flight.create;

import static bio.terra.common.FlightUtils.getDefaultExponentialBackoffRetryRule;

import bio.terra.common.CloudPlatformWrapper;
import bio.terra.common.GetResourceBufferProjectStep;
import bio.terra.common.iam.AuthenticatedUserRequest;
import bio.terra.model.DatasetRequestModel;
import bio.terra.service.auth.iam.IamProviderInterface;
import bio.terra.service.configuration.ConfigurationService;
import bio.terra.service.dataset.DatasetDao;
import bio.terra.service.dataset.DatasetService;
import bio.terra.service.dataset.DatasetStorageAccountDao;
import bio.terra.service.dataset.flight.UnlockDatasetStep;
import bio.terra.service.filedata.azure.blobstore.AzureBlobStorePdao;
import bio.terra.service.job.JobMapKeys;
import bio.terra.service.profile.ProfileService;
import bio.terra.service.profile.flight.AuthorizeBillingProfileUseStep;
import bio.terra.service.profile.flight.VerifyBillingAccountAccessStep;
import bio.terra.service.profile.google.GoogleBillingService;
import bio.terra.service.resourcemanagement.BufferService;
import bio.terra.service.resourcemanagement.ResourceService;
import bio.terra.service.resourcemanagement.azure.AzureContainerPdao;
import bio.terra.service.resourcemanagement.azure.AzureStorageAccountResource.ContainerType;
import bio.terra.service.tabulardata.google.bigquery.BigQueryDatasetPdao;
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
    BigQueryDatasetPdao bigQueryDatasetPdao = appContext.getBean(BigQueryDatasetPdao.class);
    IamProviderInterface iamClient = appContext.getBean("iamProvider", IamProviderInterface.class);
    ConfigurationService configService = appContext.getBean(ConfigurationService.class);
    ProfileService profileService = appContext.getBean(ProfileService.class);
    AzureContainerPdao azureContainerPdao = appContext.getBean(AzureContainerPdao.class);
    DatasetStorageAccountDao datasetStorageAccountDao =
        appContext.getBean(DatasetStorageAccountDao.class);
    AzureBlobStorePdao azureBlobStorePdao = appContext.getBean(AzureBlobStorePdao.class);
    GoogleBillingService googleBillingService = appContext.getBean(GoogleBillingService.class);

    DatasetRequestModel datasetRequest =
        inputParameters.get(JobMapKeys.REQUEST.getKeyName(), DatasetRequestModel.class);

    var platform = CloudPlatformWrapper.of(datasetRequest.getCloudPlatform());

    AuthenticatedUserRequest userReq =
        inputParameters.get(JobMapKeys.AUTH_USER_INFO.getKeyName(), AuthenticatedUserRequest.class);

    // Make sure this user is authorized to use the billing profile in SAM
    addStep(
        new AuthorizeBillingProfileUseStep(
            profileService, datasetRequest.getDefaultProfileId(), userReq));

    // Generate the dateset id and store it in the working map
    addStep(new CreateDatasetIdStep());

    if (platform.isGcp()) {
      addStep(new VerifyBillingAccountAccessStep(googleBillingService));

      // Get a new google project from RBS and store it in the working map
      addStep(
          new GetResourceBufferProjectStep(
              bufferService, datasetRequest.isEnableSecureMonitoring()));

      // Get or initialize the project where the dataset resources will be created
      addStep(
          new CreateDatasetInitializeProjectStep(resourceService, datasetRequest),
          getDefaultExponentialBackoffRetryRule());
    }

    // Get or create the storage account where the dataset resources will be created for Azure
    if (platform.isAzure()) {
      addStep(
          new CreateDatasetGetOrCreateStorageAccountStep(
              resourceService, datasetRequest, azureBlobStorePdao));

      // Create the metadata container
      addStep(
          new CreateDatasetGetOrCreateContainerStep(
              resourceService, datasetRequest, azureContainerPdao, ContainerType.METADATA));

      // Create the data container
      addStep(
          new CreateDatasetGetOrCreateContainerStep(
              resourceService, datasetRequest, azureContainerPdao, ContainerType.DATA));
    }

    // Create dataset metadata objects in postgres and lock the dataset
    addStep(
        new CreateDatasetMetadataStep(datasetDao, datasetRequest),
        getDefaultExponentialBackoffRetryRule());

    // For azure backed datasets, add a link co connect the storage account to the dataset
    if (platform.isAzure()) {
      addStep(
          new CreateDatasetCreateStorageAccountLinkStep(datasetStorageAccountDao, datasetRequest));
    }

    // The underlying service provides retries so we do not need to retry for IAM step
    addStep(new CreateDatasetAuthzIamStep(iamClient, userReq));

    if (platform.isGcp()) {
      addStep(new CreateDatasetPrimaryDataStep(bigQueryDatasetPdao, datasetDao));

      // Google says that ACL change propagation happens in a few seconds, but can take 5-7 minutes.
      // The max
      // operation timeout is generous.
      RetryRule pdaoAclRetryRule = getDefaultExponentialBackoffRetryRule();
      addStep(
          new CreateDatasetAuthzPrimaryDataStep(bigQueryDatasetPdao, datasetService, configService),
          pdaoAclRetryRule);

      // The underlying service provides retries so we do not need to retry for BQ Job User step at
      // this time
      addStep(new CreateDatasetAuthzBqJobUserStep(datasetService, resourceService));
    }
    addStep(new UnlockDatasetStep(datasetService, false));
  }
}
