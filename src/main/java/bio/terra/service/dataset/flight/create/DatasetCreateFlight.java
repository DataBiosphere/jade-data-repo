package bio.terra.service.dataset.flight.create;

import static bio.terra.common.FlightUtils.getDefaultExponentialBackoffRetryRule;
import static bio.terra.common.FlightUtils.getDefaultRandomBackoffRetryRule;

import bio.terra.app.configuration.ApplicationConfiguration;
import bio.terra.common.CloudPlatformWrapper;
import bio.terra.common.GetResourceBufferProjectStep;
import bio.terra.common.iam.AuthenticatedUserRequest;
import bio.terra.model.DatasetRequestModel;
import bio.terra.service.auth.iam.IamProviderInterface;
import bio.terra.service.auth.iam.IamService;
import bio.terra.service.configuration.ConfigurationService;
import bio.terra.service.dataset.DatasetBucketDao;
import bio.terra.service.dataset.DatasetDao;
import bio.terra.service.dataset.DatasetService;
import bio.terra.service.dataset.DatasetStorageAccountDao;
import bio.terra.service.dataset.flight.UnlockDatasetStep;
import bio.terra.service.filedata.azure.blobstore.AzureBlobStorePdao;
import bio.terra.service.job.JobMapKeys;
import bio.terra.service.journal.JournalService;
import bio.terra.service.profile.ProfileService;
import bio.terra.service.profile.flight.AuthorizeBillingProfileUseStep;
import bio.terra.service.profile.flight.VerifyBillingAccountAccessStep;
import bio.terra.service.profile.google.GoogleBillingService;
import bio.terra.service.resourcemanagement.BufferService;
import bio.terra.service.resourcemanagement.ResourceService;
import bio.terra.service.resourcemanagement.azure.AzureContainerPdao;
import bio.terra.service.resourcemanagement.azure.AzureMonitoringService;
import bio.terra.service.resourcemanagement.flight.AzureStorageMonitoringStepProvider;
import bio.terra.service.resourcemanagement.google.GoogleResourceManagerService;
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
    ApplicationConfiguration appConfig = appContext.getBean(ApplicationConfiguration.class);
    BufferService bufferService = appContext.getBean(BufferService.class);
    DatasetDao datasetDao = appContext.getBean(DatasetDao.class);
    DatasetBucketDao datasetBucketDao = appContext.getBean(DatasetBucketDao.class);
    DatasetService datasetService = appContext.getBean(DatasetService.class);
    ResourceService resourceService = appContext.getBean(ResourceService.class);
    BigQueryDatasetPdao bigQueryDatasetPdao = appContext.getBean(BigQueryDatasetPdao.class);
    IamProviderInterface iamClient = appContext.getBean("iamProvider", IamProviderInterface.class);
    IamService iamService = appContext.getBean(IamService.class);
    ConfigurationService configService = appContext.getBean(ConfigurationService.class);
    ProfileService profileService = appContext.getBean(ProfileService.class);
    AzureContainerPdao azureContainerPdao = appContext.getBean(AzureContainerPdao.class);
    DatasetStorageAccountDao datasetStorageAccountDao =
        appContext.getBean(DatasetStorageAccountDao.class);
    AzureBlobStorePdao azureBlobStorePdao = appContext.getBean(AzureBlobStorePdao.class);
    GoogleBillingService googleBillingService = appContext.getBean(GoogleBillingService.class);
    GoogleResourceManagerService googleResourceManagerService =
        appContext.getBean(GoogleResourceManagerService.class);
    JournalService journalService = appContext.getBean(JournalService.class);
    AzureMonitoringService monitoringService = appContext.getBean(AzureMonitoringService.class);

    DatasetRequestModel datasetRequest =
        inputParameters.get(JobMapKeys.REQUEST.getKeyName(), DatasetRequestModel.class);

    AzureStorageMonitoringStepProvider azureStorageMonitoringStepProvider =
        new AzureStorageMonitoringStepProvider(monitoringService);

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
              bufferService,
              googleResourceManagerService,
              datasetRequest.isEnableSecureMonitoring()));

      // Get or initialize the project where the dataset resources will be created
      addStep(
          new CreateDatasetInitializeProjectStep(resourceService, datasetRequest),
          getDefaultExponentialBackoffRetryRule());

      // Create the service account to use to ingest data and register it in Terra
      if (datasetRequest.isDedicatedIngestServiceAccount()) {
        addStep(new CreateDatasetCreateIngestServiceAccountStep(resourceService, datasetRequest));
        addStep(new CreateDatasetRegisterIngestServiceAccountStep(iamService));
      }
    }

    // Get or create the storage account where the dataset resources will be created for Azure
    if (platform.isAzure()) {
      addStep(
          new CreateDatasetGetOrCreateStorageAccountStep(
              resourceService, datasetRequest, azureBlobStorePdao));

      // Create the top level container
      addStep(
          new CreateDatasetGetOrCreateContainerStep(
              resourceService, datasetRequest, azureContainerPdao));

      // Turn on logging and monitoring for the storage account associated with the dataset
      azureStorageMonitoringStepProvider
          .configureSteps(datasetRequest.isEnableSecureMonitoring())
          .forEach(s -> this.addStep(s.step(), s.retryRule()));
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

    // Create the IAM resource for the dataset with any specified policy members.
    // The underlying service provides retries so we do not need to retry for IAM step
    addStep(new CreateDatasetAuthzIamStep(iamClient, userReq, datasetRequest));

    if (platform.isGcp()) {
      addStep(new CreateDatasetPrimaryDataStep(bigQueryDatasetPdao, datasetDao));

      // Google says that ACL change propagation happens in a few seconds, but can take 5-7 minutes.
      // The max operation timeout is generous.
      RetryRule pdaoAclRetryRule = getDefaultExponentialBackoffRetryRule();
      addStep(
          new CreateDatasetAuthzPrimaryDataStep(bigQueryDatasetPdao, datasetService, configService),
          pdaoAclRetryRule);

      // The underlying service provides retries so we do not need to retry for BQ Job User step at
      // this time
      addStep(new CreateDatasetAuthzBqJobUserStep(datasetService, resourceService));

      // Create bucket where dataset files will be stored
      addStep(
          new CreateDatasetGetOrCreateBucketStep(
              userReq, resourceService, datasetRequest, iamService),
          getDefaultExponentialBackoffRetryRule());

      // Record bucket in postgres
      addStep(
          new DatasetCreateMakeBucketLinkStep(datasetBucketDao),
          getDefaultRandomBackoffRetryRule(appConfig.getMaxStairwayThreads()));
    }
    addStep(new UnlockDatasetStep(datasetService, false));
    // once unlocked, the dataset summary can be written as the job response
    addStep(new CreateDatasetSetResponseStep(datasetService));
    addStep(new CreateDatasetJournalEntryStep(journalService, userReq));
  }
}
