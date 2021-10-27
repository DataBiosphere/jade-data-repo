package bio.terra.service.dataset.flight.ingest;

import static bio.terra.common.FlightUtils.getDefaultRandomBackoffRetryRule;

import bio.terra.app.configuration.ApplicationConfiguration;
import bio.terra.common.CloudPlatformWrapper;
import bio.terra.common.ValidateBucketAccessStep;
import bio.terra.model.CloudPlatform;
import bio.terra.model.IngestRequestModel;
import bio.terra.service.configuration.ConfigEnum;
import bio.terra.service.configuration.ConfigurationService;
import bio.terra.service.dataset.Dataset;
import bio.terra.service.dataset.DatasetBucketDao;
import bio.terra.service.dataset.DatasetDao;
import bio.terra.service.dataset.DatasetService;
import bio.terra.service.dataset.DatasetStorageAccountDao;
import bio.terra.service.dataset.flight.LockDatasetStep;
import bio.terra.service.dataset.flight.UnlockDatasetStep;
import bio.terra.service.filedata.FileService;
import bio.terra.service.filedata.azure.AzureSynapsePdao;
import bio.terra.service.filedata.azure.blobstore.AzureBlobStorePdao;
import bio.terra.service.filedata.azure.tables.TableDirectoryDao;
import bio.terra.service.filedata.flight.ingest.CreateBucketForBigQueryScratchStep;
import bio.terra.service.filedata.flight.ingest.IngestBuildAndWriteScratchLoadFileAzureStep;
import bio.terra.service.filedata.flight.ingest.IngestBuildAndWriteScratchLoadFileGcpStep;
import bio.terra.service.filedata.flight.ingest.IngestCleanFileStateStep;
import bio.terra.service.filedata.flight.ingest.IngestCopyLoadHistoryToBQStep;
import bio.terra.service.filedata.flight.ingest.IngestCopyLoadHistoryToStorageTableStep;
import bio.terra.service.filedata.flight.ingest.IngestCreateAzureStorageAccountStep;
import bio.terra.service.filedata.flight.ingest.IngestDriverStep;
import bio.terra.service.filedata.flight.ingest.IngestFileAzureMakeStorageAccountLinkStep;
import bio.terra.service.filedata.flight.ingest.IngestFileGetProjectStep;
import bio.terra.service.filedata.flight.ingest.IngestFileInitializeProjectStep;
import bio.terra.service.filedata.flight.ingest.IngestFileMakeBucketLinkStep;
import bio.terra.service.filedata.flight.ingest.IngestFilePrimaryDataLocationStep;
import bio.terra.service.filedata.google.firestore.FireStoreDao;
import bio.terra.service.filedata.google.gcs.GcsPdao;
import bio.terra.service.iam.AuthenticatedUserRequest;
import bio.terra.service.job.JobMapKeys;
import bio.terra.service.job.JobService;
import bio.terra.service.load.LoadService;
import bio.terra.service.load.flight.LoadLockStep;
import bio.terra.service.load.flight.LoadUnlockStep;
import bio.terra.service.profile.ProfileService;
import bio.terra.service.profile.flight.AuthorizeBillingProfileUseStep;
import bio.terra.service.resourcemanagement.ResourceService;
import bio.terra.service.resourcemanagement.azure.AzureAuthService;
import bio.terra.service.resourcemanagement.azure.AzureContainerPdao;
import bio.terra.service.resourcemanagement.google.GoogleProjectService;
import bio.terra.service.tabulardata.azure.StorageTableService;
import bio.terra.service.tabulardata.google.BigQueryPdao;
import bio.terra.stairway.Flight;
import bio.terra.stairway.FlightMap;
import bio.terra.stairway.RetryRule;
import bio.terra.stairway.RetryRuleExponentialBackoff;
import bio.terra.stairway.Step;
import java.util.Objects;
import java.util.UUID;
import org.springframework.context.ApplicationContext;

public class DatasetIngestFlight extends Flight {

  public DatasetIngestFlight(FlightMap inputParameters, Object applicationContext) {
    super(inputParameters, applicationContext);

    // get the required daos to pass into the steps
    ApplicationContext appContext = (ApplicationContext) applicationContext;
    DatasetDao datasetDao = appContext.getBean(DatasetDao.class);
    DatasetService datasetService = appContext.getBean(DatasetService.class);
    BigQueryPdao bigQueryPdao = appContext.getBean(BigQueryPdao.class);
    FireStoreDao fileDao = appContext.getBean(FireStoreDao.class);
    ConfigurationService configService = appContext.getBean(ConfigurationService.class);
    ApplicationConfiguration appConfig = appContext.getBean(ApplicationConfiguration.class);
    ProfileService profileService = appContext.getBean(ProfileService.class);
    AzureSynapsePdao azureSynapsePdao = appContext.getBean(AzureSynapsePdao.class);
    AzureAuthService azureAuthService = appContext.getBean(AzureAuthService.class);
    TableDirectoryDao tableDirectoryDao = appContext.getBean(TableDirectoryDao.class);
    ResourceService resourceService = appContext.getBean(ResourceService.class);
    AzureBlobStorePdao azureBlobStorePdao = appContext.getBean(AzureBlobStorePdao.class);
    FileService fileService = appContext.getBean(FileService.class);
    GcsPdao gcsPdao = appContext.getBean(GcsPdao.class);

    IngestRequestModel ingestRequestModel =
        inputParameters.get(JobMapKeys.REQUEST.getKeyName(), IngestRequestModel.class);
    UUID datasetId =
        UUID.fromString(inputParameters.get(JobMapKeys.DATASET_ID.getKeyName(), String.class));
    CloudPlatformWrapper cloudPlatform =
        CloudPlatformWrapper.of(
            datasetService.retrieve(datasetId).getDatasetSummary().getStorageCloudPlatform());
    AuthenticatedUserRequest userReq =
        inputParameters.get(JobMapKeys.AUTH_USER_INFO.getKeyName(), AuthenticatedUserRequest.class);

    RetryRule lockDatasetRetry =
        getDefaultRandomBackoffRetryRule(appConfig.getMaxStairwayThreads());

    if (cloudPlatform.isAzure()) {
      // TODO: We can get rid of this once https://broadworkbench.atlassian.net/browse/DR-2107
      // is complete
      addStep(
          new AuthorizeBillingProfileUseStep(
              profileService, ingestRequestModel.getProfileId(), userReq));

      // This will need to stay even after DR-2107
      addStep(new IngestCreateAzureStorageAccountStep(datasetService, resourceService));
    }

    addStep(new LockDatasetStep(datasetDao, datasetId, true), lockDatasetRetry);

    addStep(new IngestSetupStep(datasetService, configService, cloudPlatform));

    if (ingestRequestModel.getFormat() == IngestRequestModel.FormatEnum.JSON) {
      int driverWaitSeconds = configService.getParameterValue(ConfigEnum.LOAD_DRIVER_WAIT_SECONDS);
      int loadHistoryWaitSeconds =
          configService.getParameterValue(ConfigEnum.LOAD_HISTORY_WAIT_SECONDS);
      int loadHistoryChunkSize =
          configService.getParameterValue(ConfigEnum.LOAD_HISTORY_COPY_CHUNK_SIZE);
      RetryRule randomBackoffRetry =
          getDefaultRandomBackoffRetryRule(appConfig.getMaxStairwayThreads());
      RetryRule driverRetry = new RetryRuleExponentialBackoff(5, 20, 600);

      Dataset dataset = datasetService.retrieve(datasetId);
      var profileId =
          Objects.requireNonNullElse(
              ingestRequestModel.getProfileId(), dataset.getDefaultProfileId());

      if (cloudPlatform.isGcp()) {
        addGcpJsonSteps(
            appContext,
            appConfig,
            datasetService,
            gcsPdao,
            bigQueryPdao,
            configService,
            fileService,
            ingestRequestModel,
            userReq,
            dataset,
            profileId,
            randomBackoffRetry,
            driverRetry,
            driverWaitSeconds,
            loadHistoryWaitSeconds,
            loadHistoryChunkSize);
      } else if (cloudPlatform.isAzure()) {
        addAzureJsonSteps(
            appContext,
            appConfig,
            datasetService,
            azureBlobStorePdao,
            configService,
            fileService,
            ingestRequestModel,
            userReq,
            dataset,
            profileId,
            randomBackoffRetry,
            driverRetry,
            driverWaitSeconds,
            loadHistoryChunkSize);
      }
    }

    if (cloudPlatform.isGcp()) {
      addStep(new IngestControlFileCopyNeededStep(datasetService, gcsPdao));
      // If we need to copy, make (or get) the scratch bucket
      addStep(
          new ControlFileCopyNeededOptionalStep(
              new CreateBucketForBigQueryScratchStep(resourceService, datasetService)),
          getDefaultRandomBackoffRetryRule(appConfig.getMaxStairwayThreads()));
      // If we need to copy, copy to the scratch bucket
      addStep(
          new ControlFileCopyNeededOptionalStep(
              new IngestCopyControlFileStep(datasetService, gcsPdao)));
      addStep(new IngestLoadTableStep(datasetService, bigQueryPdao));
      addStep(new IngestRowIdsStep(datasetService, bigQueryPdao));
      addStep(new IngestValidateGcpRefsStep(datasetService, bigQueryPdao, fileDao));
      addStep(new IngestInsertIntoDatasetTableStep(datasetService, bigQueryPdao));
      addStep(new IngestCleanupStep(datasetService, bigQueryPdao));
    } else if (cloudPlatform.isAzure()) {
      addStep(new IngestCreateIngestRequestDataSourceStep(azureSynapsePdao, azureBlobStorePdao));
      addStep(new IngestCreateTargetDataSourceStep(azureSynapsePdao, azureBlobStorePdao));
      addStep(new IngestCreateParquetFilesStep(azureSynapsePdao, datasetService));
      addStep(
          new IngestValidateAzureRefsStep(
              azureAuthService, datasetService, azureSynapsePdao, tableDirectoryDao));
      addStep(new IngestCleanSynapseStep(azureSynapsePdao));
    }
    addStep(new UnlockDatasetStep(datasetDao, datasetId, true), lockDatasetRetry);
  }

  private void addOptionalCombinedIngestStep(Step step, RetryRule retryRule) {
    addStep(new CombinedFileIngestOptionalStep(step), retryRule);
  }

  private void addOptionalCombinedIngestStep(Step step) {
    addStep(new CombinedFileIngestOptionalStep(step));
  }

  /**
   * These are the steps needed for combined metadata + file ingest. This only works with JSON
   * formatted requests, so no CSV parsing should happen here.
   */
  private void addGcpJsonSteps(
      ApplicationContext appContext,
      ApplicationConfiguration appConfig,
      DatasetService datasetService,
      GcsPdao gcsPdao,
      BigQueryPdao bigQueryPdao,
      ConfigurationService configService,
      FileService fileService,
      IngestRequestModel ingestRequest,
      AuthenticatedUserRequest userReq,
      Dataset dataset,
      UUID profileId,
      RetryRule randomBackoffRetry,
      RetryRule driverRetry,
      int driverWaitSeconds,
      int loadHistoryWaitSeconds,
      int loadHistoryChunkSize) {

    ProfileService profileService = appContext.getBean(ProfileService.class);
    LoadService loadService = appContext.getBean(LoadService.class);
    ResourceService resourceService = appContext.getBean(ResourceService.class);
    DatasetBucketDao datasetBucketDao = appContext.getBean(DatasetBucketDao.class);
    JobService jobService = appContext.getBean(JobService.class);

    GoogleProjectService projectService = appContext.getBean(GoogleProjectService.class);

    var platform = CloudPlatform.GCP;

    // Verify that the user is allowed to access the bucket where the control file lives
    addStep(new ValidateBucketAccessStep(gcsPdao, userReq));

    // Parse the JSON file and see if there's actually any files to load.
    // If there are no files to load, then SkippableSteps taking the `ingestSkipCondition`
    // will not be run.
    addStep(new IngestJsonFileSetupGcpStep(gcsPdao, appConfig.objectMapper(), dataset));

    // Authorize the billing profile for use.
    addOptionalCombinedIngestStep(
        new AuthorizeBillingProfileUseStep(profileService, profileId, userReq));

    // Lock the load.
    addOptionalCombinedIngestStep(new LoadLockStep(loadService));

    // Get or create a Google project for files to be ingested into.
    addOptionalCombinedIngestStep(new IngestFileGetProjectStep(dataset, projectService));

    // Initialize the Google project for ingest use.
    addOptionalCombinedIngestStep(
        new IngestFileInitializeProjectStep(resourceService, dataset), randomBackoffRetry);

    // Create the bucket within the Google project for files to be ingested into.
    addOptionalCombinedIngestStep(
        new IngestFilePrimaryDataLocationStep(resourceService, dataset), randomBackoffRetry);

    // Make a link between the dataset and bucket in the database.
    addOptionalCombinedIngestStep(
        new IngestFileMakeBucketLinkStep(datasetBucketDao, dataset), randomBackoffRetry);

    // Populate the load table in our database with files to be loaded.
    addOptionalCombinedIngestStep(
        new IngestPopulateFileStateFromFlightMapGcpStep(
            loadService,
            fileService,
            gcsPdao,
            appConfig.objectMapper(),
            dataset,
            appConfig.getLoadFilePopulateBatchSize()));

    // Load the files!
    addOptionalCombinedIngestStep(
        new IngestDriverStep(
            loadService,
            configService,
            jobService,
            dataset.getId().toString(),
            ingestRequest.getLoadTag(),
            Objects.requireNonNullElse(ingestRequest.getMaxBadRecords(), 0),
            driverWaitSeconds,
            profileId,
            platform,
            userReq),
        driverRetry);

    // Create the job result with the results of the bulk file load.
    addOptionalCombinedIngestStep(
        new IngestBulkMapResponseStep(loadService, ingestRequest.getLoadTag()));

    // Create a bucket for the scratch file to be written to.
    addOptionalCombinedIngestStep(
        new CreateBucketForBigQueryScratchStep(resourceService, datasetService),
        randomBackoffRetry);

    // Build the scratch file using new file ids and store in new bucket.
    addOptionalCombinedIngestStep(
        new IngestBuildAndWriteScratchLoadFileGcpStep(appConfig.objectMapper(), gcsPdao, dataset));

    // Copy the load history into BigQuery.
    addOptionalCombinedIngestStep(
        new IngestCopyLoadHistoryToBQStep(
            bigQueryPdao,
            loadService,
            datasetService,
            dataset.getId(),
            ingestRequest.getLoadTag(),
            loadHistoryWaitSeconds,
            loadHistoryChunkSize));

    // Clean up the load table.
    addOptionalCombinedIngestStep(new IngestCleanFileStateStep(loadService));

    // Unlock the load.
    addOptionalCombinedIngestStep(new LoadUnlockStep(loadService));
  }

  private void addAzureJsonSteps(
      ApplicationContext appContext,
      ApplicationConfiguration appConfig,
      DatasetService datasetService,
      AzureBlobStorePdao azureBlobStorePdao,
      ConfigurationService configService,
      FileService fileService,
      IngestRequestModel ingestRequest,
      AuthenticatedUserRequest userReq,
      Dataset dataset,
      UUID profileId,
      RetryRule randomBackoffRetry,
      RetryRule driverRetry,
      int driverWaitSeconds,
      int loadHistoryChunkSize) {

    AzureContainerPdao azureContainerPdao = appContext.getBean(AzureContainerPdao.class);
    LoadService loadService = appContext.getBean(LoadService.class);
    DatasetStorageAccountDao datasetStorageAccountDao =
        appContext.getBean(DatasetStorageAccountDao.class);
    JobService jobService = appContext.getBean(JobService.class);
    StorageTableService storageTableService = appContext.getBean(StorageTableService.class);

    var platform = CloudPlatform.AZURE;

    // Parse the JSON file and see if there's actually any files to load.
    // If there are no files to load, then SkippableSteps taking the `ingestSkipCondition`
    // will not be run.
    addStep(
        new IngestJsonFileSetupAzureStep(appConfig.objectMapper(), azureBlobStorePdao, dataset));

    // Lock the load.
    addOptionalCombinedIngestStep(new LoadLockStep(loadService));

    // Make a link between the Storage Account and Dataset in our database.
    addOptionalCombinedIngestStep(
        new IngestFileAzureMakeStorageAccountLinkStep(datasetStorageAccountDao, dataset),
        randomBackoffRetry);

    // Populate the load table in our database with files to be loaded.
    addOptionalCombinedIngestStep(
        new IngestPopulateFileStateFromFlightMapAzureStep(
            loadService,
            fileService,
            azureBlobStorePdao,
            appConfig.objectMapper(),
            dataset,
            appConfig.getLoadFilePopulateBatchSize()));

    // Load the files!
    addOptionalCombinedIngestStep(
        new IngestDriverStep(
            loadService,
            configService,
            jobService,
            dataset.getId().toString(),
            ingestRequest.getLoadTag(),
            Objects.requireNonNullElse(ingestRequest.getMaxBadRecords(), 0),
            driverWaitSeconds,
            profileId,
            platform,
            userReq),
        driverRetry);

    // Create the job result with the results of the bulk file load.
    addOptionalCombinedIngestStep(
        new IngestBulkMapResponseStep(loadService, ingestRequest.getLoadTag()));

    // Build the scratch file using new file ids and store in new storage account container.
    addOptionalCombinedIngestStep(
        new IngestBuildAndWriteScratchLoadFileAzureStep(
            appConfig.objectMapper(), azureBlobStorePdao, azureContainerPdao, dataset));

    // Copy the load history to Azure Storage Tables.
    addOptionalCombinedIngestStep(
        new IngestCopyLoadHistoryToStorageTableStep(
            storageTableService,
            loadService,
            datasetService,
            dataset.getId(),
            ingestRequest.getLoadTag(),
            loadHistoryChunkSize));

    // Clean up the load table.
    addOptionalCombinedIngestStep(new IngestCleanFileStateStep(loadService));

    // Unlock the load.
    addOptionalCombinedIngestStep(new LoadUnlockStep(loadService));
  }
}
