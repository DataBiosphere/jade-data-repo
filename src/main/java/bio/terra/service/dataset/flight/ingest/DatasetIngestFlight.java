package bio.terra.service.dataset.flight.ingest;

import static bio.terra.common.FlightUtils.getDefaultRandomBackoffRetryRule;

import bio.terra.app.configuration.ApplicationConfiguration;
import bio.terra.common.CloudPlatformWrapper;
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
import bio.terra.service.filedata.azure.AzureSynapsePdao;
import bio.terra.service.filedata.azure.blobstore.AzureBlobStorePdao;
import bio.terra.service.filedata.azure.tables.TableDao;
import bio.terra.service.filedata.azure.tables.TableDirectoryDao;
import bio.terra.service.filedata.flight.ingest.IngestBuildAndWriteScratchLoadFileAzureStep;
import bio.terra.service.filedata.flight.ingest.IngestBuildAndWriteScratchLoadFileGcpStep;
import bio.terra.service.filedata.flight.ingest.IngestCleanFileStateStep;
import bio.terra.service.filedata.flight.ingest.IngestCopyLoadHistoryToBQStep;
import bio.terra.service.filedata.flight.ingest.IngestCreateBucketForScratchFileStep;
import bio.terra.service.filedata.flight.ingest.IngestDriverStep;
import bio.terra.service.filedata.flight.ingest.IngestFileAzureMakeStorageAccountLinkStep;
import bio.terra.service.filedata.flight.ingest.IngestFileAzurePrimaryDataLocationStep;
import bio.terra.service.filedata.flight.ingest.IngestFileGetOrCreateProject;
import bio.terra.service.filedata.flight.ingest.IngestFileGetProjectStep;
import bio.terra.service.filedata.flight.ingest.IngestFileMakeBucketLinkStep;
import bio.terra.service.filedata.flight.ingest.IngestFilePrimaryDataLocationStep;
import bio.terra.service.filedata.flight.ingest.ValidateIngestFileAzureDirectoryStep;
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
import bio.terra.service.resourcemanagement.google.GoogleProjectService;
import bio.terra.service.tabulardata.google.BigQueryPdao;
import bio.terra.stairway.Flight;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.FlightMap;
import bio.terra.stairway.RetryRule;
import bio.terra.stairway.RetryRuleExponentialBackoff;

import java.util.Objects;
import java.util.UUID;
import java.util.function.Predicate;
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

    if (cloudPlatform.is(CloudPlatform.AZURE)) {
      addStep(
          new AuthorizeBillingProfileUseStep(
              profileService, ingestRequestModel.getProfileId(), userReq));
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
      Predicate<FlightContext> ingestSkipCondition = IngestUtils.noFilesToIngest;

      Dataset dataset = datasetService.retrieve(datasetId);
      var profileId =
          Objects.requireNonNullElse(ingestRequestModel.getProfileId(), dataset.getDefaultProfileId());

      if (cloudPlatform.isGcp()) {
        addGcpJsonSteps(
            appContext,
            appConfig,
            datasetService,
            bigQueryPdao,
            configService,
            ingestRequestModel,
            userReq,
            dataset,
            profileId,
            randomBackoffRetry,
            driverRetry,
            driverWaitSeconds,
            loadHistoryWaitSeconds,
            loadHistoryChunkSize,
            ingestSkipCondition);
      } else if (cloudPlatform.isAzure()) {
        addAzureJsonSteps(
            inputParameters,
            appContext,
            appConfig,
            datasetService,
            ingestRequestModel,
            dataset,
            profileId,
            driverWaitSeconds,
            loadHistoryWaitSeconds,
            loadHistoryChunkSize);
      }
    }

    if (cloudPlatform.is(CloudPlatform.GCP)) {
      addStep(new IngestLoadTableStep(datasetService, bigQueryPdao));
      addStep(new IngestRowIdsStep(datasetService, bigQueryPdao));
      addStep(new IngestValidateGcpRefsStep(datasetService, bigQueryPdao, fileDao));
      addStep(new IngestInsertIntoDatasetTableStep(datasetService, bigQueryPdao));
      addStep(new IngestCleanupStep(datasetService, bigQueryPdao));
    } else if (cloudPlatform.is(CloudPlatform.AZURE)) {
      addStep(new IngestCreateIngestRequestDataSourceStep(azureSynapsePdao, azureBlobStorePdao));
      addStep(
          new IngestCreateTargetDataSourceStep(
              azureSynapsePdao, azureBlobStorePdao, datasetService, resourceService));
      addStep(new IngestCreateParquetFilesStep(azureSynapsePdao, datasetService));
      addStep(
          new IngestValidateAzureRefsStep(
              azureAuthService, datasetService, azureSynapsePdao, tableDirectoryDao));
      addStep(new IngestCleanSynapseStep(azureSynapsePdao));
    }
    addStep(new UnlockDatasetStep(datasetDao, datasetId, true), lockDatasetRetry);
  }

  /**
   * These are the steps needed for combined metadata + file ingest. This only works with JSON
   * formatted requests, so no CSV parsing should happen here.
   */
  private void addGcpJsonSteps(
      ApplicationContext appContext,
      ApplicationConfiguration appConfig,
      DatasetService datasetService,
      BigQueryPdao bigQueryPdao,
      ConfigurationService configService,
      IngestRequestModel ingestRequest,
      AuthenticatedUserRequest userReq,
      Dataset dataset,
      UUID profileId,
      RetryRule randomBackoffRetry,
      RetryRule driverRetry,
      int driverWaitSeconds,
      int loadHistoryWaitSeconds,
      int loadHistoryChunkSize,
      Predicate<FlightContext> ingestSkipCondition) {

    GcsPdao gcsPdao = appContext.getBean(GcsPdao.class);
    ProfileService profileService = appContext.getBean(ProfileService.class);
    LoadService loadService = appContext.getBean(LoadService.class);
    ResourceService resourceService = appContext.getBean(ResourceService.class);
    DatasetBucketDao datasetBucketDao = appContext.getBean(DatasetBucketDao.class);
    JobService jobService = appContext.getBean(JobService.class);

    GoogleProjectService projectService = appContext.getBean(GoogleProjectService.class);

    var platform = CloudPlatform.GCP;

    // Begin file + metadata load
    addStep(new IngestJsonFileSetupGcpStep(gcsPdao, appConfig.objectMapper(), dataset));
    addStep(
        new AuthorizeBillingProfileUseStep(
            profileService, profileId, userReq, ingestSkipCondition));
    addStep(new LoadLockStep(loadService, ingestSkipCondition));
    addStep(new IngestFileGetProjectStep(dataset, projectService, ingestSkipCondition));
    addStep(
        new IngestFileGetOrCreateProject(resourceService, dataset, ingestSkipCondition),
        randomBackoffRetry);
    addStep(
        new IngestFilePrimaryDataLocationStep(resourceService, dataset, ingestSkipCondition),
        randomBackoffRetry);
    addStep(
        new IngestFileMakeBucketLinkStep(datasetBucketDao, dataset, ingestSkipCondition),
        randomBackoffRetry);
    addStep(
        new IngestPopulateFileStateFromFlightMapStep(
            loadService,
            gcsPdao,
            appConfig.objectMapper(),
            dataset,
            appConfig.getLoadFilePopulateBatchSize(),
            ingestSkipCondition));
    addStep(
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
            ingestSkipCondition),
        driverRetry);

    addStep(
        new IngestBulkMapResponseStep(
            loadService, ingestRequest.getLoadTag(), ingestSkipCondition));

    addStep(
        new IngestCreateBucketForScratchFileStep(resourceService, dataset, ingestSkipCondition));

    // build the scratch file using new file ids and store in new bucket
    addStep(
        new IngestBuildAndWriteScratchLoadFileGcpStep(
            appConfig.objectMapper(), gcsPdao, dataset, ingestSkipCondition));

    addStep(
        new IngestCopyLoadHistoryToBQStep(
            bigQueryPdao,
            loadService,
            datasetService,
            dataset.getId(),
            ingestRequest.getLoadTag(),
            loadHistoryWaitSeconds,
            loadHistoryChunkSize,
            ingestSkipCondition));
    addStep(new IngestCleanFileStateStep(loadService, ingestSkipCondition));

    addStep(new LoadUnlockStep(loadService, ingestSkipCondition));
  }

  private void addAzureJsonSteps(
      FlightMap inputParameters,
      ApplicationContext appContext,
      ApplicationConfiguration appConfig,
      DatasetService datasetService,
      IngestRequestModel ingestRequest,
      Dataset dataset,
      UUID profileId,
      int driverWaitSeconds,
      int loadHistoryWaitSeconds,
      int loadHistoryChunkSize) {


    AzureBlobStorePdao azureBlobStorePdao = appContext.getBean(AzureBlobStorePdao.class);
    TableDao azureTableDao = appContext.getBean(TableDao.class);
    ProfileService profileService = appContext.getBean(ProfileService.class);
    LoadService loadService = appContext.getBean(LoadService.class);
    ResourceService resourceService = appContext.getBean(ResourceService.class);
    DatasetStorageAccountDao datasetStorageAccountDao = appContext.getBean(DatasetStorageAccountDao.class);
    ConfigurationService configurationService = appContext.getBean(ConfigurationService.class);
    JobService jobService = appContext.getBean(JobService.class);

    AuthenticatedUserRequest userReq =
        inputParameters.get(JobMapKeys.AUTH_USER_INFO.getKeyName(), AuthenticatedUserRequest.class);
    RetryRule randomBackoffRetry =
        getDefaultRandomBackoffRetryRule(appConfig.getMaxStairwayThreads());
    RetryRule driverRetry = new RetryRuleExponentialBackoff(5, 20, 600);
    Predicate<FlightContext> ingestSkipCondition = IngestUtils.noFilesToIngest;

    addStep(
        new IngestJsonFileSetupAzureStep(appConfig.objectMapper(), azureBlobStorePdao, dataset));

    // TODO: This calls `GoogleBillingService` under the hood. What do we need to do for Azure?
    addStep(
        new AuthorizeBillingProfileUseStep(
            profileService, profileId, userReq, ingestSkipCondition));
    addStep(new LoadLockStep(loadService, ingestSkipCondition));

    addStep(
        new IngestFileAzurePrimaryDataLocationStep(resourceService, dataset, ingestSkipCondition), randomBackoffRetry);
    addStep(
        new IngestFileAzureMakeStorageAccountLinkStep(datasetStorageAccountDao, dataset, ingestSkipCondition),
        randomBackoffRetry);
    addStep(new ValidateIngestFileAzureDirectoryStep(azureTableDao, dataset, ingestSkipCondition));
    addStep(
        new IngestFileAzurePrimaryDataLocationStep(resourceService, dataset), randomBackoffRetry);
    addStep(
        new IngestFileAzureMakeStorageAccountLinkStep(datasetStorageAccountDao, dataset),
        randomBackoffRetry);

      //TODO: Waiting for DR-1962 to merge with ingest file support for Azure
//    addStep(
//        new IngestPopulateFileStateFromFlightMapStep(
//            loadService,
//            gcsPdao,
//            appConfig.objectMapper(),
//            dataset,
//            appConfig.getLoadFilePopulateBatchSize(),
//            ingestSkipCondition));
//    addStep(
//        new IngestDriverStep(
//            loadService,
//            configService,
//            jobService,
//            dataset.getId().toString(),
//            loadTag,
//            Objects.requireNonNullElse(ingestRequest.getMaxBadRecords(), 0),
//            driverWaitSeconds,
//            profileId,
//            platform,
//            ingestSkipCondition),
//        driverRetry);

    addStep(
        new IngestBulkMapResponseStep(
            loadService, ingestRequest.getLoadTag(), ingestSkipCondition));

    addStep(
        new IngestCreateBucketForScratchFileStep(resourceService, dataset, ingestSkipCondition));

    // build the scratch file using new file ids and store in new bucket
    addStep(
        new IngestBuildAndWriteScratchLoadFileAzureStep(
            appConfig.objectMapper(), azureBlobStorePdao, dataset, ingestSkipCondition));

    addStep(
        new IngestCopyLoadHistoryToBQStep(
            bigQueryPdao,
            loadService,
            datasetService,
            dataset.getId(),
            ingestRequest.getLoadTag(),
            loadHistoryWaitSeconds,
            loadHistoryChunkSize,
            ingestSkipCondition));
    addStep(new IngestCleanFileStateStep(loadService, ingestSkipCondition));

    addStep(new LoadUnlockStep(loadService, ingestSkipCondition));
  }
}
