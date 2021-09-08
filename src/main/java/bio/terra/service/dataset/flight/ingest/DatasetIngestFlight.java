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
import bio.terra.service.dataset.flight.LockDatasetStep;
import bio.terra.service.dataset.flight.UnlockDatasetStep;
import bio.terra.service.filedata.azure.AzureSynapsePdao;
import bio.terra.service.filedata.azure.tables.TableDirectoryDao;
import bio.terra.service.filedata.flight.ingest.IngestBuildAndWriteScratchLoadFileStep;
import bio.terra.service.filedata.flight.ingest.IngestCleanFileStateStep;
import bio.terra.service.filedata.flight.ingest.IngestCopyLoadHistoryToBQStep;
import bio.terra.service.filedata.flight.ingest.IngestCreateBucketForScratchFileStep;
import bio.terra.service.filedata.flight.ingest.IngestDriverStep;
import bio.terra.service.filedata.flight.ingest.IngestFileGetOrCreateProject;
import bio.terra.service.filedata.flight.ingest.IngestFileGetProjectStep;
import bio.terra.service.filedata.flight.ingest.IngestFileMakeBucketLinkStep;
import bio.terra.service.filedata.flight.ingest.IngestFilePrimaryDataLocationStep;
import bio.terra.service.filedata.google.firestore.FireStoreDao;
import bio.terra.service.filedata.google.gcs.GcsPdao;
import bio.terra.service.iam.AuthenticatedUserRequest;
import bio.terra.service.job.JobMapKeys;
import bio.terra.service.job.JobService;
import bio.terra.service.load.LoadService;
import bio.terra.service.load.flight.LoadLockStep;
import bio.terra.service.load.flight.LoadMapKeys;
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
import java.util.Optional;
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

    if (ingestRequestModel.getFormat() == IngestRequestModel.FormatEnum.JSON
        && cloudPlatform.isGcp()) {
      addJsonSteps(
          inputParameters,
          appContext,
          appConfig,
          configService,
          datasetService,
          bigQueryPdao,
          ingestRequestModel,
          datasetId);
    }

    if (cloudPlatform.is(CloudPlatform.GCP)) {
      addStep(new IngestLoadTableStep(datasetService, bigQueryPdao));
      addStep(new IngestRowIdsStep(datasetService, bigQueryPdao));
      addStep(new IngestValidateGcpRefsStep(datasetService, bigQueryPdao, fileDao));
      addStep(new IngestInsertIntoDatasetTableStep(datasetService, bigQueryPdao));
      addStep(new IngestCleanupStep(datasetService, bigQueryPdao));
    } else if (cloudPlatform.is(CloudPlatform.AZURE)) {
      addStep(new IngestCreateIngestRequestDataSourceStep(azureSynapsePdao));
      addStep(
          new IngestCreateTargetDataSourceStep(azureSynapsePdao, datasetService, resourceService));
      addStep(new IngestCreateParquetFilesStep(azureSynapsePdao, datasetService));
      addStep(
          new IngestValidateAzureRefsStep(
              resourceService,
              azureAuthService,
              datasetService,
              azureSynapsePdao,
              tableDirectoryDao));
      addStep(new IngestCleanSynapseStep(azureSynapsePdao));
    }
    addStep(new UnlockDatasetStep(datasetDao, datasetId, true), lockDatasetRetry);
  }

  /**
   * These are the steps needed for combined metadata + file ingest. This only works with JSON
   * formatted requests, so no CSV parsing should happen here.
   */
  private void addJsonSteps(
      FlightMap inputParameters,
      ApplicationContext appContext,
      ApplicationConfiguration appConfig,
      ConfigurationService configService,
      DatasetService datasetService,
      BigQueryPdao bigQueryPdao,
      IngestRequestModel ingestRequest,
      UUID datasetId) {

    GcsPdao gcsPdao = appContext.getBean(GcsPdao.class);
    ProfileService profileService = appContext.getBean(ProfileService.class);
    LoadService loadService = appContext.getBean(LoadService.class);
    ResourceService resourceService = appContext.getBean(ResourceService.class);
    DatasetBucketDao datasetBucketDao = appContext.getBean(DatasetBucketDao.class);
    ConfigurationService configurationService = appContext.getBean(ConfigurationService.class);
    JobService jobService = appContext.getBean(JobService.class);

    GoogleProjectService projectService = appContext.getBean(GoogleProjectService.class);
    AuthenticatedUserRequest userReq =
        inputParameters.get(JobMapKeys.AUTH_USER_INFO.getKeyName(), AuthenticatedUserRequest.class);
    RetryRule randomBackoffRetry =
        getDefaultRandomBackoffRetryRule(appConfig.getMaxStairwayThreads());
    RetryRule driverRetry = new RetryRuleExponentialBackoff(5, 20, 600);
    Predicate<FlightContext> ingestSkipCondition = IngestUtils.noFilesToIngest;

    Dataset dataset = datasetService.retrieve(datasetId);

    var platform = CloudPlatformWrapper.of(dataset.getDatasetSummary().getStorageCloudPlatform());

    var profileId =
        Optional.ofNullable(ingestRequest.getProfileId()).orElse(dataset.getDefaultProfileId());

    int driverWaitSeconds = configService.getParameterValue(ConfigEnum.LOAD_DRIVER_WAIT_SECONDS);
    int loadHistoryWaitSeconds =
        configService.getParameterValue(ConfigEnum.LOAD_HISTORY_WAIT_SECONDS);
    int loadHistoryChunkSize =
        configService.getParameterValue(ConfigEnum.LOAD_HISTORY_COPY_CHUNK_SIZE);

    String loadTag = inputParameters.get(LoadMapKeys.LOAD_TAG, String.class);
    // Begin file + metadata load
    addStep(
        new IngestJsonFileSetupStep(
            gcsPdao, appConfig.objectMapper(), dataset, configurationService));
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
            configurationService,
            jobService,
            datasetId.toString(),
            loadTag,
            Optional.ofNullable(ingestRequest.getMaxBadRecords()).orElse(0),
            driverWaitSeconds,
            profileId,
            platform.getCloudPlatform(),
            ingestSkipCondition),
        driverRetry);

    addStep(
        new IngestBulkMapResponseStep(
            loadService, ingestRequest.getLoadTag(), ingestSkipCondition));

    addStep(
        new IngestCreateBucketForScratchFileStep(resourceService, dataset, ingestSkipCondition));

    // build the scratch file using new file ids and store in new bucket
    addStep(
        new IngestBuildAndWriteScratchLoadFileStep(
            appConfig.objectMapper(), gcsPdao, dataset, ingestSkipCondition));

    addStep(
        new IngestCopyLoadHistoryToBQStep(
            bigQueryPdao,
            loadService,
            datasetService,
            datasetId,
            loadTag,
            loadHistoryWaitSeconds,
            loadHistoryChunkSize,
            ingestSkipCondition));
    addStep(new IngestCleanFileStateStep(loadService, ingestSkipCondition));

    addStep(new LoadUnlockStep(loadService, ingestSkipCondition));
  }
}
