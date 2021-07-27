package bio.terra.service.dataset.flight.ingest;

import static bio.terra.common.FlightUtils.getDefaultRandomBackoffRetryRule;

import bio.terra.app.configuration.ApplicationConfiguration;
import bio.terra.common.CloudPlatformWrapper;
import bio.terra.model.IngestRequestModel;
import bio.terra.service.configuration.ConfigurationService;
import bio.terra.service.dataset.Dataset;
import bio.terra.service.dataset.DatasetBucketDao;
import bio.terra.service.dataset.DatasetDao;
import bio.terra.service.dataset.DatasetService;
import bio.terra.service.dataset.flight.LockDatasetStep;
import bio.terra.service.dataset.flight.UnlockDatasetStep;
import bio.terra.service.filedata.flight.ingest.IngestBuildLoadFileStep;
import bio.terra.service.filedata.flight.ingest.IngestCleanFileStateStep;
import bio.terra.service.filedata.flight.ingest.IngestCopyLoadHistoryToBQStep;
import bio.terra.service.filedata.flight.ingest.IngestCreateBucketForScratchFileStep;
import bio.terra.service.filedata.flight.ingest.IngestCreateScratchFileStep;
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
import bio.terra.service.resourcemanagement.google.GoogleProjectService;
import bio.terra.service.tabulardata.google.BigQueryPdao;
import bio.terra.stairway.Flight;
import bio.terra.stairway.FlightMap;
import bio.terra.stairway.RetryRule;
import bio.terra.stairway.RetryRuleExponentialBackoff;
import java.util.Optional;
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
    GcsPdao gcsPdao = appContext.getBean(GcsPdao.class);
    FireStoreDao fileDao = appContext.getBean(FireStoreDao.class);
    ConfigurationService configService = appContext.getBean(ConfigurationService.class);
    ApplicationConfiguration appConfig = appContext.getBean(ApplicationConfiguration.class);
    ProfileService profileService = appContext.getBean(ProfileService.class);
    LoadService loadService = appContext.getBean(LoadService.class);
    ResourceService resourceService = appContext.getBean(ResourceService.class);
    DatasetBucketDao datasetBucketDao = appContext.getBean(DatasetBucketDao.class);
    ConfigurationService configurationService = appContext.getBean(ConfigurationService.class);
    JobService jobService = appContext.getBean(JobService.class);
    GoogleProjectService projectService = appContext.getBean(GoogleProjectService.class);

    // get data from inputs that steps need
    UUID datasetId =
        UUID.fromString(inputParameters.get(JobMapKeys.DATASET_ID.getKeyName(), String.class));
    Dataset dataset = datasetService.retrieve(datasetId);

    RetryRule lockDatasetRetry =
        getDefaultRandomBackoffRetryRule(appConfig.getMaxStairwayThreads());

    AuthenticatedUserRequest userReq =
        inputParameters.get(JobMapKeys.AUTH_USER_INFO.getKeyName(), AuthenticatedUserRequest.class);

    IngestRequestModel ingestRequest =
        inputParameters.get(JobMapKeys.REQUEST.getKeyName(), IngestRequestModel.class);

    RetryRule randomBackoffRetry =
        getDefaultRandomBackoffRetryRule(appConfig.getMaxStairwayThreads());
    RetryRule driverRetry = new RetryRuleExponentialBackoff(5, 20, 600);

    var platform = CloudPlatformWrapper.of(dataset.getDatasetSummary().getStorageCloudPlatform());

    var profileId =
        Optional.ofNullable(ingestRequest.getProfileId()).orElse(dataset.getDefaultProfileId());

    int driverWaitSeconds = inputParameters.get(LoadMapKeys.DRIVER_WAIT_SECONDS, Integer.class);
    int loadHistoryWaitSeconds =
        inputParameters.get(LoadMapKeys.LOAD_HISTORY_WAIT_SECONDS, Integer.class);
    int fileChunkSize =
        inputParameters.get(LoadMapKeys.LOAD_HISTORY_COPY_CHUNK_SIZE, Integer.class);

    String loadTag = inputParameters.get(LoadMapKeys.LOAD_TAG, String.class);

    addStep(new LockDatasetStep(datasetDao, datasetId, true), lockDatasetRetry);
    addStep(new IngestSetupStep(datasetService, configService));

    if (ingestRequest.getFormat() == IngestRequestModel.FormatEnum.JSON) {

      // Begin file + metadata load
      addStep(new IngestParseJsonFileStep(gcsPdao, appConfig.objectMapper(), dataset));
      addStep(
          new AuthorizeBillingProfileUseStep(
              profileService, profileId, userReq, IngestUtils.noFilesToIngestPredicate()));
      addStep(new LoadLockStep(loadService, IngestUtils.noFilesToIngestPredicate()));
      addStep(
          new IngestFileGetProjectStep(
              dataset, projectService, IngestUtils.noFilesToIngestPredicate()));
      addStep(
          new IngestFileGetOrCreateProject(
              resourceService, dataset, IngestUtils.noFilesToIngestPredicate()),
          randomBackoffRetry);
      addStep(
          new IngestFilePrimaryDataLocationStep(
              resourceService, dataset, IngestUtils.noFilesToIngestPredicate()),
          randomBackoffRetry);
      addStep(
          new IngestFileMakeBucketLinkStep(
              datasetBucketDao, dataset, IngestUtils.noFilesToIngestPredicate()),
          randomBackoffRetry);
      addStep(
          new IngestPopulateFileStateFromFlightMapStep(
              loadService, IngestUtils.noFilesToIngestPredicate()));
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
              IngestUtils.noFilesToIngestPredicate()),
          driverRetry);

      addStep(
          new IngestBulkMapResponseStep(
              loadService, ingestRequest.getLoadTag(), IngestUtils.noFilesToIngestPredicate()));
      // build the scratch file using new file ids and store in new bucket
      addStep(
          new IngestBuildLoadFileStep(
              appConfig.objectMapper(), IngestUtils.noFilesToIngestPredicate()));
      addStep(
          new IngestCreateBucketForScratchFileStep(
              resourceService, dataset, IngestUtils.noFilesToIngestPredicate()));
      addStep(new IngestCreateScratchFileStep(gcsPdao, IngestUtils.noFilesToIngestPredicate()));
      addStep(
          new IngestCopyLoadHistoryToBQStep(
              loadService,
              datasetService,
              loadTag,
              datasetId.toString(),
              bigQueryPdao,
              fileChunkSize,
              loadHistoryWaitSeconds,
              IngestUtils.noFilesToIngestPredicate()));
      addStep(new IngestCleanFileStateStep(loadService, IngestUtils.noFilesToIngestPredicate()));

      addStep(new LoadUnlockStep(loadService, IngestUtils.noFilesToIngestPredicate()));
      // End file + metadata load
    }

    // handing in the load scratch file
    addStep(new IngestLoadTableStep(datasetService, bigQueryPdao));
    addStep(new IngestRowIdsStep(datasetService, bigQueryPdao));
    addStep(new IngestValidateRefsStep(datasetService, bigQueryPdao, fileDao));
    addStep(new IngestInsertIntoDatasetTableStep(datasetService, bigQueryPdao));
    addStep(new IngestCleanupStep(datasetService, bigQueryPdao));
    addStep(new UnlockDatasetStep(datasetDao, datasetId, true), lockDatasetRetry);
  }
}
