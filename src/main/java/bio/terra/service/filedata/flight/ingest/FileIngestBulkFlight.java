package bio.terra.service.filedata.flight.ingest;

import static bio.terra.common.FlightUtils.getDefaultRandomBackoffRetryRule;

import bio.terra.app.configuration.ApplicationConfiguration;
import bio.terra.common.CloudPlatformWrapper;
import bio.terra.common.ValidateBucketAccessStep;
import bio.terra.common.iam.AuthenticatedUserRequest;
import bio.terra.model.BulkLoadArrayRequestModel;
import bio.terra.model.BulkLoadRequestModel;
import bio.terra.service.auth.iam.IamService;
import bio.terra.service.configuration.ConfigurationService;
import bio.terra.service.dataset.Dataset;
import bio.terra.service.dataset.DatasetBucketDao;
import bio.terra.service.dataset.DatasetService;
import bio.terra.service.dataset.DatasetStorageAccountDao;
import bio.terra.service.dataset.flight.LockDatasetStep;
import bio.terra.service.dataset.flight.UnlockDatasetStep;
import bio.terra.service.filedata.azure.blobstore.AzureBlobStorePdao;
import bio.terra.service.filedata.google.gcs.GcsPdao;
import bio.terra.service.job.JobMapKeys;
import bio.terra.service.job.JobService;
import bio.terra.service.load.LoadService;
import bio.terra.service.load.flight.LoadLockStep;
import bio.terra.service.load.flight.LoadMapKeys;
import bio.terra.service.load.flight.LoadUnlockStep;
import bio.terra.service.profile.ProfileService;
import bio.terra.service.profile.flight.AuthorizeBillingProfileUseStep;
import bio.terra.service.profile.flight.VerifyBillingAccountAccessStep;
import bio.terra.service.profile.google.GoogleBillingService;
import bio.terra.service.resourcemanagement.ResourceService;
import bio.terra.service.resourcemanagement.google.GoogleProjectService;
import bio.terra.service.tabulardata.azure.StorageTableService;
import bio.terra.service.tabulardata.google.bigquery.BigQueryDatasetPdao;
import bio.terra.stairway.Flight;
import bio.terra.stairway.FlightMap;
import bio.terra.stairway.RetryRule;
import bio.terra.stairway.RetryRuleExponentialBackoff;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import org.springframework.context.ApplicationContext;

/*
 * Required input parameters:
 * - DATASET_ID dataset we are loading into
 * - LOAD_TAG is the load tag for this ingest
 * - CONCURRENT_INGESTS is the number of ingests to allow to run in parallel
 * - CONCURRENT_FILES is the number of file loads to run in parallel
 * - REQUEST is a BulkLoadRequestModel or BulkLoadArrayRequestModel
 * - IS_ARRAY boolean, true if this is a bulk load array version
 */

public class FileIngestBulkFlight extends Flight {

  public FileIngestBulkFlight(FlightMap inputParameters, Object applicationContext) {

    super(inputParameters, applicationContext);

    ApplicationContext appContext = (ApplicationContext) applicationContext;
    LoadService loadService = appContext.getBean(LoadService.class);
    ApplicationConfiguration appConfig = appContext.getBean(ApplicationConfiguration.class);
    ResourceService resourceService = appContext.getBean(ResourceService.class);
    BigQueryDatasetPdao bigQueryDatasetPdao = appContext.getBean(BigQueryDatasetPdao.class);
    DatasetService datasetService = appContext.getBean(DatasetService.class);
    ConfigurationService configurationService = appContext.getBean(ConfigurationService.class);
    JobService jobService = appContext.getBean(JobService.class);
    GcsPdao gcsPdao = appContext.getBean(GcsPdao.class);
    ProfileService profileService = appContext.getBean(ProfileService.class);
    IamService iamService = appContext.getBean(IamService.class);
    DatasetBucketDao datasetBucketDao = appContext.getBean(DatasetBucketDao.class);
    DatasetStorageAccountDao datasetStorageAccountDao =
        appContext.getBean(DatasetStorageAccountDao.class);
    GoogleProjectService googleProjectService = appContext.getBean(GoogleProjectService.class);
    GoogleBillingService googleBillingService = appContext.getBean(GoogleBillingService.class);
    StorageTableService storageTableService = appContext.getBean(StorageTableService.class);
    AzureBlobStorePdao azureBlobStorePdao = appContext.getBean(AzureBlobStorePdao.class);
    ExecutorService executor = appContext.getBean("performanceThreadpool", ExecutorService.class);
    ObjectMapper bulkLoadObjectMapper = appConfig.bulkLoadObjectMapper();

    // Common input parameters
    String datasetId = inputParameters.get(JobMapKeys.DATASET_ID.getKeyName(), String.class);
    UUID datasetUuid = UUID.fromString(datasetId);
    Dataset dataset = datasetService.retrieve(datasetUuid);

    var platform = CloudPlatformWrapper.of(dataset.getDatasetSummary().getStorageCloudPlatform());

    String loadTag = inputParameters.get(LoadMapKeys.LOAD_TAG, String.class);
    int driverWaitSeconds = inputParameters.get(LoadMapKeys.DRIVER_WAIT_SECONDS, Integer.class);
    int loadHistoryWaitSeconds =
        inputParameters.get(LoadMapKeys.LOAD_HISTORY_WAIT_SECONDS, Integer.class);
    int loadHistoryChunkSize =
        inputParameters.get(LoadMapKeys.LOAD_HISTORY_COPY_CHUNK_SIZE, Integer.class);
    boolean isArray = inputParameters.get(LoadMapKeys.IS_ARRAY, Boolean.class);
    AuthenticatedUserRequest userReq =
        inputParameters.get(JobMapKeys.AUTH_USER_INFO.getKeyName(), AuthenticatedUserRequest.class);

    // TODO: for reserving a bulk load slot:
    //    int concurrentIngests = inputParameters.get(LoadMapKeys.CONCURRENT_INGESTS,
    // Integer.class);
    //  We can maybe just use the load tag lock table to know how many are active.

    // Parameters dependent on which request we get
    int maxFailedFileLoads;
    UUID profileId;

    if (isArray) {
      BulkLoadArrayRequestModel loadRequest =
          inputParameters.get(JobMapKeys.REQUEST.getKeyName(), BulkLoadArrayRequestModel.class);
      maxFailedFileLoads = loadRequest.getMaxFailedFileLoads();
      profileId = loadRequest.getProfileId();
    } else {
      BulkLoadRequestModel loadRequest =
          inputParameters.get(JobMapKeys.REQUEST.getKeyName(), BulkLoadRequestModel.class);
      maxFailedFileLoads = loadRequest.getMaxFailedFileLoads();
      profileId = loadRequest.getProfileId();
    }

    RetryRule randomBackoffRetry =
        getDefaultRandomBackoffRetryRule(appConfig.getMaxStairwayThreads());
    RetryRule driverRetry = new RetryRuleExponentialBackoff(5, 20, 600);

    // The flight plan:
    // 0. Make sure this user is allowed to use the billing profile and that the underlying
    //    billing information remains valid.
    // 1. Lock the load tag - only one flight operating on a load tag at a time
    // 2. TODO: reserve a bulk load slot to make sure we have the threads to do the flight; abort
    //    otherwise (DR-754)
    // 3. Locate the bucket where this file should go and store it in the working map. We need to
    //    make the decision about where we will put the file and remember it persistently in the
    //    working map before we copy the file in. That allows the copy undo to know the location to
    //    look at to delete the file. We do this once here and pass the information into the worker
    //    flight. We also need to know the project that contains the bucket. It will be charged for
    //    the copying if the source file listing the files to load is in a requester pay bucket.
    // 4. Depends on the request type:
    //    a. isArray - put the array into the load_file table for processing
    //    b. !isArray - read the file into the load_file table for processing
    // 5. Main loading loop - shared with bulk ingest from a file in a bucket
    // 6. Depends on request type:
    //    a. isArray - generate the bulk array response: summary and array of results
    //    b. !isArray - generate the bulk file response - just the summary information
    // 7. TODO: Copy results into the database BigQuery (DR-694)
    // 8. Clean load_file table
    // 9. TODO: release the bulk load slot (DR-754) - may not need a step if we use the count of
    //    locked tags
    // 10. Unlock the load tag
    addStep(new AuthorizeBillingProfileUseStep(profileService, profileId, userReq));
    // For Azure datasets, the billing profile for file ingest must match the default
    if (platform.isAzure()) {
      addStep(new IngestFileValidateAzureBillingProfileStep(profileId, dataset));
    }
    addStep(new IngestFileValidateCloudPlatformStep(dataset));
    addStep(new LockDatasetStep(datasetService, datasetUuid, true), randomBackoffRetry);
    addStep(new LoadLockStep(loadService));
    if (platform.isGcp()) {
      addStep(new VerifyBillingAccountAccessStep(googleBillingService));
      if (!dataset.isSelfHosted()) {
        addStep(new IngestFileGetProjectStep(dataset, googleProjectService));
        addStep(new IngestFileInitializeProjectStep(resourceService, dataset), randomBackoffRetry);
        addStep(
            new IngestFilePrimaryDataLocationStep(userReq, resourceService, dataset, iamService),
            randomBackoffRetry);
        addStep(new IngestFileMakeBucketLinkStep(datasetBucketDao, dataset), randomBackoffRetry);
      }
    } else if (platform.isAzure()) {
      addStep(
          new IngestFileAzurePrimaryDataLocationStep(resourceService, dataset), randomBackoffRetry);
      addStep(
          new IngestFileAzureMakeStorageAccountLinkStep(datasetStorageAccountDao, dataset),
          randomBackoffRetry);
    }

    if (isArray) {
      addStep(new IngestPopulateFileStateFromArrayStep(loadService));
    } else {
      if (platform.isGcp()) {
        addStep(
            new ValidateBucketAccessStep(
                gcsPdao, dataset.getProjectResource().getGoogleProjectId(), userReq));
        addStep(
            new IngestPopulateFileStateFromFileGcpStep(
                loadService,
                appConfig.getMaxBadLoadFileLineErrorsReported(),
                appConfig.getLoadFilePopulateBatchSize(),
                gcsPdao,
                bulkLoadObjectMapper,
                executor,
                userReq));
      } else {
        addStep(
            new IngestPopulateFileStateFromFileAzureStep(
                loadService,
                appConfig.getMaxBadLoadFileLineErrorsReported(),
                appConfig.getLoadFilePopulateBatchSize(),
                azureBlobStorePdao,
                bulkLoadObjectMapper,
                executor,
                userReq));
      }
    }
    addStep(
        new IngestDriverStep(
            loadService,
            configurationService,
            jobService,
            datasetId,
            loadTag,
            maxFailedFileLoads,
            driverWaitSeconds,
            profileId,
            platform.getCloudPlatform(),
            userReq),
        driverRetry);

    if (isArray) {
      addStep(new IngestBulkArrayResponseStep(loadService, loadTag));
    } else {
      addStep(new IngestBulkFileResponseStep(loadService, loadTag));
    }

    if (platform.isGcp()) {
      addStep(
          new IngestCopyLoadHistoryToBQStep(
              bigQueryDatasetPdao,
              loadService,
              datasetService,
              datasetUuid,
              loadTag,
              loadHistoryWaitSeconds,
              loadHistoryChunkSize));
    } else if (platform.isAzure()) {
      addStep(
          new IngestCopyLoadHistoryToStorageTableStep(
              storageTableService,
              loadService,
              datasetService,
              datasetUuid,
              loadTag,
              loadHistoryChunkSize),
          randomBackoffRetry);
    }
    addStep(new IngestCleanFileStateStep(loadService));

    addStep(new LoadUnlockStep(loadService));
    addStep(new UnlockDatasetStep(datasetService, datasetUuid, true), randomBackoffRetry);
  }
}
