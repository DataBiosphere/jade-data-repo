package bio.terra.service.filedata.flight.ingest;

import static bio.terra.common.FlightUtils.getDefaultExponentialBackoffRetryRule;
import static bio.terra.common.FlightUtils.getDefaultRandomBackoffRetryRule;

import bio.terra.app.configuration.ApplicationConfiguration;
import bio.terra.common.CloudPlatformWrapper;
import bio.terra.common.ValidateBucketAccessStep;
import bio.terra.common.iam.AuthenticatedUserRequest;
import bio.terra.model.FileLoadModel;
import bio.terra.service.auth.iam.IamService;
import bio.terra.service.configuration.ConfigurationService;
import bio.terra.service.dataset.Dataset;
import bio.terra.service.dataset.DatasetBucketDao;
import bio.terra.service.dataset.DatasetService;
import bio.terra.service.dataset.DatasetStorageAccountDao;
import bio.terra.service.dataset.flight.LockDatasetStep;
import bio.terra.service.dataset.flight.UnlockDatasetStep;
import bio.terra.service.filedata.FileService;
import bio.terra.service.filedata.azure.blobstore.AzureBlobStorePdao;
import bio.terra.service.filedata.azure.tables.TableDao;
import bio.terra.service.filedata.google.firestore.FireStoreDao;
import bio.terra.service.filedata.google.gcs.GcsPdao;
import bio.terra.service.job.JobMapKeys;
import bio.terra.service.load.LoadService;
import bio.terra.service.load.flight.LoadLockStep;
import bio.terra.service.load.flight.LoadUnlockStep;
import bio.terra.service.profile.ProfileService;
import bio.terra.service.profile.flight.AuthorizeBillingProfileUseStep;
import bio.terra.service.profile.flight.VerifyBillingAccountAccessStep;
import bio.terra.service.profile.google.GoogleBillingService;
import bio.terra.service.resourcemanagement.ResourceService;
import bio.terra.service.resourcemanagement.google.GoogleProjectService;
import bio.terra.stairway.FlightMap;
import bio.terra.stairway.RetryRule;
import java.util.UUID;
import org.springframework.context.ApplicationContext;

// The FileIngestFlight is specific to firestore. Another cloud or file system implementation
// might be quite different and would need a different flight.
// TODO: Refactor flights when we do the cloud refactor work.
public class FileIngestFlight extends FileIngestTypeFlight {

  public FileIngestFlight(FlightMap inputParameters, Object applicationContext) {
    super(inputParameters, applicationContext);

    ApplicationContext appContext = (ApplicationContext) applicationContext;
    FireStoreDao fileDao = appContext.getBean(FireStoreDao.class);
    FileService fileService = appContext.getBean(FileService.class);
    GcsPdao gcsPdao = appContext.getBean(GcsPdao.class);
    AzureBlobStorePdao azureBlobStorePdao = appContext.getBean(AzureBlobStorePdao.class);
    TableDao azureTableDao = appContext.getBean(TableDao.class);
    DatasetService datasetService = appContext.getBean(DatasetService.class);
    ResourceService resourceService = appContext.getBean(ResourceService.class);
    LoadService loadService = appContext.getBean(LoadService.class);
    ApplicationConfiguration appConfig = appContext.getBean(ApplicationConfiguration.class);
    ConfigurationService configService = appContext.getBean(ConfigurationService.class);
    ProfileService profileService = appContext.getBean(ProfileService.class);
    DatasetBucketDao datasetBucketDao = appContext.getBean(DatasetBucketDao.class);
    GoogleProjectService googleProjectService = appContext.getBean(GoogleProjectService.class);
    GoogleBillingService googleBillingService = appContext.getBean(GoogleBillingService.class);
    DatasetStorageAccountDao datasetStorageAccountDao =
        appContext.getBean(DatasetStorageAccountDao.class);
    IamService iamService = appContext.getBean(IamService.class);

    UUID datasetId =
        UUID.fromString(inputParameters.get(JobMapKeys.DATASET_ID.getKeyName(), String.class));
    Dataset dataset = datasetService.retrieve(datasetId);

    var platform = CloudPlatformWrapper.of(dataset.getDatasetSummary().getStorageCloudPlatform());

    FileLoadModel fileLoadModel =
        inputParameters.get(JobMapKeys.REQUEST.getKeyName(), FileLoadModel.class);
    UUID profileId = fileLoadModel.getProfileId();

    AuthenticatedUserRequest userReq =
        inputParameters.get(JobMapKeys.AUTH_USER_INFO.getKeyName(), AuthenticatedUserRequest.class);

    RetryRule randomBackoffRetry =
        getDefaultRandomBackoffRetryRule(appConfig.getMaxStairwayThreads());

    // The flight plan:
    // 0. Validate the file load model.
    // 1. Make sure this user is allowed to use the billing profile and that the underlying
    //    billing information remains valid.
    // 1a. Check to see if this is an Azure billing profile and fail until it's implemented.
    // 2. Take out a shared lock on the dataset. This is to make sure the dataset isn't deleted
    // while this
    //    flight is running.
    // 3. Lock the load tag - only one flight operating on a load tag at a time
    // 4. Generate the new file id and store it in the working map. We need to allocate the file id
    // before any
    //    other operation so that it is persisted in the working map. In particular,
    // IngestFileDirectoryStep undo
    //    needs to know the file id in order to clean up.
    // 5. Create the directory entry for the file. The state where there is a directory entry for a
    // file, but
    //    no entry in the file collection, indicates that the file is being ingested (or deleted)
    // and so REST API
    //    lookups will not reveal that it exists. We make the directory entry first, because that
    // atomic operation
    //    prevents a second ingest with the same path from getting created.
    // 6. Locate the bucket where this file should go and store it in the working map. We need to
    // make the
    //    decision about where we will put the file and remember it persistently in the working map
    // before
    //    we copy the file in. That allows the copy undo to know the location to look at to delete
    // the file.
    //    We also check to see if the dataset is already linked to the bucket and remember that in
    // the working map.
    //    That allows the next step to properly insert or remove link.
    // 7. If the dataset is not already linked to the bucket, link it
    // 8. Copy the file into the bucket. Return the gspath, checksum, size, and create time in the
    // working map.
    // 9. Create the file entry in the filesystem. The file object takes the gspath, checksum, size,
    // and create
    //    time of the actual file in GCS. That ensures that the file info we return on REST API (and
    // DRS) lookups
    //    matches what users will see when they examine the GCS object. When the file entry is
    // (atomically)
    //    created in the file firestore collection, the file becomes visible for REST API lookups.
    // 10. Unlock the load tag
    // 11. Unlock the dataset

    addStep(new ValidateIngestFileLoadModelStep());

    addStep(new AuthorizeBillingProfileUseStep(profileService, profileId, userReq));
    if (platform.isAzure()) {
      addStep(new IngestFileValidateAzureBillingProfileStep(profileId, dataset));
    }
    addStep(new IngestFileValidateCloudPlatformStep(dataset));
    addStep(new LockDatasetStep(datasetService, datasetId, true), randomBackoffRetry);
    addStep(new LoadLockStep(loadService));
    addStep(new IngestFileIdStep(configService));
    addStep(
        new ValidateBucketAccessStep(gcsPdao, userReq, dataset),
        getDefaultExponentialBackoffRetryRule());

    if (platform.isGcp()) {
      addStep(new VerifyBillingAccountAccessStep(googleBillingService));
      addStep(new ValidateIngestFileDirectoryStep(fileDao, dataset));
      if (!dataset.isSelfHosted()) {
        addStep(new IngestFileGetProjectStep(dataset, googleProjectService));
        addStep(new IngestFileInitializeProjectStep(resourceService, dataset), randomBackoffRetry);
        addStep(
            new IngestFilePrimaryDataLocationStep(userReq, resourceService, dataset, iamService),
            randomBackoffRetry);
        addStep(new IngestFileMakeBucketLinkStep(datasetBucketDao, dataset), randomBackoffRetry);
      }
      addFileCopyAndDirectoryRecordStepsGcp(
          fileDao, gcsPdao, configService, dataset, randomBackoffRetry);
      addStep(new IngestFileFileStep(fileDao, fileService, dataset), randomBackoffRetry);
    } else if (platform.isAzure()) {
      addStep(
          new IngestFileAzurePrimaryDataLocationStep(resourceService, dataset), randomBackoffRetry);
      addStep(
          new IngestFileAzureMakeStorageAccountLinkStep(datasetStorageAccountDao, dataset),
          randomBackoffRetry);
      addStep(new ValidateIngestFileAzureDirectoryStep(azureTableDao, dataset), randomBackoffRetry);
      addFileCopyAndDirectoryRecordStepsAzure(
          azureBlobStorePdao, configService, azureTableDao, userReq, dataset, randomBackoffRetry);
      addStep(new IngestFileAzureFileStep(azureTableDao, fileService, dataset), randomBackoffRetry);
    }
    addStep(new LoadUnlockStep(loadService));
    addStep(new UnlockDatasetStep(datasetService, datasetId, true), randomBackoffRetry);
  }
}
