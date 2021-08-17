package bio.terra.service.filedata.flight.ingest;

import static bio.terra.common.FlightUtils.getDefaultRandomBackoffRetryRule;

import bio.terra.app.configuration.ApplicationConfiguration;
import bio.terra.common.CloudPlatformWrapper;
import bio.terra.model.FileLoadModel;
import bio.terra.service.configuration.ConfigurationService;
import bio.terra.service.dataset.Dataset;
import bio.terra.service.dataset.DatasetBucketDao;
import bio.terra.service.dataset.DatasetDao;
import bio.terra.service.dataset.DatasetService;
import bio.terra.service.dataset.DatasetStorageAccountDao;
import bio.terra.service.dataset.flight.LockDatasetStep;
import bio.terra.service.dataset.flight.UnlockDatasetStep;
import bio.terra.service.filedata.FileMetadataUtils;
import bio.terra.service.filedata.FileService;
import bio.terra.service.filedata.azure.blobstore.AzureBlobStorePdao;
import bio.terra.service.filedata.google.firestore.FireStoreDao;
import bio.terra.service.filedata.google.gcs.GcsPdao;
import bio.terra.service.iam.AuthenticatedUserRequest;
import bio.terra.service.job.JobMapKeys;
import bio.terra.service.load.LoadService;
import bio.terra.service.load.flight.LoadLockStep;
import bio.terra.service.load.flight.LoadUnlockStep;
import bio.terra.service.profile.ProfileService;
import bio.terra.service.profile.flight.AuthorizeBillingProfileUseStep;
import bio.terra.service.resourcemanagement.ResourceService;
import bio.terra.service.resourcemanagement.google.GoogleProjectService;
import bio.terra.stairway.Flight;
import bio.terra.stairway.FlightMap;
import bio.terra.stairway.RetryRule;

import java.io.File;
import java.util.UUID;
import org.springframework.context.ApplicationContext;

// The FileIngestFlight is specific to firestore. Another cloud or file system implementation
// might be quite different and would need a different flight.
// TODO: Refactor flights when we do the cloud refactor work.
public class FileIngestFlight extends Flight {

  public FileIngestFlight(FlightMap inputParameters, Object applicationContext) {
    super(inputParameters, applicationContext);

    ApplicationContext appContext = (ApplicationContext) applicationContext;
    FireStoreDao fileDao = appContext.getBean(FireStoreDao.class);
    FileMetadataUtils fileMetadataUtils = appContext.getBean(FileMetadataUtils.class);
    FileService fileService = appContext.getBean(FileService.class);
    GcsPdao gcsPdao = appContext.getBean(GcsPdao.class);
    AzureBlobStorePdao azureBlobStorePdao = appContext.getBean(AzureBlobStorePdao.class);
    DatasetService datasetService = appContext.getBean(DatasetService.class);
    DatasetDao datasetDao = appContext.getBean(DatasetDao.class);
    ResourceService resourceService = appContext.getBean(ResourceService.class);
    LoadService loadService = appContext.getBean(LoadService.class);
    ApplicationConfiguration appConfig = appContext.getBean(ApplicationConfiguration.class);
    ConfigurationService configService = appContext.getBean(ConfigurationService.class);
    ProfileService profileService = appContext.getBean(ProfileService.class);
    DatasetBucketDao datasetBucketDao = appContext.getBean(DatasetBucketDao.class);
    GoogleProjectService googleProjectService = appContext.getBean(GoogleProjectService.class);
    DatasetStorageAccountDao datasetStorageAccountDao =
        appContext.getBean(DatasetStorageAccountDao.class);

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
    // 0. Make sure this user is allowed to use the billing profile and that the underlying
    //    billing information remains valid.
    // 0a. Check to see if this is an Azure billing profile and fail until it's implemented.
    // 1. Take out a shared lock on the dataset. This is to make sure the dataset isn't deleted
    // while this
    //    flight is running.
    // 2. Lock the load tag - only one flight operating on a load tag at a time
    // 3. Generate the new file id and store it in the working map. We need to allocate the file id
    // before any
    //    other operation so that it is persisted in the working map. In particular,
    // IngestFileDirectoryStep undo
    //    needs to know the file id in order to clean up.
    // 4. Create the directory entry for the file. The state where there is a directory entry for a
    // file, but
    //    no entry in the file collection, indicates that the file is being ingested (or deleted)
    // and so REST API
    //    lookups will not reveal that it exists. We make the directory entry first, because that
    // atomic operation
    //    prevents a second ingest with the same path from getting created.
    // 5. Locate the bucket where this file should go and store it in the working map. We need to
    // make the
    //    decision about where we will put the file and remember it persistently in the working map
    // before
    //    we copy the file in. That allows the copy undo to know the location to look at to delete
    // the file.
    //    We also check to see if the dataset is already linked to the bucket and remember that in
    // the working map.
    //    That allows the next step to properly insert or remove link.
    // 6. If the dataset is not already linked to the bucket, link it
    // 7. Copy the file into the bucket. Return the gspath, checksum, size, and create time in the
    // working map.
    // 8. Create the file entry in the filesystem. The file object takes the gspath, checksum, size,
    // and create
    //    time of the actual file in GCS. That ensures that the file info we return on REST API (and
    // DRS) lookups
    //    matches what users will see when they examine the GCS object. When the file entry is
    // (atomically)
    //    created in the file firestore collection, the file becomes visible for REST API lookups.
    // 9. Unlock the load tag
    // 10. Unlock the dataset
    addStep(new AuthorizeBillingProfileUseStep(profileService, profileId, userReq));
    addStep(new IngestFileValidateCloudPlatformStep(dataset));
    addStep(new LockDatasetStep(datasetDao, datasetId, true), randomBackoffRetry);
    addStep(new LoadLockStep(loadService));
    addStep(new IngestFileIdStep(configService));
    addStep(new ValidateIngestFileDirectoryStep(fileDao, dataset));
    addStep(new IngestFileDirectoryStep(fileDao, fileMetadataUtils, dataset), randomBackoffRetry);
    addStep(new IngestFileGetProjectStep(dataset, googleProjectService));
    addStep(new IngestFileGetOrCreateProject(resourceService, dataset), randomBackoffRetry);
    if (platform.isGcp()) {
      addStep(new IngestFilePrimaryDataLocationStep(resourceService, dataset), randomBackoffRetry);
      addStep(new IngestFileMakeBucketLinkStep(datasetBucketDao, dataset), randomBackoffRetry);
      addStep(new IngestFilePrimaryDataStep(dataset, gcsPdao, configService));
    } else if (platform.isAzure()) {
      addStep(
          new IngestFileAzurePrimaryDataLocationStep(resourceService, dataset), randomBackoffRetry);
      addStep(
          new IngestFileAzureMakeStorageAccountLinkStep(datasetStorageAccountDao, dataset),
          randomBackoffRetry);
      addStep(new IngestFileAzurePrimaryDataStep(azureBlobStorePdao, configService));
    }
    addStep(new IngestFileFileStep(fileDao, fileService, dataset), randomBackoffRetry);
    addStep(new LoadUnlockStep(loadService));
    addStep(new UnlockDatasetStep(datasetDao, datasetId, true), randomBackoffRetry);
  }
}
