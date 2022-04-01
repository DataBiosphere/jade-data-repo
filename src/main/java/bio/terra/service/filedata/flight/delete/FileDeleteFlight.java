package bio.terra.service.filedata.flight.delete;

import static bio.terra.common.FlightUtils.getDefaultRandomBackoffRetryRule;

import bio.terra.app.configuration.ApplicationConfiguration;
import bio.terra.common.CloudPlatformWrapper;
import bio.terra.common.iam.AuthenticatedUserRequest;
import bio.terra.service.configuration.ConfigurationService;
import bio.terra.service.dataset.Dataset;
import bio.terra.service.dataset.DatasetService;
import bio.terra.service.dataset.flight.LockDatasetStep;
import bio.terra.service.dataset.flight.UnlockDatasetStep;
import bio.terra.service.filedata.azure.blobstore.AzureBlobStorePdao;
import bio.terra.service.filedata.azure.tables.TableDao;
import bio.terra.service.filedata.google.firestore.FireStoreDao;
import bio.terra.service.filedata.google.firestore.FireStoreDependencyDao;
import bio.terra.service.filedata.google.gcs.GcsPdao;
import bio.terra.service.job.JobMapKeys;
import bio.terra.service.profile.ProfileDao;
import bio.terra.service.resourcemanagement.ResourceService;
import bio.terra.stairway.Flight;
import bio.terra.stairway.FlightMap;
import bio.terra.stairway.RetryRule;
import java.util.UUID;
import org.springframework.context.ApplicationContext;

public class FileDeleteFlight extends Flight {

  public FileDeleteFlight(FlightMap inputParameters, Object applicationContext) {
    super(inputParameters, applicationContext);

    ApplicationContext appContext = (ApplicationContext) applicationContext;
    FireStoreDao fileDao = appContext.getBean(FireStoreDao.class);
    TableDao tableDao = appContext.getBean(TableDao.class);
    FireStoreDependencyDao dependencyDao = appContext.getBean(FireStoreDependencyDao.class);
    GcsPdao gcsPdao = appContext.getBean(GcsPdao.class);
    AzureBlobStorePdao azureBlobStorePdao = appContext.getBean(AzureBlobStorePdao.class);
    DatasetService datasetService = appContext.getBean(DatasetService.class);
    ResourceService resourceService = appContext.getBean(ResourceService.class);
    ApplicationConfiguration appConfig = appContext.getBean(ApplicationConfiguration.class);
    ConfigurationService configService = appContext.getBean(ConfigurationService.class);
    ProfileDao profileDao = appContext.getBean(ProfileDao.class);

    UUID datasetId =
        UUID.fromString(inputParameters.get(JobMapKeys.DATASET_ID.getKeyName(), String.class));
    String fileId = inputParameters.get(JobMapKeys.FILE_ID.getKeyName(), String.class);
    AuthenticatedUserRequest userReq =
        inputParameters.get(JobMapKeys.AUTH_USER_INFO.getKeyName(), AuthenticatedUserRequest.class);
    // TODO: fix this
    //  Error handling within this constructor results in an obscure throw from
    //  Java (INVOCATION_EXCEPTION), instead of getting a good DATASET_NOT_FOUND error.
    //  We should NOT put code like that in the flight constructor.
    //  ** Well, what we really should do is fix Stairway to throw the contained exception **
    Dataset dataset = datasetService.retrieve(datasetId);

    var platform = CloudPlatformWrapper.of(dataset.getDatasetSummary().getStorageCloudPlatform());

    RetryRule fileSystemRetry = getDefaultRandomBackoffRetryRule(appConfig.getMaxStairwayThreads());
    RetryRule lockDatasetRetry =
        getDefaultRandomBackoffRetryRule(appConfig.getMaxStairwayThreads());

    // The flight plan:
    // 0. Take out a shared lock on the dataset. This is to make sure the dataset isn't deleted
    // while this
    //    flight is running.
    // 1. Lookup file and store the file data in the flight map. Check dependencies to make sure
    // that the
    //    delete is allowed. We do the lookup and store so that we have all of the file information,
    // since
    //    once we start deleting things, we can't look it up again!
    // 2. Delete the file object - after this point, the file is not shown through the REST API.
    // 3. pdao GCS delete the file
    // 4. Delete the directory entry
    // This flight updates GCS and firestore in exactly the reverse order of create, so no new
    // data structure states are introduced by this flight.
    addStep(new LockDatasetStep(datasetService, datasetId, true), lockDatasetRetry);
    if (platform.isGcp()) {
      addStep(
          new DeleteFileLookupStep(fileDao, fileId, dataset, dependencyDao, configService),
          fileSystemRetry);
      addStep(new DeleteFileMetadataStep(fileDao, fileId, dataset), fileSystemRetry);
      addStep(new DeleteFilePrimaryDataStep(gcsPdao));
      addStep(new DeleteFileDirectoryStep(fileDao, fileId, dataset), fileSystemRetry);
    } else if (platform.isAzure()) {
      addStep(
          new DeleteFileAzureLookupStep(
              tableDao, fileId, dataset, configService, resourceService, profileDao),
          fileSystemRetry);
      addStep(new DeleteFileAzureMetadataStep(tableDao, fileId, dataset), fileSystemRetry);
      addStep(new DeleteFileAzurePrimaryDataStep(azureBlobStorePdao, userReq));
      addStep(new DeleteFileAzureDirectoryStep(tableDao, fileId, dataset), fileSystemRetry);
    }
    addStep(new UnlockDatasetStep(datasetService, datasetId, true), lockDatasetRetry);
  }
}
