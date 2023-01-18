package bio.terra.service.filedata.flight.ingest;

import static bio.terra.common.FlightUtils.getDefaultRandomBackoffRetryRule;

import bio.terra.app.configuration.ApplicationConfiguration;
import bio.terra.common.CloudPlatformWrapper;
import bio.terra.common.iam.AuthenticatedUserRequest;
import bio.terra.service.configuration.ConfigurationService;
import bio.terra.service.dataset.Dataset;
import bio.terra.service.dataset.DatasetService;
import bio.terra.service.filedata.FileService;
import bio.terra.service.filedata.azure.blobstore.AzureBlobStorePdao;
import bio.terra.service.filedata.azure.tables.TableDao;
import bio.terra.service.filedata.google.firestore.FireStoreDao;
import bio.terra.service.filedata.google.gcs.GcsPdao;
import bio.terra.service.job.JobMapKeys;
import bio.terra.stairway.FlightMap;
import bio.terra.stairway.RetryRuleRandomBackoff;
import java.util.UUID;
import org.springframework.context.ApplicationContext;

/*
 * The flight is launched from the IngestDriverStep within one of the bulk load flights.
 * It performs a single file ingest into a dataset.
 * Input parameters expected:
 * - DATASET_ID - dataset into which we load the file
 * - REQUEST - a FileLoadModel describing the file to load
 */

public class FileIngestWorkerFlight extends FileIngestTypeFlight {

  public FileIngestWorkerFlight(FlightMap inputParameters, Object applicationContext) {
    super(inputParameters, applicationContext);

    ApplicationContext appContext = (ApplicationContext) applicationContext;
    FireStoreDao fileDao = appContext.getBean(FireStoreDao.class);
    FileService fileService = appContext.getBean(FileService.class);
    GcsPdao gcsPdao = appContext.getBean(GcsPdao.class);
    AzureBlobStorePdao azureBlobStorePdao = appContext.getBean(AzureBlobStorePdao.class);
    DatasetService datasetService = appContext.getBean(DatasetService.class);
    ApplicationConfiguration appConfig = appContext.getBean(ApplicationConfiguration.class);
    ConfigurationService configService = appContext.getBean(ConfigurationService.class);
    TableDao azureTableDao = appContext.getBean(TableDao.class);

    AuthenticatedUserRequest userReq =
        inputParameters.get(JobMapKeys.AUTH_USER_INFO.getKeyName(), AuthenticatedUserRequest.class);

    UUID datasetId =
        UUID.fromString(inputParameters.get(JobMapKeys.DATASET_ID.getKeyName(), String.class));

    var platform =
        CloudPlatformWrapper.of(
            inputParameters.get(JobMapKeys.CLOUD_PLATFORM.getKeyName(), String.class));
    Dataset dataset = datasetService.retrieve(datasetId);

    RetryRuleRandomBackoff fileSystemRetry =
        getDefaultRandomBackoffRetryRule(appConfig.getMaxStairwayThreads());

    // The flight plan:
    // 0. Validate the file load model.
    // 1. Generate the new file id and store it in the working map. We need to allocate the file id
    //   before any other operation so that it is persisted in the working map. In particular,
    //   IngestFileDirectoryStep undo needs to know the file id in order to clean up.
    // 2. Create the directory entry for the file. The state where there is a directory entry for a
    //   file, but no entry in the file collection, indicates that the file is being ingested
    //   (or deleted) and so REST API lookups will not reveal that it exists. We make the directory
    //   entry first, because that atomic operation prevents a second ingest with the same path from
    //   getting created.
    // 3. Copy the file into the bucket. Return the gspath, checksum, size, and create time in the
    //   working map.
    // 4. Create the file entry in the filesystem. The file object takes the gspath, checksum, size,
    //   and create time of the actual file in GCS. That ensures that the file info we return on
    //   REST API (and DRS) lookups matches what users will see when they examine the GCS object.
    //   When the file entry is (atomically) created in the file firestore collection,
    //   the file becomes visible for REST API lookups.

    addStep(new ValidateIngestFileLoadModelStep());

    addStep(new IngestFileIdStep(configService));

    if (platform.isGcp()) {
      addStep(new ValidateIngestFileDirectoryStep(fileDao, dataset));
      addFileCopyAndDirectoryRecordStepsGcp(
          fileDao, gcsPdao, configService, dataset, fileSystemRetry);
      addStep(new IngestFileFileStep(fileDao, fileService, dataset), fileSystemRetry);
    } else if (platform.isAzure()) {
      addStep(new ValidateIngestFileAzureDirectoryStep(azureTableDao, dataset), fileSystemRetry);
      addFileCopyAndDirectoryRecordStepsAzure(
          azureBlobStorePdao, configService, azureTableDao, userReq, dataset, fileSystemRetry);
      addStep(new IngestFileAzureFileStep(azureTableDao, fileService, dataset), fileSystemRetry);
    }
  }
}
