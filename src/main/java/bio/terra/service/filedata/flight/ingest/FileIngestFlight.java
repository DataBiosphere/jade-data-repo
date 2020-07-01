package bio.terra.service.filedata.flight.ingest;

import bio.terra.app.configuration.ApplicationConfiguration;
import bio.terra.model.FileLoadModel;
import bio.terra.service.configuration.ConfigurationService;
import bio.terra.service.dataset.Dataset;
import bio.terra.service.dataset.DatasetDao;
import bio.terra.service.dataset.DatasetService;
import bio.terra.service.dataset.flight.LockDatasetStep;
import bio.terra.service.dataset.flight.UnlockDatasetStep;
import bio.terra.service.filedata.FileService;
import bio.terra.service.filedata.google.firestore.FireStoreDao;
import bio.terra.service.filedata.google.firestore.FireStoreUtils;
import bio.terra.service.filedata.google.gcs.GcsPdao;
import bio.terra.service.job.JobMapKeys;
import bio.terra.service.load.LoadService;
import bio.terra.service.load.flight.LoadLockStep;
import bio.terra.service.load.flight.LoadUnlockStep;
import bio.terra.service.resourcemanagement.DataLocationService;
import bio.terra.stairway.Flight;
import bio.terra.stairway.FlightMap;
import bio.terra.stairway.RetryRuleRandomBackoff;
import org.springframework.context.ApplicationContext;

import java.util.UUID;

// The FileIngestFlight is specific to firestore. Another cloud or file system implementation
// might be quite different and would need a different flight.
// TODO: Refactor flights when we do the cloud refactor work.
public class FileIngestFlight extends Flight {

    public FileIngestFlight(FlightMap inputParameters, Object applicationContext) {
        super(inputParameters, applicationContext);

        ApplicationContext appContext = (ApplicationContext) applicationContext;
        FireStoreDao fileDao = (FireStoreDao)appContext.getBean("fireStoreDao");
        FireStoreUtils fireStoreUtils = (FireStoreUtils)appContext.getBean("fireStoreUtils");
        FileService fileService = (FileService)appContext.getBean("fileService");
        GcsPdao gcsPdao = (GcsPdao)appContext.getBean("gcsPdao");
        DatasetService datasetService = (DatasetService)appContext.getBean("datasetService");
        DatasetDao datasetDao = (DatasetDao) appContext.getBean("datasetDao");
        DataLocationService locationService = (DataLocationService)appContext.getBean("dataLocationService");
        LoadService loadService = (LoadService)appContext.getBean("loadService");
        ApplicationConfiguration appConfig =
            (ApplicationConfiguration)appContext.getBean("applicationConfiguration");
        ConfigurationService configService = (ConfigurationService)appContext.getBean("configurationService");

        UUID datasetId = UUID.fromString(inputParameters.get(JobMapKeys.DATASET_ID.getKeyName(), String.class));
        Dataset dataset = datasetService.retrieve(datasetId);

        FileLoadModel fileLoadModel = inputParameters.get(JobMapKeys.REQUEST.getKeyName(), FileLoadModel.class);
        String profileId = fileLoadModel.getProfileId();

        RetryRuleRandomBackoff lockDatasetRetry =
            new RetryRuleRandomBackoff(500, appConfig.getMaxStairwayThreads(), 5);

        RetryRuleRandomBackoff fileSystemRetry = new RetryRuleRandomBackoff(500, appConfig.getMaxStairwayThreads(), 5);
        RetryRuleRandomBackoff createBucketRetry =
            new RetryRuleRandomBackoff(500, appConfig.getMaxStairwayThreads(), 5);

        // The flight plan:
        // 0. Take out a shared lock on the dataset. This is to make sure the dataset isn't deleted while this
        //    flight is running.
        // 1. Lock the load tag - only one flight operating on a load tag at a time
        // 2. Generate the new file id and store it in the working map. We need to allocate the file id before any
        //    other operation so that it is persisted in the working map. In particular, IngestFileDirectoryStep undo
        //    needs to know the file id in order to clean up.
        // 3. Create the directory entry for the file. The state where there is a directory entry for a file, but
        //    no entry in the file collection, indicates that the file is being ingested (or deleted) and so REST API
        //    lookups will not reveal that it exists. We make the directory entry first, because that atomic operation
        //    prevents a second ingest with the same path from getting created.
        // 4. Locate the bucket where this file should go and store it in the working map. We need to make the
        //    decision about where we will put the file and remember it persistently in the working map before
        //    we copy the file in. That allows the copy undo to know the location to look at to delete the file.
        // 5. Copy the file into the bucket. Return the gspath, checksum, size, and create time in the working map.
        // 6. Create the file entry in the filesystem. The file object takes the gspath, checksum, size, and create
        //    time of the actual file in GCS. That ensures that the file info we return on REST API (and DRS) lookups
        //    matches what users will see when they examine the GCS object. When the file entry is (atomically)
        //    created in the file firestore collection, the file becomes visible for REST API lookups.
        // 7. Unlock the load tag
        // 8. Unlock the dataset
        addStep(new LockDatasetStep(datasetDao, datasetId, true), lockDatasetRetry);
        addStep(new LoadLockStep(loadService));
        addStep(new IngestFileIdStep(configService));
        addStep(new IngestFileDirectoryStep(fileDao, fireStoreUtils, dataset), fileSystemRetry);
        addStep(new IngestFilePrimaryDataLocationStep(locationService, profileId), createBucketRetry);
        addStep(new IngestFilePrimaryDataStep(dataset, gcsPdao, configService));
        addStep(new IngestFileFileStep(fileDao, fileService, dataset), fileSystemRetry);
        addStep(new LoadUnlockStep(loadService));
        addStep(new UnlockDatasetStep(datasetDao, datasetId, true));
    }

}
