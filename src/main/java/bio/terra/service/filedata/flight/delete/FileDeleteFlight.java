package bio.terra.service.filedata.flight.delete;

import bio.terra.app.configuration.ApplicationConfiguration;
import bio.terra.service.configuration.ConfigurationService;
import bio.terra.service.dataset.Dataset;
import bio.terra.service.dataset.DatasetDao;
import bio.terra.service.dataset.DatasetService;
import bio.terra.service.dataset.flight.LockDatasetStep;
import bio.terra.service.dataset.flight.UnlockDatasetStep;
import bio.terra.service.filedata.google.firestore.FireStoreDao;
import bio.terra.service.filedata.google.firestore.FireStoreDependencyDao;
import bio.terra.service.filedata.google.gcs.GcsPdao;
import bio.terra.service.job.JobMapKeys;
import bio.terra.service.resourcemanagement.DataLocationService;
import bio.terra.stairway.Flight;
import bio.terra.stairway.FlightMap;
import bio.terra.stairway.RetryRuleRandomBackoff;
import org.springframework.context.ApplicationContext;

import java.util.UUID;

public class FileDeleteFlight extends Flight {

    public FileDeleteFlight(FlightMap inputParameters, Object applicationContext) {
        super(inputParameters, applicationContext);

        ApplicationContext appContext = (ApplicationContext) applicationContext;
        FireStoreDao fileDao = (FireStoreDao)appContext.getBean("fireStoreDao");
        FireStoreDependencyDao dependencyDao = (FireStoreDependencyDao)appContext.getBean("fireStoreDependencyDao");
        GcsPdao gcsPdao = (GcsPdao)appContext.getBean("gcsPdao");
        DatasetService datasetService = (DatasetService) appContext.getBean("datasetService");
        DatasetDao datasetDao = (DatasetDao) appContext.getBean("datasetDao");
        DataLocationService locationService = (DataLocationService) appContext.getBean("dataLocationService");
        ApplicationConfiguration appConfig =
            (ApplicationConfiguration)appContext.getBean("applicationConfiguration");
        ConfigurationService configService = (ConfigurationService) appContext.getBean("configurationService");

        String datasetId = inputParameters.get(JobMapKeys.DATASET_ID.getKeyName(), String.class);
        String fileId = inputParameters.get(JobMapKeys.FILE_ID.getKeyName(), String.class);

        // TODO: fix this
        //  Error handling within this constructor results in an obscure throw from
        //  Java (INVOCATION_EXCEPTION), instead of getting a good DATASET_NOT_FOUND error.
        //  We should NOT put code like that in the flight constructor.
        Dataset dataset = datasetService.retrieve(UUID.fromString(datasetId));

        RetryRuleRandomBackoff fileSystemRetry =
            new RetryRuleRandomBackoff(500, appConfig.getMaxStairwayThreads(), 5);
        RetryRuleRandomBackoff lockDatasetRetry =
            new RetryRuleRandomBackoff(500, appConfig.getMaxStairwayThreads(), 5);

        // The flight plan:
        // 0. Take out a shared lock on the dataset. This is to make sure the dataset isn't deleted while this
        //    flight is running.
        // 1. Lookup file and store the file data in the flight map. Check dependencies to make sure that the
        //    delete is allowed. We do the lookup and store so that we have all of the file information, since
        //    once we start deleting things, we can't look it up again!
        // 2. Delete the file object - after this point, the file is not shown through the REST API.
        // 3. pdao GCS delete the file
        // 4. Delete the directory entry
        // This flight updates GCS and firestore in exactly the reverse order of create, so no new
        // data structure states are introduced by this flight.
        addStep(new LockDatasetStep(datasetDao, UUID.fromString(datasetId), true),
            lockDatasetRetry);
        addStep(new DeleteFileLookupStep(fileDao, fileId, dataset, dependencyDao, configService), fileSystemRetry);
        addStep(new DeleteFileMetadataStep(fileDao, fileId, dataset), fileSystemRetry);
        addStep(new DeleteFilePrimaryDataStep(dataset, fileId, gcsPdao, fileDao, locationService));
        addStep(new DeleteFileDirectoryStep(fileDao, fileId, dataset), fileSystemRetry);
        addStep(new UnlockDatasetStep(datasetDao, UUID.fromString(datasetId), true));
    }

}
