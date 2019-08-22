package bio.terra.filesystem.flight.ingest;

import bio.terra.filesystem.FireStoreDao;
import bio.terra.filesystem.FireStoreUtils;
import bio.terra.metadata.Dataset;
import bio.terra.pdao.gcs.GcsPdao;
import bio.terra.resourcemanagement.service.ProfileService;
import bio.terra.service.DatasetService;
import bio.terra.service.FileService;
import bio.terra.service.JobMapKeys;
import bio.terra.stairway.Flight;
import bio.terra.stairway.FlightMap;
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
        FireStoreUtils fireStoreUtils = (FireStoreUtils)appContext.getBean("fireStorUtils");
        FileService fileService = (FileService)appContext.getBean("fileService");
        GcsPdao gcsPdao = (GcsPdao)appContext.getBean("gcsPdao");
        DatasetService datasetService = (DatasetService)appContext.getBean("datasetService");
        ProfileService profileService = (ProfileService)appContext.getBean("profileService");

        String datasetId = inputParameters.get(JobMapKeys.DATASET_ID.getKeyName(), String.class);
        Dataset dataset = datasetService.retrieve(UUID.fromString(datasetId));

        // The flight plan:
        addStep(new IngestFileObjectIdStep());
        addStep(new IngestFileDirectoryStep(fileDao, fireStoreUtils, dataset));
        addStep(new IngestFilePrimaryDataStep(fileDao, dataset, gcsPdao));
        addStep(new IngestFileFileStep(fileDao, fileService, dataset));
    }

}
