package bio.terra.flight.file.ingest;

import bio.terra.filesystem.FireStoreDao;
import bio.terra.filesystem.FireStoreUtils;
import bio.terra.metadata.Dataset;
import bio.terra.pdao.gcs.GcsPdao;
import bio.terra.resourcemanagement.service.ProfileService;
import bio.terra.service.DatasetService;
import bio.terra.service.FileService;
import bio.terra.service.JobMapKeys;
import bio.terra.service.dataproject.DataLocationService;
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
        DataLocationService locationService = (DataLocationService)appContext.getBean("dataLocationService");

        String datasetId = inputParameters.get(JobMapKeys.DATASET_ID.getKeyName(), String.class);
        Dataset dataset = datasetService.retrieve(UUID.fromString(datasetId));

        // The flight plan:
        // 1. Generate the new file object id and store it in the working map
        // 2. Create the directory entry for the file; lack of a file entry means it will be invisible
        //    to outside retrieval. Existence of a directory entry keeps a second file from getting the
        //    same name.
        // 3. Locate the bucket where this file sould go and store it in the working map
        // 4. Copy the file into the bucket. Return the gspath, checksum, size, and create time
        // 5. Create the file object.
        addStep(new IngestFileObjectIdStep());
        addStep(new IngestFileDirectoryStep(fileDao, fireStoreUtils, dataset));
        addStep(new IngestFilePrimaryDataLocationStep(fileDao, dataset, locationService));
        addStep(new IngestFilePrimaryDataStep(fileDao, dataset, gcsPdao));
        addStep(new IngestFileFileStep(fileDao, fileService, dataset));
    }

}
