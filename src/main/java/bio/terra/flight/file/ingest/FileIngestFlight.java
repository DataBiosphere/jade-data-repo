package bio.terra.flight.file.ingest;

import bio.terra.filesystem.FireStoreDirectoryDao;
import bio.terra.metadata.Dataset;
import bio.terra.pdao.gcs.GcsPdao;
import bio.terra.resourcemanagement.service.ProfileService;
import bio.terra.service.FileService;
import bio.terra.service.JobMapKeys;
import bio.terra.service.DatasetService;
import bio.terra.stairway.Flight;
import bio.terra.stairway.FlightMap;
import org.springframework.context.ApplicationContext;

import java.util.UUID;

public class FileIngestFlight extends Flight {

    public FileIngestFlight(FlightMap inputParameters, Object applicationContext) {
        super(inputParameters, applicationContext);

        ApplicationContext appContext = (ApplicationContext) applicationContext;
        FireStoreDirectoryDao fileDao = (FireStoreDirectoryDao)appContext.getBean("fireStoreFileDao");
        FileService fileService = (FileService)appContext.getBean("fileService");
        GcsPdao gcsPdao = (GcsPdao)appContext.getBean("gcsPdao");
        DatasetService datasetService = (DatasetService)appContext.getBean("datasetService");
        ProfileService profileService = (ProfileService)appContext.getBean("profileService");

        String datasetId = inputParameters.get(JobMapKeys.DATASET_ID.getKeyName(), String.class);
        Dataset dataset = datasetService.retrieve(UUID.fromString(datasetId));
        // The new flight plan:
        // 1. Metadata step:
        //    Compute the target gspath where the file should go and generate the objectId
        //    Create any missing directories and the fileref in firestore - fileref is marked as "ingesting"
        // 2. pdao does the file copy and returns file gspath, checksum and size
        // 3. Create the file object in firestore with fileinfo from pdao
        // 4. Update the directory fileref marking it as "existing"


        // The flight plan:
        // 1. Metadata step:
        //    Compute the target gspath where the file should go
        //    Create the file object in the database; marked as not present
        // 2. pdao does the file copy and returns file gspath, checksum and size
        // 3. Update the file object with the gspath, checksum and size and mark as present
        addStep(new IngestFileMetadataStepStart(fileDao, dataset, profileService));
        addStep(new IngestFilePrimaryDataStep(fileDao, dataset, gcsPdao));
        addStep(new IngestFileMetadataStepComplete(fileDao, fileService, dataset));
    }

}
