package bio.terra.flight.file.ingest;

import bio.terra.dao.DrDatasetDao;
import bio.terra.filesystem.FireStoreFileDao;
import bio.terra.metadata.DrDataset;
import bio.terra.pdao.gcs.GcsPdao;
import bio.terra.service.FileService;
import bio.terra.service.JobMapKeys;
import bio.terra.stairway.Flight;
import bio.terra.stairway.FlightMap;
import org.springframework.context.ApplicationContext;

import java.util.UUID;

public class FileIngestFlight extends Flight {

    public FileIngestFlight(FlightMap inputParameters, Object applicationContext) {
        super(inputParameters, applicationContext);

        ApplicationContext appContext = (ApplicationContext) applicationContext;
        DrDatasetDao datasetDao = (DrDatasetDao)appContext.getBean("drDatasetDao");
        FireStoreFileDao fileDao = (FireStoreFileDao)appContext.getBean("fireStoreFileDao");
        FileService fileService = (FileService)appContext.getBean("fileService");
        GcsPdao gcsPdao = (GcsPdao)appContext.getBean("gcsPdao");

        String datasetId = inputParameters.get(JobMapKeys.DATASET_ID.getKeyName(), String.class);
        DrDataset dataset = datasetDao.retrieve(UUID.fromString(datasetId));

        // The flight plan:
        // 1. Metadata step:
        //    Compute the target gspath where the file should go
        //    Create the file object in the database; marked as not present
        // 2. pdao does the file copy and returns file gspath, checksum and size
        // 3. Update the file object with the gspath, checksum and size and mark as present
        addStep(new IngestFileMetadataStepStart(fileDao, dataset));
        addStep(new IngestFilePrimaryDataStep(fileDao, dataset, fileService, gcsPdao));
        addStep(new IngestFileMetadataStepComplete(fileDao, fileService));
    }

}
