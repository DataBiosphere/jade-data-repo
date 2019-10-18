package bio.terra.service.filedata.flight.ingest;

import bio.terra.service.filedata.google.firestore.FireStoreDao;
import bio.terra.service.filedata.google.firestore.FireStoreUtils;
import bio.terra.service.dataset.Dataset;
import bio.terra.service.filedata.google.gcs.GcsPdao;
import bio.terra.service.dataset.DatasetService;
import bio.terra.service.filedata.FileService;
import bio.terra.service.job.JobMapKeys;
import bio.terra.service.resourcemanagement.DataLocationService;
import bio.terra.stairway.Flight;
import bio.terra.stairway.FlightMap;
import bio.terra.stairway.UserRequestInfo;
import org.springframework.context.ApplicationContext;

import java.util.UUID;

// The FileIngestFlight is specific to firestore. Another cloud or file system implementation
// might be quite different and would need a different flight.
// TODO: Refactor flights when we do the cloud refactor work.
public class FileIngestFlight extends Flight {

    public FileIngestFlight(FlightMap inputParameters, Object applicationContext, UserRequestInfo userRequestInfo) {
        super(inputParameters, applicationContext, userRequestInfo);

        ApplicationContext appContext = (ApplicationContext) applicationContext;
        FireStoreDao fileDao = (FireStoreDao)appContext.getBean("fireStoreDao");
        FireStoreUtils fireStoreUtils = (FireStoreUtils)appContext.getBean("fireStoreUtils");
        FileService fileService = (FileService)appContext.getBean("fileService");
        GcsPdao gcsPdao = (GcsPdao)appContext.getBean("gcsPdao");
        DatasetService datasetService = (DatasetService)appContext.getBean("datasetService");
        DataLocationService locationService = (DataLocationService)appContext.getBean("dataLocationService");

        // TODO: fix this
        // There are two problems here.
        // First, the PATH_PARAMETERS debacle.
        // Second, error handling within this constructor results in an obscure throw from
        // Java (INVOCATION_EXCEPTION), instead of getting a good DATASET_NOT_FOUND error.
        // We should NOT put code like that in the flight constructor.

        // get data from inputs that steps need
        UUID datasetId = UUID.fromString(inputParameters.get(
            JobMapKeys.DATASET_ID.getKeyName(), String.class));
        Dataset dataset = datasetService.retrieve(datasetId);

        // The flight plan:
        // 1. Generate the new file id and store it in the working map. We need to allocate the file id before any
        //    other operation so that it is persisted in the working map. In particular, IngestFileDirectoryStep undo
        //    needs to know the file id in order to clean up.
        // 2. Create the directory entry for the file. The state where there is a directory entry for a file, but
        //    no entry in the file collection, indicates that the file is being ingested (or deleted) and so REST API
        //    lookups will not reveal that it exists. We make the directory entry first, because that atomic operation
        //    prevents a second ingest with the same path from getting created.
        // 3. Locate the bucket where this file should go and store it in the working map. We need to make the
        //    decision about where we will put the file and remember it persistently in the working map before
        //    we copy the file in. That allows the copy undo to know the location to look at to delete the file.
        // 4. Copy the file into the bucket. Return the gspath, checksum, size, and create time in the working map.
        // 5. Create the file entry in the filesystem. The file object takes the gspath, checksum, size, and create
        //    time of the actual file in GCS. That ensures that the file info we return on REST API (and DRS) lookups
        //    matches what users will see when they examine the GCS object. When the file entry is (atomically)
        //    created in the file firestore collection, the file becomes visible for REST API lookups.
        addStep(new IngestFileIdStep());
        addStep(new IngestFileDirectoryStep(fileDao, fireStoreUtils, dataset));
        addStep(new IngestFilePrimaryDataLocationStep(fileDao, dataset, locationService));
        addStep(new IngestFilePrimaryDataStep(fileDao, dataset, gcsPdao));
        addStep(new IngestFileFileStep(fileDao, fileService, dataset));
    }

}
