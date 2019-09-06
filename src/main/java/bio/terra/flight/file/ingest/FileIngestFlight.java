package bio.terra.flight.file.ingest;

import bio.terra.filesystem.FireStoreDao;
import bio.terra.filesystem.FireStoreUtils;
import bio.terra.metadata.Dataset;
import bio.terra.pdao.gcs.GcsPdao;
import bio.terra.service.DatasetService;
import bio.terra.service.FileService;
import bio.terra.service.JobMapKeys;
import bio.terra.service.dataproject.DataLocationService;
import bio.terra.stairway.Flight;
import bio.terra.stairway.FlightMap;
import bio.terra.stairway.UserRequestInfo;
import org.springframework.context.ApplicationContext;

import java.util.Map;
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
        Map<String, String> pathParams = (Map<String, String>) inputParameters.get(
            JobMapKeys.PATH_PARAMETERS.getKeyName(), Map.class);
        UUID datasetId = UUID.fromString(pathParams.get(JobMapKeys.DATASET_ID.getKeyName()));
        Dataset dataset = datasetService.retrieve(datasetId);

        // The flight plan:
        // 1. Generate the new file id and store it in the working map
        // 2. Create the directory entry for the file; lack of a file entry means it will be invisible
        //    to outside retrieval. Existence of a directory entry keeps a second file from getting the
        //    same name.
        // 3. Locate the bucket where this file should go and store it in the working map
        // 4. Copy the file into the bucket. Return the gspath, checksum, size, and create time
        // 5. Create the file entry in the filesystem.
        addStep(new IngestFileIdStep());
        addStep(new IngestFileDirectoryStep(fileDao, fireStoreUtils, dataset));
        addStep(new IngestFilePrimaryDataLocationStep(fileDao, dataset, locationService));
        addStep(new IngestFilePrimaryDataStep(fileDao, dataset, gcsPdao));
        addStep(new IngestFileFileStep(fileDao, fileService, dataset));
    }

}
