package bio.terra.flight.file.delete;

import bio.terra.filesystem.FireStoreDao;
import bio.terra.filesystem.FireStoreDependencyDao;
import bio.terra.metadata.Dataset;
import bio.terra.pdao.gcs.GcsPdao;
import bio.terra.service.DatasetService;
import bio.terra.service.JobMapKeys;
import bio.terra.service.dataproject.DataLocationService;
import bio.terra.stairway.Flight;
import bio.terra.stairway.FlightMap;
import bio.terra.stairway.UserRequestInfo;
import org.springframework.context.ApplicationContext;

import java.util.Map;
import java.util.UUID;

public class FileDeleteFlight extends Flight {

    public FileDeleteFlight(FlightMap inputParameters, Object applicationContext, UserRequestInfo userRequestInfo) {
        super(inputParameters, applicationContext, userRequestInfo);

        ApplicationContext appContext = (ApplicationContext) applicationContext;
        FireStoreDao fileDao = (FireStoreDao)appContext.getBean("fireStoreDao");
        FireStoreDependencyDao dependencyDao = (FireStoreDependencyDao)appContext.getBean("fireStoreDependencyDao");
        GcsPdao gcsPdao = (GcsPdao)appContext.getBean("gcsPdao");
        DatasetService datasetService = (DatasetService) appContext.getBean("datasetService");
        DataLocationService locationService = (DataLocationService) appContext.getBean("dataLocationService");

        // TODO: fix this
        // There are two problems here.
        // First, the PATH_PARAMETERS debacle.
        // Second, error handling within this constructor results in an obscure throw from
        // Java (INVOCATION_EXCEPTION), instead of getting a good DATASET_NOT_FOUND error.
        // We should NOT put code like that in the flight constructor.
        Map<String, String> pathParams = (Map<String, String>) inputParameters.get(
            JobMapKeys.PATH_PARAMETERS.getKeyName(), Map.class);
        String datasetId = pathParams.get(JobMapKeys.DATASET_ID.getKeyName());
        String fileId = pathParams.get(JobMapKeys.FILE_ID.getKeyName());
        Dataset dataset = datasetService.retrieve(UUID.fromString(datasetId));

        // The flight plan:
        // 1. Lookup file object and store the data in the flight map; check dependencies
        // 2. Delete the file object - after this point, no one will be able to retrieve the file
        // 3. pdao GCS delete the file
        // 4. Delete the directory entry
        addStep(new DeleteFileLookupStep(fileDao, fileId, dataset, dependencyDao));
        addStep(new DeleteFileMetadataStep(fileDao, fileId, dataset));
        addStep(new DeleteFilePrimaryDataStep(dataset, fileId, gcsPdao, fileDao, locationService));
        addStep(new DeleteFileDirectoryStep(fileDao, fileId, dataset));
    }

}
