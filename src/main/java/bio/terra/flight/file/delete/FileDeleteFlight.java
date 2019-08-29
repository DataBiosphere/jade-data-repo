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

        String datasetId = inputParameters.get(JobMapKeys.DATASET_ID.getKeyName(), String.class);
        String fileId = inputParameters.get(JobMapKeys.REQUEST.getKeyName(), String.class);
        Dataset dataset = datasetService.retrieve(UUID.fromString(datasetId));

        // The flight plan:
        // 1. Lookup file object and store the data in the flight map; check dependencies
        // 2. Delete the file object - after this point, no one will be able to retrieve the file
        // 3. pdao GCS delete the file
        // 4. Delete the directory entry
        addStep(new DeleteFileLookupStep(fileDao, fileId, dataset, dependencyDao));
        addStep(new DeleteFileObjectStep(fileDao, fileId, dataset));
        addStep(new DeleteFilePrimaryDataStep(dataset, fileId, gcsPdao, fileDao, locationService));
        addStep(new DeleteFileDirectoryStep(fileDao, fileId, dataset));
    }

}
