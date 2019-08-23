package bio.terra.filesystem.flight.delete;

import bio.terra.filesystem.FireStoreDao;
import bio.terra.metadata.Dataset;
import bio.terra.pdao.gcs.GcsPdao;
import bio.terra.service.DatasetService;
import bio.terra.service.JobMapKeys;
import bio.terra.stairway.Flight;
import bio.terra.stairway.FlightMap;
import org.springframework.context.ApplicationContext;

import java.util.UUID;

public class FileDeleteFlight extends Flight {

    public FileDeleteFlight(FlightMap inputParameters, Object applicationContext) {
        super(inputParameters, applicationContext);

        ApplicationContext appContext = (ApplicationContext) applicationContext;
        FireStoreDao fileDao = (FireStoreDao)appContext.getBean("fireStoreDao");
        GcsPdao gcsPdao = (GcsPdao)appContext.getBean("gcsPdao");
        DatasetService datasetService = (DatasetService) appContext.getBean("datasetService");

        String datasetId = inputParameters.get(JobMapKeys.DATASET_ID.getKeyName(), String.class);
        String fileId = inputParameters.get(JobMapKeys.REQUEST.getKeyName(), String.class);
        Dataset dataset = datasetService.retrieve(UUID.fromString(datasetId));

        // The flight plan:
        // 1. Lookup file object and store the data in the flight map
        // 2. Delete the file object - after this point, no one will be able to retrieve the file
        // 3. pdao GCS delete the file
        // 4. Delete the directory entry
// <<< YOU ARE HERE >>>
        addStep(new DeleteFileLookupStep(fileDao, fileId, dataset));
        addStep(new DeleteFileObjectStep(fileDao, fileId, dataset));
        addStep(new DeleteFilePrimaryDataStep(dataset, fileId, gcsPdao, fileDao));
        addStep(new DeleteFileMetadataStepComplete(fileDao, fileId, dataset));
    }

}
