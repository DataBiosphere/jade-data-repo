package bio.terra.service.filedata.flight.delete;

import bio.terra.service.filedata.google.firestore.FireStoreDao;
import bio.terra.service.filedata.google.firestore.FireStoreDependencyDao;
import bio.terra.service.dataset.Dataset;
import bio.terra.service.filedata.google.gcs.GcsPdao;
import bio.terra.service.dataset.DatasetService;
import bio.terra.service.job.JobMapKeys;
import bio.terra.service.resourcemanagement.DataLocationService;
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
        String fileId = inputParameters.get(JobMapKeys.FILE_ID.getKeyName(), String.class);

        // TODO: fix this
        // Error handling within this constructor results in an obscure throw from
        // Java (INVOCATION_EXCEPTION), instead of getting a good DATASET_NOT_FOUND error.
        // We should NOT put code like that in the flight constructor.
        Dataset dataset = datasetService.retrieve(UUID.fromString(datasetId));

        // The flight plan:
        // 1. Lookup file and store the file data in the flight map. Check dependencies to make sure that the
        //    delete is allowed. We do the lookup and store so that we have all of the file information, since
        //    once we start deleting things, we can't look it up again!
        // 2. Delete the file object - after this point, the file is not shown through the REST API.
        // 3. pdao GCS delete the file
        // 4. Delete the directory entry
        // This flight updates GCS and firestore in exactly the reverse order of create, so no new
        // data structure states are introduced by this flight.
        addStep(new DeleteFileLookupStep(fileDao, fileId, dataset, dependencyDao));
        addStep(new DeleteFileMetadataStep(fileDao, fileId, dataset));
        addStep(new DeleteFilePrimaryDataStep(dataset, fileId, gcsPdao, fileDao, locationService));
        addStep(new DeleteFileDirectoryStep(fileDao, fileId, dataset));
    }

}
