package bio.terra.flight.dataset.delete;

import bio.terra.controller.AuthenticatedUserRequest;
import bio.terra.dataset.DatasetDao;
import bio.terra.snapshot.SnapshotDao;
import bio.terra.filedata.google.firestore.FireStoreDao;
import bio.terra.filedata.google.firestore.FireStoreDependencyDao;
import bio.terra.tabulardata.google.BigQueryPdao;
import bio.terra.filedata.google.gcs.GcsPdao;
import bio.terra.dataset.DatasetService;
import bio.terra.service.JobMapKeys;
import bio.terra.service.SamClientService;
import bio.terra.stairway.Flight;
import bio.terra.stairway.FlightMap;
import bio.terra.stairway.UserRequestInfo;
import org.springframework.context.ApplicationContext;

import java.util.UUID;

public class DatasetDeleteFlight extends Flight {



    public DatasetDeleteFlight(FlightMap inputParameters, Object applicationContext, UserRequestInfo userRequestInfo) {
        super(inputParameters, applicationContext, userRequestInfo);

        // get the required daos to pass into the steps
        ApplicationContext appContext = (ApplicationContext) applicationContext;
        DatasetDao datasetDao = (DatasetDao)appContext.getBean("datasetDao");
        SnapshotDao snapshotDao = (SnapshotDao)appContext.getBean("snapshotDao");
        BigQueryPdao bigQueryPdao = (BigQueryPdao)appContext.getBean("bigQueryPdao");
        GcsPdao gcsPdao = (GcsPdao)appContext.getBean("gcsPdao");
        FireStoreDependencyDao dependencyDao = (FireStoreDependencyDao)appContext.getBean("fireStoreDependencyDao");
        FireStoreDao fileDao = (FireStoreDao)appContext.getBean("fireStoreDao");
        SamClientService samClient = (SamClientService)appContext.getBean("samClientService");
        DatasetService datasetService = (DatasetService) appContext.getBean("datasetService");

        // get data from inputs that steps need
        UUID datasetId = UUID.fromString(inputParameters.get(
            JobMapKeys.DATASET_ID.getKeyName(), String.class));
        AuthenticatedUserRequest userReq = inputParameters.get(
            JobMapKeys.AUTH_USER_INFO.getKeyName(), AuthenticatedUserRequest.class);

        addStep(new DeleteDatasetValidateStep(snapshotDao, dependencyDao, datasetService, datasetId));
        addStep(new DeleteDatasetPrimaryDataStep(bigQueryPdao, gcsPdao, fileDao, datasetService, datasetId));
        addStep(new DeleteDatasetMetadataStep(datasetDao, datasetId));
        addStep(new DeleteDatasetAuthzResource(samClient, datasetId, userReq));
    }
}
