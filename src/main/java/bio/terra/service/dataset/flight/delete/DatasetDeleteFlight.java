package bio.terra.service.dataset.flight.delete;

import bio.terra.service.iam.AuthenticatedUserRequest;
import bio.terra.service.dataset.DatasetDao;
import bio.terra.service.snapshot.SnapshotDao;
import bio.terra.service.filedata.google.firestore.FireStoreDao;
import bio.terra.service.filedata.google.firestore.FireStoreDependencyDao;
import bio.terra.service.tabulardata.google.BigQueryPdao;
import bio.terra.service.filedata.google.gcs.GcsPdao;
import bio.terra.service.dataset.DatasetService;
import bio.terra.service.job.JobMapKeys;
import bio.terra.service.iam.SamClientService;
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
