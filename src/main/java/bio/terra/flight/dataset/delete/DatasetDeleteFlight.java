package bio.terra.flight.dataset.delete;

import bio.terra.controller.AuthenticatedUserRequest;
import bio.terra.dao.DatasetDao;
import bio.terra.dao.SnapshotDao;
import bio.terra.filesystem.FireStoreDao;
import bio.terra.filesystem.FireStoreDependencyDao;
import bio.terra.pdao.bigquery.BigQueryPdao;
import bio.terra.pdao.gcs.GcsPdao;
import bio.terra.service.DatasetService;
import bio.terra.service.JobMapKeys;
import bio.terra.service.SamClientService;
import bio.terra.stairway.Flight;
import bio.terra.stairway.FlightMap;
import bio.terra.stairway.UserRequestInfo;
import org.springframework.context.ApplicationContext;

import java.util.Map;
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
        /*Map<String, String> pathParams = (Map<String, String>) inputParameters.get(
            JobMapKeys.PATH_PARAMETERS.getKeyName(), Map.class);
        UUID datasetId = UUID.fromString(pathParams.get(JobMapKeys.DATASET_ID.getKeyName()));*/
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
