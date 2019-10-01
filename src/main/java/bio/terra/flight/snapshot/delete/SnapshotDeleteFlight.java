package bio.terra.flight.snapshot.delete;

import bio.terra.controller.AuthenticatedUserRequest;
import bio.terra.snapshot.dao.SnapshotDao;
import bio.terra.filesystem.FireStoreDao;
import bio.terra.filesystem.FireStoreDependencyDao;
import bio.terra.pdao.bigquery.BigQueryPdao;
import bio.terra.dataset.service.DatasetService;
import bio.terra.service.JobMapKeys;
import bio.terra.service.SamClientService;
import bio.terra.snapshot.service.SnapshotService;
import bio.terra.stairway.Flight;
import bio.terra.stairway.FlightMap;
import bio.terra.stairway.UserRequestInfo;
import org.springframework.context.ApplicationContext;

import java.util.Map;
import java.util.UUID;

public class SnapshotDeleteFlight extends Flight {

    public SnapshotDeleteFlight(FlightMap inputParameters, Object applicationContext, UserRequestInfo userRequestInfo) {
        super(inputParameters, applicationContext, userRequestInfo);

        // get the required daos to pass into the steps
        ApplicationContext appContext = (ApplicationContext) applicationContext;
        SnapshotDao snapshotDao = (SnapshotDao)appContext.getBean("snapshotDao");
        SnapshotService snapshotService = (SnapshotService) appContext.getBean("snapshotService");
        FireStoreDependencyDao dependencyDao = (FireStoreDependencyDao)appContext.getBean("fireStoreDependencyDao");
        FireStoreDao fileDao = (FireStoreDao)appContext.getBean("fireStoreDao");
        BigQueryPdao bigQueryPdao = (BigQueryPdao)appContext.getBean("bigQueryPdao");
        SamClientService samClient = (SamClientService)appContext.getBean("samClientService");
        DatasetService datasetService = (DatasetService)appContext.getBean("datasetService");

        Map<String, String> pathParams = (Map<String, String>) inputParameters.get(
            JobMapKeys.PATH_PARAMETERS.getKeyName(), Map.class);
        UUID snapshotId = UUID.fromString(pathParams.get(JobMapKeys.SNAPSHOT_ID.getKeyName()));
        AuthenticatedUserRequest userReq = inputParameters.get(
            JobMapKeys.AUTH_USER_INFO.getKeyName(), AuthenticatedUserRequest.class);

        // Delete access control first so Readers and Discoverers can no longer see snapshot
        // Google auto-magically removes the ACLs from files and BQ objects when SAM
        // deletes the snapshot group, so no ACL cleanup is needed beyond that.
        addStep(new DeleteSnapshotAuthzResource(samClient, snapshotId, userReq));
        // Must delete primary data before metadata; it relies on being able to retrieve the
        // snapshot object from the metadata to know what to delete.
        addStep(new DeleteSnapshotPrimaryDataStep(
            bigQueryPdao,
            snapshotService,
            dependencyDao,
            fileDao,
            snapshotId,
            datasetService));
        addStep(new DeleteSnapshotMetadataStep(snapshotDao, snapshotId));
    }
}
