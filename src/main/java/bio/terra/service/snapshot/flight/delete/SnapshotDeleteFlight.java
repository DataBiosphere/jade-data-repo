package bio.terra.service.snapshot.flight.delete;

import bio.terra.service.iam.AuthenticatedUserRequest;
import bio.terra.service.snapshot.SnapshotDao;
import bio.terra.service.filedata.google.firestore.FireStoreDao;
import bio.terra.service.filedata.google.firestore.FireStoreDependencyDao;
import bio.terra.service.tabulardata.google.BigQueryPdao;
import bio.terra.service.dataset.DatasetService;
import bio.terra.service.job.JobMapKeys;
import bio.terra.service.iam.SamClientService;
import bio.terra.service.snapshot.SnapshotService;
import bio.terra.stairway.Flight;
import bio.terra.stairway.FlightMap;
import bio.terra.stairway.UserRequestInfo;
import org.springframework.context.ApplicationContext;

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

        UUID snapshotId = UUID.fromString(inputParameters.get(
            JobMapKeys.SNAPSHOT_ID.getKeyName(), String.class));
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
