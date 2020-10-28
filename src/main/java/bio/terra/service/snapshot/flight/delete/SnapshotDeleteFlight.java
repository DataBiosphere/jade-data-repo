package bio.terra.service.snapshot.flight.delete;

import bio.terra.service.configuration.ConfigurationService;
import bio.terra.service.dataset.DatasetService;
import bio.terra.service.filedata.google.firestore.FireStoreDao;
import bio.terra.service.filedata.google.firestore.FireStoreDependencyDao;
import bio.terra.service.iam.AuthenticatedUserRequest;
import bio.terra.service.iam.IamService;
import bio.terra.service.job.JobMapKeys;
import bio.terra.service.resourcemanagement.DataLocationService;
import bio.terra.service.resourcemanagement.google.GoogleResourceService;
import bio.terra.service.snapshot.SnapshotDao;
import bio.terra.service.snapshot.SnapshotService;
import bio.terra.service.snapshot.flight.LockSnapshotStep;
import bio.terra.service.snapshot.flight.UnlockSnapshotStep;
import bio.terra.service.tabulardata.google.BigQueryPdao;
import bio.terra.stairway.Flight;
import bio.terra.stairway.FlightMap;
import org.springframework.context.ApplicationContext;

import java.util.UUID;

public class SnapshotDeleteFlight extends Flight {

    public SnapshotDeleteFlight(FlightMap inputParameters, Object applicationContext) {
        super(inputParameters, applicationContext);

        // get the required daos to pass into the steps
        ApplicationContext appContext = (ApplicationContext) applicationContext;
        SnapshotDao snapshotDao = (SnapshotDao)appContext.getBean("snapshotDao");
        SnapshotService snapshotService = (SnapshotService) appContext.getBean("snapshotService");
        FireStoreDependencyDao dependencyDao = (FireStoreDependencyDao)appContext.getBean("fireStoreDependencyDao");
        FireStoreDao fileDao = (FireStoreDao)appContext.getBean("fireStoreDao");
        BigQueryPdao bigQueryPdao = (BigQueryPdao)appContext.getBean("bigQueryPdao");
        DataLocationService dataLocationService = (DataLocationService) appContext.getBean("dataLocationService");
        GoogleResourceService resourceService = (GoogleResourceService) appContext.getBean("googleResourceService");
        IamService iamClient = (IamService)appContext.getBean("iamService");
        DatasetService datasetService = (DatasetService)appContext.getBean("datasetService");
        ConfigurationService configService = (ConfigurationService)appContext.getBean("configurationService");

        UUID snapshotId = UUID.fromString(inputParameters.get(
            JobMapKeys.SNAPSHOT_ID.getKeyName(), String.class));
        AuthenticatedUserRequest userReq = inputParameters.get(
            JobMapKeys.AUTH_USER_INFO.getKeyName(), AuthenticatedUserRequest.class);

        addStep(new LockSnapshotStep(snapshotDao, snapshotId, true));
        // Delete access control on objects that was explicitly added by data repo operations.  Do this before delete
        // resource from SAM to ensure we can get the metadata needed to perform the operation
        addStep(new DeleteSnapshotAuthzBqAcls(
            iamClient,
            resourceService,
            dataLocationService,
            snapshotService,
            snapshotId,
            userReq));
        // Delete access control first so Readers and Discoverers can no longer see snapshot
        // Google auto-magically removes the ACLs from BQ objects when SAM
        // deletes the snapshot group, so no ACL cleanup is needed beyond that.
        addStep(new DeleteSnapshotAuthzResource(iamClient, snapshotId, userReq));
        // Must delete primary data before metadata; it relies on being able to retrieve the
        // snapshot object from the metadata to know what to delete.
        addStep(new DeleteSnapshotPrimaryDataStep(
            bigQueryPdao,
            snapshotService,
            dependencyDao,
            fileDao,
            snapshotId,
            datasetService,
            configService));
        addStep(new DeleteSnapshotMetadataStep(snapshotDao, snapshotId));
        addStep(new UnlockSnapshotStep(snapshotDao, snapshotId));
    }
}
