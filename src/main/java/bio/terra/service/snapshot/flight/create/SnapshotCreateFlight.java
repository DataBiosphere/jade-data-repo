package bio.terra.service.snapshot.flight.create;

import bio.terra.service.iam.AuthenticatedUserRequest;
import bio.terra.service.snapshot.SnapshotDao;
import bio.terra.service.filedata.google.firestore.FireStoreDao;
import bio.terra.service.filedata.google.firestore.FireStoreDependencyDao;
import bio.terra.model.SnapshotRequestModel;
import bio.terra.service.tabulardata.google.BigQueryPdao;
import bio.terra.service.filedata.google.gcs.GcsPdao;
import bio.terra.service.dataset.DatasetService;
import bio.terra.service.job.JobMapKeys;
import bio.terra.service.iam.SamClientService;
import bio.terra.service.snapshot.SnapshotService;
import bio.terra.stairway.Flight;
import bio.terra.stairway.FlightMap;
import bio.terra.stairway.UserRequestInfo;
import org.springframework.context.ApplicationContext;

public class SnapshotCreateFlight extends Flight {

    public SnapshotCreateFlight(FlightMap inputParameters, Object applicationContext, UserRequestInfo userRequestInfo) {
        super(inputParameters, applicationContext, userRequestInfo);

        // get the required daos to pass into the steps
        ApplicationContext appContext = (ApplicationContext) applicationContext;
        SnapshotDao snapshotDao = (SnapshotDao)appContext.getBean("snapshotDao");
        SnapshotService snapshotService = (SnapshotService)appContext.getBean("snapshotService");
        BigQueryPdao bigQueryPdao = (BigQueryPdao)appContext.getBean("bigQueryPdao");
        FireStoreDependencyDao dependencyDao = (FireStoreDependencyDao)appContext.getBean("fireStoreDependencyDao");
        FireStoreDao fileDao = (FireStoreDao)appContext.getBean("fireStoreDao");
        SamClientService samClient = (SamClientService)appContext.getBean("samClientService");
        GcsPdao gcsPdao = (GcsPdao) appContext.getBean("gcsPdao");
        DatasetService datasetService = (DatasetService) appContext.getBean("datasetService");

        AuthenticatedUserRequest userReq = inputParameters.get(
            JobMapKeys.AUTH_USER_INFO.getKeyName(), AuthenticatedUserRequest.class);
        SnapshotRequestModel snapshotReq = inputParameters.get(
            JobMapKeys.REQUEST.getKeyName(), SnapshotRequestModel.class);

        // 1. metadata step - create the snapshot object in postgres
        // 2. primary data step - make the big query dataset with views
        // 3. firestore data step - make the firestore file system for the snapshot
        // 4. firestore compute step - calculate checksums and sizes for all directories in the snapshot
        // 5. authorize snapshot - set permissions on BQ and files to enable access
        addStep(new CreateSnapshotMetadataStep(snapshotDao, snapshotService, snapshotReq));
        addStep(new CreateSnapshotPrimaryDataStep(
            bigQueryPdao, snapshotDao, dependencyDao, datasetService, snapshotReq));
        addStep(new CreateSnapshotFireStoreDataStep(
            bigQueryPdao, snapshotService, dependencyDao, datasetService, snapshotReq, fileDao));
        addStep(new CreateSnapshotFireStoreComputeStep(snapshotService, snapshotReq, fileDao));
        addStep(new AuthorizeSnapshot(
            bigQueryPdao, samClient, dependencyDao, snapshotService, gcsPdao, datasetService, snapshotReq, userReq));
    }
}
