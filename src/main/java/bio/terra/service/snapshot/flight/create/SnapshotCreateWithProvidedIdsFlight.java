package bio.terra.service.snapshot.flight.create;

import bio.terra.model.SnapshotProvidedIdsRequestModel;
import bio.terra.service.iam.AuthenticatedUserRequest;
import bio.terra.service.snapshot.SnapshotDao;
import bio.terra.service.filedata.google.firestore.FireStoreDao;
import bio.terra.service.filedata.google.firestore.FireStoreDependencyDao;
import bio.terra.service.snapshot.SnapshotProvidedIdsRequest;
import bio.terra.service.tabulardata.google.BigQueryPdao;
import bio.terra.service.filedata.google.gcs.GcsPdao;
import bio.terra.service.dataset.DatasetService;
import bio.terra.service.job.JobMapKeys;
import bio.terra.service.iam.IamService;
import bio.terra.service.snapshot.SnapshotService;
import bio.terra.stairway.Flight;
import bio.terra.stairway.FlightMap;
import bio.terra.stairway.UserRequestInfo;
import org.springframework.context.ApplicationContext;

public class SnapshotCreateWithProvidedIdsFlight extends Flight {

    public SnapshotCreateWithProvidedIdsFlight(
        FlightMap inputParameters,
        Object applicationContext,
        UserRequestInfo userRequestInfo) {
        super(inputParameters, applicationContext, userRequestInfo);

        // get the required daos to pass into the steps
        ApplicationContext appContext = (ApplicationContext) applicationContext;
        SnapshotDao snapshotDao = (SnapshotDao)appContext.getBean("snapshotDao");
        SnapshotService snapshotService = (SnapshotService)appContext.getBean("snapshotService");
        BigQueryPdao bigQueryPdao = (BigQueryPdao)appContext.getBean("bigQueryPdao");
        FireStoreDependencyDao dependencyDao = (FireStoreDependencyDao)appContext.getBean("fireStoreDependencyDao");
        FireStoreDao fileDao = (FireStoreDao)appContext.getBean("fireStoreDao");
        IamService iamClient = (IamService)appContext.getBean("iamService");
        GcsPdao gcsPdao = (GcsPdao) appContext.getBean("gcsPdao");
        DatasetService datasetService = (DatasetService) appContext.getBean("datasetService");

        AuthenticatedUserRequest userReq = inputParameters.get(
            JobMapKeys.AUTH_USER_INFO.getKeyName(), AuthenticatedUserRequest.class);
        SnapshotProvidedIdsRequestModel snapshotReqModel = inputParameters.get(
            JobMapKeys.REQUEST.getKeyName(), SnapshotProvidedIdsRequestModel.class);
        SnapshotProvidedIdsRequest spiReq = new SnapshotProvidedIdsRequest(snapshotReqModel);

        // 1. metadata step - create the snapshot object in postgres
        // 2. primary data step - make the big query dataset with views
        // 3. firestore data step - make the firestore file system for the snapshot
        // 4. firestore compute step - calculate checksums and sizes for all directories in the snapshot
        // 5. authorize snapshot - set permissions on BQ and files to enable access
        addStep(new CreateSnapshotMetadataStep(snapshotDao, snapshotService, spiReq));
        addStep(new CreateSnapshotWithProvidedIdsPrimaryDataStep(bigQueryPdao, snapshotDao, snapshotReqModel));
        addStep(new CreateSnapshotFireStoreDataStep(
            bigQueryPdao, snapshotService, dependencyDao, datasetService, spiReq, fileDao));
        addStep(new CreateSnapshotFireStoreComputeStep(snapshotService, spiReq, fileDao));
        addStep(new AuthorizeSnapshot(
            bigQueryPdao, iamClient, dependencyDao, snapshotService, gcsPdao, datasetService, spiReq, userReq));
    }
}
