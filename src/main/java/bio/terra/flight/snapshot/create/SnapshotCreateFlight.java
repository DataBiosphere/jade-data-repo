package bio.terra.flight.snapshot.create;

import bio.terra.controller.AuthenticatedUserRequest;
import bio.terra.dao.SnapshotDao;
import bio.terra.filesystem.FireStoreDependencyDao;
import bio.terra.model.SnapshotRequestModel;
import bio.terra.pdao.bigquery.BigQueryPdao;
import bio.terra.pdao.gcs.GcsPdao;
import bio.terra.service.DatasetService;
import bio.terra.service.JobMapKeys;
import bio.terra.service.SamClientService;
import bio.terra.service.SnapshotService;
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
        SamClientService samClient = (SamClientService)appContext.getBean("samClientService");
        GcsPdao gcsPdao = (GcsPdao) appContext.getBean("gcsPdao");
        DatasetService datasetService = (DatasetService) appContext.getBean("datasetService");

        AuthenticatedUserRequest userReq = inputParameters.get(
            JobMapKeys.AUTH_USER_INFO.getKeyName(), AuthenticatedUserRequest.class);
        SnapshotRequestModel snapshotReq = inputParameters.get(
            JobMapKeys.REQUEST.getKeyName(), SnapshotRequestModel.class);

        addStep(new CreateSnapshotMetadataStep(snapshotDao, snapshotService, snapshotReq));
        addStep(new CreateSnapshotPrimaryDataStep(
            bigQueryPdao, snapshotDao, dependencyDao, datasetService, snapshotReq));
        addStep(new AuthorizeSnapshot(
            bigQueryPdao, samClient, dependencyDao, snapshotDao, gcsPdao, datasetService, snapshotReq, userReq));
    }
}
