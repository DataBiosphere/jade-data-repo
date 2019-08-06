package bio.terra.flight.snapshot.create;

import bio.terra.controller.AuthenticatedUser;
import bio.terra.dao.SnapshotDao;
import bio.terra.filesystem.FireStoreDependencyDao;
import bio.terra.pdao.bigquery.BigQueryPdao;
import bio.terra.pdao.gcs.GcsPdao;
import bio.terra.service.SnapshotService;
import bio.terra.service.SamClientService;
import bio.terra.service.DatasetService;
import bio.terra.stairway.Flight;
import bio.terra.stairway.FlightMap;
import org.springframework.context.ApplicationContext;

public class SnapshotCreateFlight extends Flight {

    public SnapshotCreateFlight(FlightMap inputParameters, Object applicationContext, AuthenticatedUser userInfo) {
        super(inputParameters, applicationContext, userInfo);

        // get the required daos to pass into the steps
        ApplicationContext appContext = (ApplicationContext) applicationContext;
        SnapshotDao snapshotDao = (SnapshotDao)appContext.getBean("snapshotDao");
        SnapshotService snapshotService = (SnapshotService)appContext.getBean("snapshotService");
        BigQueryPdao bigQueryPdao = (BigQueryPdao)appContext.getBean("bigQueryPdao");
        FireStoreDependencyDao dependencyDao = (FireStoreDependencyDao)appContext.getBean("fireStoreDependencyDao");
        SamClientService samClient = (SamClientService)appContext.getBean("samClientService");
        GcsPdao gcsPdao = (GcsPdao) appContext.getBean("gcsPdao");
        DatasetService datasetService = (DatasetService) appContext.getBean("datasetService");

        addStep(new CreateSnapshotMetadataStep(snapshotDao, snapshotService));
        addStep(new CreateSnapshotPrimaryDataStep(bigQueryPdao, snapshotDao, dependencyDao, datasetService));
        addStep(new AuthorizeSnapshot(bigQueryPdao, samClient, dependencyDao, snapshotDao, gcsPdao, datasetService));
    }
}
