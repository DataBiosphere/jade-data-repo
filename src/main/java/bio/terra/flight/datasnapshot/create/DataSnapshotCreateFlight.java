package bio.terra.flight.datasnapshot.create;

import bio.terra.dao.DataSnapshotDao;
import bio.terra.filesystem.FireStoreDependencyDao;
import bio.terra.pdao.bigquery.BigQueryPdao;
import bio.terra.service.DatasetService;
import bio.terra.service.SamClientService;
import bio.terra.stairway.Flight;
import bio.terra.stairway.FlightMap;
import org.springframework.context.ApplicationContext;

public class DataSnapshotCreateFlight extends Flight {

    public DataSnapshotCreateFlight(FlightMap inputParameters, Object applicationContext) {
        super(inputParameters, applicationContext);

        // get the required daos to pass into the steps
        ApplicationContext appContext = (ApplicationContext) applicationContext;
        DataSnapshotDao dataSnapshotDao = (DataSnapshotDao)appContext.getBean("dataSnapshotDao");
        DatasetService datasetService = (DatasetService)appContext.getBean("datasetService");
        BigQueryPdao bigQueryPdao = (BigQueryPdao)appContext.getBean("bigQueryPdao");
        FireStoreDependencyDao dependencyDao = (FireStoreDependencyDao)appContext.getBean("fireStoreDependencyDao");
        SamClientService samClient = (SamClientService)appContext.getBean("samClientService");


        addStep(new CreateDataSnapshotMetadataStep(dataSnapshotDao, datasetService));
        addStep(new CreateDataSnapshotPrimaryDataStep(bigQueryPdao, datasetService, dataSnapshotDao, dependencyDao));
        addStep(new AuthorizeDataSnapshot(bigQueryPdao, samClient));
    }
}
