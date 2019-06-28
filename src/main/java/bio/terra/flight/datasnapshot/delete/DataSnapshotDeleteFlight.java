package bio.terra.flight.datasnapshot.delete;

import bio.terra.dao.DataSnapshotDao;
import bio.terra.filesystem.FireStoreDependencyDao;
import bio.terra.pdao.bigquery.BigQueryPdao;
import bio.terra.service.SamClientService;
import bio.terra.stairway.Flight;
import bio.terra.stairway.FlightMap;
import org.springframework.context.ApplicationContext;

import java.util.UUID;

public class DataSnapshotDeleteFlight extends Flight {

    public DataSnapshotDeleteFlight(FlightMap inputParameters, Object applicationContext) {
        super(inputParameters, applicationContext);

        // get the required daos to pass into the steps
        ApplicationContext appContext = (ApplicationContext) applicationContext;
        DataSnapshotDao dataSnapshotDao = (DataSnapshotDao)appContext.getBean("dataSnapshotDao");
        FireStoreDependencyDao dependencyDao = (FireStoreDependencyDao)appContext.getBean("fireStoreDependencyDao");
        BigQueryPdao bigQueryPdao = (BigQueryPdao)appContext.getBean("bigQueryPdao");
        SamClientService samClient = (SamClientService)appContext.getBean("samClientService");

        UUID datasetId = inputParameters.get("id", UUID.class);

        // Delete access control first so Readers and Discoverers can no longer see data snapshot
        addStep(new DeleteDataSnapshotAuthzResource(samClient, datasetId));
        // Must delete primary data before metadata; it relies on being able to retrieve the
        // data snapshot object from the metadata to know what to delete.
        addStep(new DeleteDataSnapshotPrimaryDataStep(bigQueryPdao, dataSnapshotDao, datasetId));
        addStep(new DeleteDataSnapshotMetadataStep(dataSnapshotDao, datasetId, dependencyDao));
    }
}
