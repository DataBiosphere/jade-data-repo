package bio.terra.flight.snapshot.delete;

import bio.terra.stairway.UserRequestInfo;
import bio.terra.dao.SnapshotDao;
import bio.terra.filesystem.FireStoreDependencyDao;
import bio.terra.pdao.bigquery.BigQueryPdao;
import bio.terra.service.DatasetService;
import bio.terra.service.JobMapKeys;
import bio.terra.service.SamClientService;
import bio.terra.stairway.Flight;
import bio.terra.stairway.FlightMap;
import org.springframework.context.ApplicationContext;

import java.util.UUID;

public class SnapshotDeleteFlight extends Flight {

    public SnapshotDeleteFlight(FlightMap inputParameters, Object applicationContext, UserRequestInfo userRequestInfo) {
        super(inputParameters, applicationContext, userRequestInfo);

        // get the required daos to pass into the steps
        ApplicationContext appContext = (ApplicationContext) applicationContext;
        SnapshotDao snapshotDao = (SnapshotDao)appContext.getBean("snapshotDao");
        FireStoreDependencyDao dependencyDao = (FireStoreDependencyDao)appContext.getBean("fireStoreDependencyDao");
        BigQueryPdao bigQueryPdao = (BigQueryPdao)appContext.getBean("bigQueryPdao");
        SamClientService samClient = (SamClientService)appContext.getBean("samClientService");
        DatasetService datasetService = (DatasetService)appContext.getBean("datasetService");
        UUID snapshotId = inputParameters.get(JobMapKeys.REQUEST.getKeyName(), UUID.class);

        // Delete access control first so Readers and Discoverers can no longer see snapshot
        // Google auto-magically removes the ACLs from files and BQ objects when SAM
        // deletes the snapshot group, so no ACL cleanup is needed beyond that.
        addStep(new DeleteSnapshotAuthzResource(samClient, snapshotId));
        // Must delete primary data before metadata; it relies on being able to retrieve the
        // snapshot object from the metadata to know what to delete.
        addStep(
            new DeleteSnapshotPrimaryDataStep(bigQueryPdao, snapshotDao, dependencyDao, snapshotId, datasetService));
        addStep(new DeleteSnapshotMetadataStep(snapshotDao, snapshotId));
    }
}
