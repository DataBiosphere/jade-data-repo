package bio.terra.flight.dataset.delete;

import bio.terra.dao.DatasetDao;
import bio.terra.filesystem.FireStoreDependencyDao;
import bio.terra.pdao.bigquery.BigQueryPdao;
import bio.terra.service.SamClientService;
import bio.terra.stairway.Flight;
import bio.terra.stairway.FlightMap;
import org.springframework.context.ApplicationContext;

import java.util.UUID;

public class DatasetDeleteFlight extends Flight {

    public DatasetDeleteFlight(FlightMap inputParameters, Object applicationContext) {
        super(inputParameters, applicationContext);

        // get the required daos to pass into the steps
        ApplicationContext appContext = (ApplicationContext) applicationContext;
        DatasetDao datasetDao = (DatasetDao)appContext.getBean("datasetDao");
        FireStoreDependencyDao dependencyDao = (FireStoreDependencyDao)appContext.getBean("fireStoreDependencyDao");
        BigQueryPdao bigQueryPdao = (BigQueryPdao)appContext.getBean("bigQueryPdao");
        SamClientService samClient = (SamClientService)appContext.getBean("samClientService");

        UUID datasetId = inputParameters.get("id", UUID.class);

        // Delete access control first so Readers and Discoverers can no longer see dataset
        // Google auto-magically removes the ACLs from files and BQ objects when SAM
        // deletes the dataset group, so no ACL cleanup is needed beyond that.
        addStep(new DeleteDatasetAuthzResource(samClient, datasetId));
        // Must delete primary data before metadata; it relies on being able to retrieveModel the
        // dataset object from the metadata to know what to delete.
        addStep(new DeleteDatasetPrimaryDataStep(bigQueryPdao, datasetDao, dependencyDao, datasetId));
        addStep(new DeleteDatasetMetadataStep(datasetDao, datasetId));
    }
}
