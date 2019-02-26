package bio.terra.flight.dataset.delete;

import bio.terra.dao.DatasetDao;
import bio.terra.pdao.bigquery.BigQueryPdao;
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
        BigQueryPdao bigQueryPdao = (BigQueryPdao)appContext.getBean("bigQueryPdao");

        String id = inputParameters.get("id", String.class);
        UUID datasetId = UUID.fromString(id);

        // Must delete primary data first; it relies on being able to retrieve the
        // dataset object from the metadata to know what to delete.
        addStep(new DeleteDatasetPrimaryDataStep(bigQueryPdao, datasetDao, datasetId));
        addStep(new DeleteDatasetMetadataStep(datasetDao, datasetId));
    }
}
