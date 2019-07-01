package bio.terra.flight.dataset.ingest;

import bio.terra.dao.DrDatasetDao;
import bio.terra.filesystem.FireStoreFileDao;
import bio.terra.pdao.bigquery.BigQueryPdao;
import bio.terra.stairway.Flight;
import bio.terra.stairway.FlightMap;
import org.springframework.context.ApplicationContext;

public class DrDatasetIngestFlight extends Flight {

    public DrDatasetIngestFlight(FlightMap inputParameters, Object applicationContext) {
        super(inputParameters, applicationContext);

        // get the required daos to pass into the steps
        ApplicationContext appContext = (ApplicationContext) applicationContext;
        DrDatasetDao datasetDao = (DrDatasetDao)appContext.getBean("drDatasetDao");
        BigQueryPdao bigQueryPdao = (BigQueryPdao)appContext.getBean("bigQueryPdao");
        FireStoreFileDao fileDao  = (FireStoreFileDao)appContext.getBean("fireStoreFileDao");

        addStep(new IngestSetupStep(datasetDao));
        addStep(new IngestLoadTableStep(datasetDao, bigQueryPdao));
        addStep(new IngestRowIdsStep(datasetDao, bigQueryPdao));
        addStep(new IngestValidateRefsStep(datasetDao, bigQueryPdao, fileDao));
        addStep(new IngestInsertIntoDrDatasetTableStep(datasetDao, bigQueryPdao));
        addStep(new IngestCleanupStep(bigQueryPdao));
    }

}
