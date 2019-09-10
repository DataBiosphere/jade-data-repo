package bio.terra.flight.dataset.ingest;

import bio.terra.filesystem.FireStoreDao;
import bio.terra.pdao.bigquery.BigQueryPdao;
import bio.terra.service.DatasetService;
import bio.terra.stairway.Flight;
import bio.terra.stairway.FlightMap;
import bio.terra.stairway.UserRequestInfo;
import org.springframework.context.ApplicationContext;

public class DatasetIngestFlight extends Flight {

    public DatasetIngestFlight(FlightMap inputParameters, Object applicationContext, UserRequestInfo userRequestInfo) {
        super(inputParameters, applicationContext, userRequestInfo);

        // get the required daos to pass into the steps
        ApplicationContext appContext = (ApplicationContext) applicationContext;
        DatasetService datasetService = (DatasetService) appContext.getBean("datasetService");
        BigQueryPdao bigQueryPdao = (BigQueryPdao)appContext.getBean("bigQueryPdao");
        FireStoreDao fileDao  = (FireStoreDao)appContext.getBean("fireStoreDao");

        addStep(new IngestSetupStep(datasetService));
        addStep(new IngestLoadTableStep(datasetService, bigQueryPdao));
        addStep(new IngestRowIdsStep(datasetService, bigQueryPdao));
        addStep(new IngestValidateRefsStep(datasetService, bigQueryPdao, fileDao));
        addStep(new IngestEvaluateOverlapStep(datasetService, bigQueryPdao));
        addStep(new IngestInsertIntoDatasetTableStep(datasetService, bigQueryPdao));
        addStep(new IngestCleanupStep(datasetService, bigQueryPdao));
    }

}
