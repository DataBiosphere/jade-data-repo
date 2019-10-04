package bio.terra.flight.dataset.ingest;

import bio.terra.model.IngestRequestModel;
import bio.terra.filedata.google.FireStoreDao;
import bio.terra.pdao.bigquery.BigQueryPdao;
import bio.terra.service.DatasetService;
import bio.terra.service.JobMapKeys;
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
        IngestRequestModel ingestRequestModel = inputParameters.get(JobMapKeys.REQUEST.getKeyName(),
            IngestRequestModel.class);
        IngestRequestModel.StrategyEnum ingestStrategy = ingestRequestModel.getStrategy();

        addStep(new IngestSetupStep(datasetService, bigQueryPdao));
        addStep(new IngestLoadTableStep(datasetService, bigQueryPdao));
        addStep(new IngestRowIdsStep(datasetService, bigQueryPdao));
        addStep(new IngestValidateRefsStep(datasetService, bigQueryPdao, fileDao));
        if (ingestStrategy == IngestRequestModel.StrategyEnum.UPSERT) {
            addStep(new IngestEvaluateOverlapStep(datasetService, bigQueryPdao));
            addStep(new IngestSoftDeleteChangedRowsService(datasetService, bigQueryPdao));
            addStep(new IngestUpsertIntoDatasetTableStep(datasetService, bigQueryPdao));
        } else {
            // 'append' strategy.
            addStep(new IngestInsertIntoDatasetTableStep(datasetService, bigQueryPdao));
        }
        addStep(new IngestCleanupStep(datasetService, bigQueryPdao));
    }
}
