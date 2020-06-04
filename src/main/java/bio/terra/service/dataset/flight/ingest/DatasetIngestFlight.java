package bio.terra.service.dataset.flight.ingest;

import bio.terra.app.configuration.ApplicationConfiguration;
import bio.terra.service.configuration.ConfigurationService;
import bio.terra.service.dataset.DatasetDao;
import bio.terra.service.dataset.DatasetService;
import bio.terra.service.dataset.flight.LockDatasetStep;
import bio.terra.service.dataset.flight.UnlockDatasetStep;
import bio.terra.service.filedata.google.firestore.FireStoreDao;
import bio.terra.service.job.JobMapKeys;
import bio.terra.service.tabulardata.google.BigQueryPdao;
import bio.terra.stairway.Flight;
import bio.terra.stairway.FlightMap;
import bio.terra.stairway.RetryRuleRandomBackoff;
import org.springframework.context.ApplicationContext;

import java.util.UUID;

public class DatasetIngestFlight extends Flight {

    public DatasetIngestFlight(FlightMap inputParameters, Object applicationContext) {
        super(inputParameters, applicationContext);

        // get the required daos to pass into the steps
        ApplicationContext appContext = (ApplicationContext) applicationContext;
        DatasetDao datasetDao = (DatasetDao) appContext.getBean("datasetDao");
        DatasetService datasetService = (DatasetService) appContext.getBean("datasetService");
        BigQueryPdao bigQueryPdao = (BigQueryPdao)appContext.getBean("bigQueryPdao");
        FireStoreDao fileDao  = (FireStoreDao)appContext.getBean("fireStoreDao");
        ConfigurationService configService = (ConfigurationService)appContext.getBean("configurationService");
        ApplicationConfiguration appConfig =
            (ApplicationConfiguration)appContext.getBean("applicationConfiguration");

        // get data from inputs that steps need
        UUID datasetId = UUID.fromString(inputParameters.get(JobMapKeys.DATASET_ID.getKeyName(), String.class));

        RetryRuleRandomBackoff lockDatasetRetry =
            new RetryRuleRandomBackoff(500, appConfig.getMaxStairwayThreads(), 5);

        addStep(new LockDatasetStep(datasetDao, datasetId, true), lockDatasetRetry);
        addStep(new IngestSetupStep(datasetService, configService));
        addStep(new IngestLoadTableStep(datasetService, bigQueryPdao));
        addStep(new IngestRowIdsStep(datasetService, bigQueryPdao));
        addStep(new IngestValidateRefsStep(datasetService, bigQueryPdao, fileDao));
        addStep(new IngestInsertIntoDatasetTableStep(datasetService, bigQueryPdao));
        addStep(new IngestCleanupStep(datasetService, bigQueryPdao));
        addStep(new UnlockDatasetStep(datasetDao, datasetId, true));
    }
}
