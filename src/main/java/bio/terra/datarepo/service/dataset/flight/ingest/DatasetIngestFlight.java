package bio.terra.datarepo.service.dataset.flight.ingest;

import static bio.terra.datarepo.common.FlightUtils.getDefaultRandomBackoffRetryRule;

import bio.terra.datarepo.app.configuration.ApplicationConfiguration;
import bio.terra.datarepo.service.configuration.ConfigurationService;
import bio.terra.datarepo.service.dataset.DatasetDao;
import bio.terra.datarepo.service.dataset.DatasetService;
import bio.terra.datarepo.service.dataset.flight.LockDatasetStep;
import bio.terra.datarepo.service.dataset.flight.UnlockDatasetStep;
import bio.terra.datarepo.service.filedata.google.firestore.FireStoreDao;
import bio.terra.datarepo.service.job.JobMapKeys;
import bio.terra.datarepo.service.tabulardata.google.BigQueryPdao;
import bio.terra.stairway.Flight;
import bio.terra.stairway.FlightMap;
import bio.terra.stairway.RetryRule;
import java.util.UUID;
import org.springframework.context.ApplicationContext;

public class DatasetIngestFlight extends Flight {

  public DatasetIngestFlight(FlightMap inputParameters, Object applicationContext) {
    super(inputParameters, applicationContext);

    // get the required daos to pass into the steps
    ApplicationContext appContext = (ApplicationContext) applicationContext;
    DatasetDao datasetDao = appContext.getBean(DatasetDao.class);
    DatasetService datasetService = appContext.getBean(DatasetService.class);
    BigQueryPdao bigQueryPdao = appContext.getBean(BigQueryPdao.class);
    FireStoreDao fileDao = appContext.getBean(FireStoreDao.class);
    ConfigurationService configService = appContext.getBean(ConfigurationService.class);
    ApplicationConfiguration appConfig = appContext.getBean(ApplicationConfiguration.class);

    // get data from inputs that steps need
    UUID datasetId =
        UUID.fromString(inputParameters.get(JobMapKeys.DATASET_ID.getKeyName(), String.class));

    RetryRule lockDatasetRetry =
        getDefaultRandomBackoffRetryRule(appConfig.getMaxStairwayThreads());

    addStep(new LockDatasetStep(datasetDao, datasetId, true), lockDatasetRetry);
    addStep(new IngestSetupStep(datasetService, configService));
    addStep(new IngestLoadTableStep(datasetService, bigQueryPdao));
    addStep(new IngestRowIdsStep(datasetService, bigQueryPdao));
    addStep(new IngestValidateRefsStep(datasetService, bigQueryPdao, fileDao));
    addStep(new IngestInsertIntoDatasetTableStep(datasetService, bigQueryPdao));
    addStep(new IngestCleanupStep(datasetService, bigQueryPdao));
    addStep(new UnlockDatasetStep(datasetDao, datasetId, true), lockDatasetRetry);
  }
}
