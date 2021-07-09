package bio.terra.datarepo.service.dataset.flight.datadelete;

import static bio.terra.datarepo.common.FlightUtils.getDefaultRandomBackoffRetryRule;

import bio.terra.datarepo.app.configuration.ApplicationConfiguration;
import bio.terra.datarepo.service.configuration.ConfigurationService;
import bio.terra.datarepo.service.dataset.DatasetDao;
import bio.terra.datarepo.service.dataset.DatasetService;
import bio.terra.datarepo.service.dataset.flight.LockDatasetStep;
import bio.terra.datarepo.service.dataset.flight.UnlockDatasetStep;
import bio.terra.datarepo.service.iam.IamAction;
import bio.terra.datarepo.service.iam.IamProviderInterface;
import bio.terra.datarepo.service.iam.IamResourceType;
import bio.terra.datarepo.service.iam.flight.VerifyAuthorizationStep;
import bio.terra.datarepo.service.job.JobMapKeys;
import bio.terra.datarepo.service.tabulardata.google.BigQueryPdao;
import bio.terra.stairway.Flight;
import bio.terra.stairway.FlightMap;
import bio.terra.stairway.RetryRule;
import java.util.UUID;
import org.springframework.context.ApplicationContext;

public class DatasetDataDeleteFlight extends Flight {

  public DatasetDataDeleteFlight(FlightMap inputParameters, Object applicationContext) {
    super(inputParameters, applicationContext);

    // get the required daos and services to pass into the steps
    ApplicationContext appContext = (ApplicationContext) applicationContext;
    DatasetDao datasetDao = appContext.getBean(DatasetDao.class);
    DatasetService datasetService = appContext.getBean(DatasetService.class);
    BigQueryPdao bigQueryPdao = appContext.getBean(BigQueryPdao.class);
    IamProviderInterface iamClient = appContext.getBean("iamProvider", IamProviderInterface.class);
    ConfigurationService configService = appContext.getBean(ConfigurationService.class);
    ApplicationConfiguration appConfig = appContext.getBean(ApplicationConfiguration.class);

    // get data from inputs that steps need
    String datasetId = inputParameters.get(JobMapKeys.DATASET_ID.getKeyName(), String.class);

    RetryRule lockDatasetRetry =
        getDefaultRandomBackoffRetryRule(appConfig.getMaxStairwayThreads());

    addStep(
        new VerifyAuthorizationStep(
            iamClient, IamResourceType.DATASET, datasetId, IamAction.SOFT_DELETE));

    // need to lock, need dataset name and flight id
    addStep(new LockDatasetStep(datasetDao, UUID.fromString(datasetId), true), lockDatasetRetry);

    // validate tables exist, check access to files, and create external temp tables
    addStep(new CreateExternalTablesStep(bigQueryPdao, datasetService));

    // insert into soft delete table
    addStep(new DataDeletionStep(bigQueryPdao, datasetService, configService));

    // unlock
    addStep(new UnlockDatasetStep(datasetDao, UUID.fromString(datasetId), true), lockDatasetRetry);

    // cleanup
    addStep(new DropExternalTablesStep(bigQueryPdao, datasetService));
  }
}
