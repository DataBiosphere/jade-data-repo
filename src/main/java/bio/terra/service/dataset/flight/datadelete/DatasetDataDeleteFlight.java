package bio.terra.service.dataset.flight.datadelete;

import static bio.terra.common.FlightUtils.getDefaultRandomBackoffRetryRule;

import bio.terra.app.configuration.ApplicationConfiguration;
import bio.terra.service.configuration.ConfigurationService;
import bio.terra.service.dataset.DatasetDao;
import bio.terra.service.dataset.DatasetService;
import bio.terra.service.dataset.flight.LockDatasetStep;
import bio.terra.service.dataset.flight.UnlockDatasetStep;
import bio.terra.service.filedata.flight.ingest.CreateBucketForBigQueryScratchStep;
import bio.terra.service.filedata.google.gcs.GcsPdao;
import bio.terra.service.iam.IamAction;
import bio.terra.service.iam.IamProviderInterface;
import bio.terra.service.iam.IamResourceType;
import bio.terra.service.iam.flight.VerifyAuthorizationStep;
import bio.terra.service.job.JobMapKeys;
import bio.terra.service.resourcemanagement.ResourceService;
import bio.terra.service.tabulardata.google.BigQueryPdao;
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
    ResourceService resourceService = appContext.getBean(ResourceService.class);
    GcsPdao gcsPdao = appContext.getBean(GcsPdao.class);

    // get data from inputs that steps need
    String datasetId = inputParameters.get(JobMapKeys.DATASET_ID.getKeyName(), String.class);

    RetryRule lockDatasetRetry =
        getDefaultRandomBackoffRetryRule(appConfig.getMaxStairwayThreads());

    addStep(
        new VerifyAuthorizationStep(
            iamClient, IamResourceType.DATASET, datasetId, IamAction.SOFT_DELETE));

    // need to lock, need dataset name and flight id
    addStep(new LockDatasetStep(datasetDao, UUID.fromString(datasetId), true), lockDatasetRetry);

    // See if we need to copy the control file to a scratch bucket in the same region as BQ
    addStep(new DataDeletionControlFileCopyNeededStep(datasetService, gcsPdao));

    // If we need to copy, make (or get) the scratch bucket
    addStep(
        new CreateBucketForBigQueryScratchStep(
            resourceService, datasetService, DataDeletionUtils.scratchFileCopyNotNeeded),
        getDefaultRandomBackoffRetryRule(appConfig.getMaxStairwayThreads()));

    // If we need to copy, copy to the scratch bucket
    addStep(
        new DataDeletionCopyFilesToBigQueryScratchBucketStep(
            datasetService, gcsPdao, DataDeletionUtils.scratchFileCopyNotNeeded));

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
