package bio.terra.service.dataset.flight.datadelete;

import static bio.terra.common.FlightUtils.getDefaultRandomBackoffRetryRule;

import bio.terra.app.configuration.ApplicationConfiguration;
import bio.terra.common.ValidateBucketAccessStep;
import bio.terra.common.iam.AuthenticatedUserRequest;
import bio.terra.model.DataDeletionRequest;
import bio.terra.service.configuration.ConfigurationService;
import bio.terra.service.dataset.DatasetService;
import bio.terra.service.dataset.flight.LockDatasetStep;
import bio.terra.service.dataset.flight.UnlockDatasetStep;
import bio.terra.service.dataset.flight.xactions.TransactionCommitStep;
import bio.terra.service.dataset.flight.xactions.TransactionLockStep;
import bio.terra.service.dataset.flight.xactions.TransactionOpenStep;
import bio.terra.service.dataset.flight.xactions.TransactionUnlockStep;
import bio.terra.service.dataset.flight.xactions.upgrade.TransactionUpgradeSingleDatasetStep;
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
    DatasetService datasetService = appContext.getBean(DatasetService.class);
    BigQueryPdao bigQueryPdao = appContext.getBean(BigQueryPdao.class);
    IamProviderInterface iamClient = appContext.getBean("iamProvider", IamProviderInterface.class);
    ConfigurationService configService = appContext.getBean(ConfigurationService.class);
    ApplicationConfiguration appConfig = appContext.getBean(ApplicationConfiguration.class);
    ResourceService resourceService = appContext.getBean(ResourceService.class);
    GcsPdao gcsPdao = appContext.getBean(GcsPdao.class);

    // get data from inputs that steps need
    String datasetId = inputParameters.get(JobMapKeys.DATASET_ID.getKeyName(), String.class);

    AuthenticatedUserRequest userReq =
        inputParameters.get(JobMapKeys.AUTH_USER_INFO.getKeyName(), AuthenticatedUserRequest.class);

    RetryRule lockDatasetRetry =
        getDefaultRandomBackoffRetryRule(appConfig.getMaxStairwayThreads());

    DataDeletionRequest request =
        inputParameters.get(JobMapKeys.REQUEST.getKeyName(), DataDeletionRequest.class);

    addStep(
        new VerifyAuthorizationStep(
            iamClient, IamResourceType.DATASET, datasetId, IamAction.SOFT_DELETE));

    if (request.getSpecType() == DataDeletionRequest.SpecTypeEnum.GCSFILE) {
      addStep(new ValidateBucketAccessStep(gcsPdao, userReq));
    }

    // Migrate the dataset schema if needed
    addStep(
        new TransactionUpgradeSingleDatasetStep(
            datasetService, bigQueryPdao, UUID.fromString(datasetId)));
    // need to lock, need dataset name and flight id
    addStep(
        new LockDatasetStep(datasetService, UUID.fromString(datasetId), true), lockDatasetRetry);

    // Note: don't worry about Azure until we tackle DR-2252
    boolean autocommit;
    if (request.getTransactionId() == null) {
      // Note: don't unlock transaction so we keep a history of what flight an auto-commit
      // transaction was created for
      String transactionDesc = "Autocommit transaction";
      addStep(
          new TransactionOpenStep(
              datasetService, bigQueryPdao, userReq, transactionDesc, false, false));
      autocommit = true;
    } else {
      addStep(
          new TransactionLockStep(
              datasetService, bigQueryPdao, request.getTransactionId(), true, userReq));
      autocommit = false;
    }

    // If we need to copy, make (or get) the scratch bucket
    addStep(
        new CreateBucketForBigQueryScratchStep(resourceService, datasetService),
        getDefaultRandomBackoffRetryRule(appConfig.getMaxStairwayThreads()));

    // If we need to copy, copy to the scratch bucket
    addStep(new DataDeletionCopyFilesToBigQueryScratchBucketStep(datasetService, gcsPdao));

    // validate tables exist, check access to files, and create external temp tables
    addStep(new CreateExternalTablesStep(bigQueryPdao, datasetService));

    // insert into soft delete table
    addStep(new DataDeletionStep(bigQueryPdao, datasetService, configService, userReq, autocommit));

    if (!autocommit) {
      addStep(
          new TransactionUnlockStep(
              datasetService, bigQueryPdao, request.getTransactionId(), userReq));
    } else {
      addStep(new TransactionCommitStep(datasetService, bigQueryPdao, userReq, false, null));
    }

    // unlock
    addStep(
        new UnlockDatasetStep(datasetService, UUID.fromString(datasetId), true), lockDatasetRetry);

    // cleanup
    addStep(new DropExternalTablesStep(bigQueryPdao, datasetService));
    addStep(new DataDeletionDeleteScratchFilesGcsStep(gcsPdao));
  }
}
