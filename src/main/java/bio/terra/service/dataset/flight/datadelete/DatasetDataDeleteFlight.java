package bio.terra.service.dataset.flight.datadelete;

import static bio.terra.common.FlightUtils.getDefaultRandomBackoffRetryRule;

import bio.terra.app.configuration.ApplicationConfiguration;
import bio.terra.common.ValidateBucketAccessStep;
import bio.terra.common.iam.AuthenticatedUserRequest;
import bio.terra.model.DataDeletionRequest;
import bio.terra.service.auth.iam.IamAction;
import bio.terra.service.auth.iam.IamProviderInterface;
import bio.terra.service.auth.iam.IamResourceType;
import bio.terra.service.auth.iam.flight.VerifyAuthorizationStep;
import bio.terra.service.configuration.ConfigurationService;
import bio.terra.service.dataset.DatasetService;
import bio.terra.service.dataset.flight.LockDatasetStep;
import bio.terra.service.dataset.flight.UnlockDatasetStep;
import bio.terra.service.dataset.flight.transactions.TransactionCommitStep;
import bio.terra.service.dataset.flight.transactions.TransactionLockStep;
import bio.terra.service.dataset.flight.transactions.TransactionOpenStep;
import bio.terra.service.dataset.flight.transactions.TransactionUnlockStep;
import bio.terra.service.filedata.flight.ingest.CreateBucketForBigQueryScratchStep;
import bio.terra.service.filedata.google.gcs.GcsPdao;
import bio.terra.service.job.JobMapKeys;
import bio.terra.service.resourcemanagement.ResourceService;
import bio.terra.service.tabulardata.google.bigquery.BigQueryDatasetPdao;
import bio.terra.service.tabulardata.google.bigquery.BigQueryTransactionPdao;
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
    BigQueryTransactionPdao bigQueryTransactionPdao =
        appContext.getBean(BigQueryTransactionPdao.class);
    BigQueryDatasetPdao bigQueryDatasetPdao = appContext.getBean(BigQueryDatasetPdao.class);
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
              datasetService, bigQueryTransactionPdao, userReq, transactionDesc, false, false));
      autocommit = true;
    } else {
      addStep(
          new TransactionLockStep(
              datasetService, bigQueryTransactionPdao, request.getTransactionId(), true, userReq));
      autocommit = false;
    }

    // If we need to copy, make (or get) the scratch bucket
    addStep(
        new CreateBucketForBigQueryScratchStep(resourceService, datasetService),
        getDefaultRandomBackoffRetryRule(appConfig.getMaxStairwayThreads()));

    // If we need to copy, copy to the scratch bucket
    addStep(new DataDeletionCopyFilesToBigQueryScratchBucketStep(datasetService, gcsPdao));

    // validate tables exist, check access to files, and create external temp tables
    addStep(new CreateExternalTablesStep(bigQueryDatasetPdao, datasetService));

    // insert into soft delete table
    addStep(
        new DataDeletionStep(
            bigQueryTransactionPdao,
            bigQueryDatasetPdao,
            datasetService,
            configService,
            userReq,
            autocommit));

    if (!autocommit) {
      addStep(
          new TransactionUnlockStep(
              datasetService, bigQueryTransactionPdao, request.getTransactionId(), userReq));
    } else {
      addStep(
          new TransactionCommitStep(datasetService, bigQueryTransactionPdao, userReq, false, null));
    }

    // unlock
    addStep(
        new UnlockDatasetStep(datasetService, UUID.fromString(datasetId), true), lockDatasetRetry);

    // cleanup
    addStep(new DropExternalTablesStep(datasetService));
    addStep(new DataDeletionDeleteScratchFilesGcsStep(gcsPdao));
  }
}
