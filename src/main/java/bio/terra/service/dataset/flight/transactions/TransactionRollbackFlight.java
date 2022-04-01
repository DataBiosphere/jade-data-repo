package bio.terra.service.dataset.flight.transactions;

import bio.terra.common.CloudPlatformWrapper;
import bio.terra.common.exception.CommonExceptions;
import bio.terra.common.iam.AuthenticatedUserRequest;
import bio.terra.service.dataset.Dataset;
import bio.terra.service.dataset.DatasetService;
import bio.terra.service.job.JobMapKeys;
import bio.terra.service.tabulardata.google.bigquery.BigQueryTransactionPdao;
import bio.terra.stairway.Flight;
import bio.terra.stairway.FlightMap;
import java.util.UUID;
import org.springframework.context.ApplicationContext;

public class TransactionRollbackFlight extends Flight {

  public TransactionRollbackFlight(FlightMap inputParameters, Object applicationContext) {
    super(inputParameters, applicationContext);

    // get the required daos to pass into the steps
    ApplicationContext appContext = (ApplicationContext) applicationContext;
    DatasetService datasetService = appContext.getBean(DatasetService.class);
    BigQueryTransactionPdao bigQueryTransactionPdao =
        appContext.getBean(BigQueryTransactionPdao.class);

    UUID datasetId =
        UUID.fromString(inputParameters.get(JobMapKeys.DATASET_ID.getKeyName(), String.class));
    UUID transactionId =
        UUID.fromString(inputParameters.get(JobMapKeys.TRANSACTION_ID.getKeyName(), String.class));
    Dataset dataset = datasetService.retrieve(datasetId);
    CloudPlatformWrapper cloudPlatform =
        CloudPlatformWrapper.of(dataset.getDatasetSummary().getStorageCloudPlatform());
    AuthenticatedUserRequest userReq =
        inputParameters.get(JobMapKeys.AUTH_USER_INFO.getKeyName(), AuthenticatedUserRequest.class);

    if (cloudPlatform.isGcp()) {
      addStep(
          new TransactionLockStep(
              datasetService, bigQueryTransactionPdao, transactionId, false, userReq));
      addStep(
          new TransactionRollbackMetadataStep(
              datasetService, bigQueryTransactionPdao, transactionId));
      addStep(
          new TransactionRollbackDataStep(datasetService, bigQueryTransactionPdao, transactionId));
      addStep(
          new TransactionRollbackSoftDeleteStep(
              datasetService, bigQueryTransactionPdao, transactionId));
      addStep(
          new TransactionRollbackStep(
              datasetService, bigQueryTransactionPdao, transactionId, userReq));
      addStep(
          new TransactionUnlockStep(
              datasetService, bigQueryTransactionPdao, transactionId, userReq));
    } else if (cloudPlatform.isAzure()) {
      throw CommonExceptions.TRANSACTIONS_NOT_IMPLEMENTED_IN_AZURE;
    }
  }
}
