package bio.terra.service.dataset.flight.xactions;

import bio.terra.common.CloudPlatformWrapper;
import bio.terra.common.exception.CommonExceptions;
import bio.terra.common.iam.AuthenticatedUserRequest;
import bio.terra.service.dataset.Dataset;
import bio.terra.service.dataset.DatasetService;
import bio.terra.service.job.JobMapKeys;
import bio.terra.service.tabulardata.google.BigQueryPdao;
import bio.terra.stairway.Flight;
import bio.terra.stairway.FlightMap;
import java.util.UUID;
import org.springframework.context.ApplicationContext;

public class TransactionCommitFlight extends Flight {

  public TransactionCommitFlight(FlightMap inputParameters, Object applicationContext) {
    super(inputParameters, applicationContext);

    // get the required daos to pass into the steps
    ApplicationContext appContext = (ApplicationContext) applicationContext;
    DatasetService datasetService = appContext.getBean(DatasetService.class);
    BigQueryPdao bigQueryPdao = appContext.getBean(BigQueryPdao.class);

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
      addStep(new TransactionLockStep(datasetService, bigQueryPdao, transactionId, false, userReq));
      addStep(new TransactionVerifyStep(datasetService, bigQueryPdao, transactionId));
      addStep(new TransactionCommitStep(datasetService, bigQueryPdao, userReq, true));
      addStep(new TransactionUnlockStep(datasetService, bigQueryPdao, transactionId, userReq));
    } else if (cloudPlatform.isAzure()) {
      throw CommonExceptions.TRANSACTIONS_NOT_IMPLEMENTED_IN_AZURE;
    }
  }
}
