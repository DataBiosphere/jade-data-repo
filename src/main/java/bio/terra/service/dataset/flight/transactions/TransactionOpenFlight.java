package bio.terra.service.dataset.flight.transactions;

import bio.terra.common.CloudPlatformWrapper;
import bio.terra.common.exception.CommonExceptions;
import bio.terra.common.iam.AuthenticatedUserRequest;
import bio.terra.model.TransactionCreateModel;
import bio.terra.service.dataset.Dataset;
import bio.terra.service.dataset.DatasetService;
import bio.terra.service.job.JobMapKeys;
import bio.terra.service.tabulardata.google.bigquery.BigQueryTransactionPdao;
import bio.terra.stairway.Flight;
import bio.terra.stairway.FlightMap;
import java.util.UUID;
import org.springframework.context.ApplicationContext;

public class TransactionOpenFlight extends Flight {

  public TransactionOpenFlight(FlightMap inputParameters, Object applicationContext) {
    super(inputParameters, applicationContext);

    // get the required daos to pass into the steps
    ApplicationContext appContext = (ApplicationContext) applicationContext;
    DatasetService datasetService = appContext.getBean(DatasetService.class);
    BigQueryTransactionPdao bigQueryTransactionPdao =
        appContext.getBean(BigQueryTransactionPdao.class);

    UUID datasetId =
        UUID.fromString(inputParameters.get(JobMapKeys.DATASET_ID.getKeyName(), String.class));
    TransactionCreateModel transactionRequestModel =
        inputParameters.get(JobMapKeys.REQUEST.getKeyName(), TransactionCreateModel.class);
    Dataset dataset = datasetService.retrieve(datasetId);
    CloudPlatformWrapper cloudPlatform =
        CloudPlatformWrapper.of(dataset.getDatasetSummary().getStorageCloudPlatform());
    AuthenticatedUserRequest userReq =
        inputParameters.get(JobMapKeys.AUTH_USER_INFO.getKeyName(), AuthenticatedUserRequest.class);

    if (cloudPlatform.isGcp()) {
      addStep(
          new TransactionOpenStep(
              datasetService,
              bigQueryTransactionPdao,
              userReq,
              transactionRequestModel.getDescription(),
              true,
              true));
    } else if (cloudPlatform.isAzure()) {
      throw CommonExceptions.TRANSACTIONS_NOT_IMPLEMENTED_IN_AZURE;
    }
    addStep(new TransactionUnlockStep(datasetService, bigQueryTransactionPdao, null, userReq));
  }
}
