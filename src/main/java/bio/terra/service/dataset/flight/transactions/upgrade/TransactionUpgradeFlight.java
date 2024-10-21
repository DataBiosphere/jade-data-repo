package bio.terra.service.dataset.flight.transactions.upgrade;

import bio.terra.common.iam.AuthenticatedUserRequest;
import bio.terra.service.auth.iam.IamResourceType;
import bio.terra.service.auth.iam.IamService;
import bio.terra.service.common.JournalRecordUpdateEntryStep;
import bio.terra.service.dataset.DatasetService;
import bio.terra.service.job.JobMapKeys;
import bio.terra.service.journal.JournalService;
import bio.terra.service.tabulardata.google.bigquery.BigQueryDatasetPdao;
import bio.terra.stairway.Flight;
import bio.terra.stairway.FlightMap;
import java.util.UUID;
import org.springframework.context.ApplicationContext;

public class TransactionUpgradeFlight extends Flight {

  public TransactionUpgradeFlight(FlightMap inputParameters, Object applicationContext) {
    super(inputParameters, applicationContext);

    // get the required daos to pass into the steps
    ApplicationContext appContext = (ApplicationContext) applicationContext;
    IamService iamService = appContext.getBean(IamService.class);
    DatasetService datasetService = appContext.getBean(DatasetService.class);
    BigQueryDatasetPdao bigQueryDatasetPdao = appContext.getBean(BigQueryDatasetPdao.class);
    JournalService journalService = appContext.getBean(JournalService.class);

    AuthenticatedUserRequest userReq =
        inputParameters.get(JobMapKeys.AUTH_USER_INFO.getKeyName(), AuthenticatedUserRequest.class);
    UUID datasetId =
        UUID.fromString(inputParameters.get(JobMapKeys.DATASET_ID.getKeyName(), String.class));
    addStep(new TransactionUpgradeStep(iamService, datasetService, bigQueryDatasetPdao, userReq));
    addStep(
        new JournalRecordUpdateEntryStep(
            journalService,
            userReq,
            datasetId,
            IamResourceType.DATASET,
            "Transaction upgrade on dataset."));
  }
}
