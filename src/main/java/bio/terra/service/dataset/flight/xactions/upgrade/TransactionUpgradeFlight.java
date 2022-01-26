package bio.terra.service.dataset.flight.xactions.upgrade;

import bio.terra.common.iam.AuthenticatedUserRequest;
import bio.terra.service.dataset.DatasetService;
import bio.terra.service.iam.IamService;
import bio.terra.service.job.JobMapKeys;
import bio.terra.service.tabulardata.google.BigQueryPdao;
import bio.terra.stairway.Flight;
import bio.terra.stairway.FlightMap;
import org.springframework.context.ApplicationContext;

public class TransactionUpgradeFlight extends Flight {

  public TransactionUpgradeFlight(FlightMap inputParameters, Object applicationContext) {
    super(inputParameters, applicationContext);

    // get the required daos to pass into the steps
    ApplicationContext appContext = (ApplicationContext) applicationContext;
    IamService iamService = appContext.getBean(IamService.class);
    DatasetService datasetService = appContext.getBean(DatasetService.class);
    BigQueryPdao bigQueryPdao = appContext.getBean(BigQueryPdao.class);

    AuthenticatedUserRequest userReq =
        inputParameters.get(JobMapKeys.AUTH_USER_INFO.getKeyName(), AuthenticatedUserRequest.class);

    addStep(new TransactionUpgradeStep(iamService, datasetService, bigQueryPdao, userReq));
  }
}
