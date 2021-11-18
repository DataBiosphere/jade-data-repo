package bio.terra.service.upgrade.flight;

import bio.terra.model.UpgradeModel;
import bio.terra.service.dataset.DatasetService;
import bio.terra.service.iam.AuthenticatedUserRequest;
import bio.terra.service.iam.IamService;
import bio.terra.service.job.JobMapKeys;
import bio.terra.service.tabulardata.google.BigQueryPdao;
import bio.terra.stairway.Flight;
import bio.terra.stairway.FlightMap;
import org.springframework.context.ApplicationContext;

public class RowMetadataUpdateFlight extends Flight {
  public RowMetadataUpdateFlight(FlightMap inputParameters, Object applicationContext) {
    super(inputParameters, applicationContext);

    ApplicationContext appContext = (ApplicationContext) applicationContext;
    DatasetService datasetService = (DatasetService) appContext.getBean("datasetService");
    IamService iamService = (IamService) appContext.getBean("iamService");
    BigQueryPdao bigQueryPdao = (BigQueryPdao) appContext.getBean("bigQueryPdao");

    UpgradeModel request = inputParameters.get(JobMapKeys.REQUEST.getKeyName(), UpgradeModel.class);
    AuthenticatedUserRequest userReq =
        inputParameters.get(JobMapKeys.AUTH_USER_INFO.getKeyName(), AuthenticatedUserRequest.class);

    addStep(
        new BackfillRowMetadataTablesStep(
            request, datasetService, iamService, bigQueryPdao, userReq));
  }
}
