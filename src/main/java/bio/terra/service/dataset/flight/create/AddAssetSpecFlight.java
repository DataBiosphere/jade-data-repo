package bio.terra.service.dataset.flight.create;

import bio.terra.common.iam.AuthenticatedUserRequest;
import bio.terra.service.auth.iam.IamResourceType;
import bio.terra.service.common.JournalCreateUpdateEntryStep;
import bio.terra.service.configuration.ConfigurationService;
import bio.terra.service.dataset.AssetDao;
import bio.terra.service.dataset.DatasetService;
import bio.terra.service.job.JobMapKeys;
import bio.terra.service.journal.JournalService;
import bio.terra.stairway.Flight;
import bio.terra.stairway.FlightMap;
import java.util.UUID;
import org.springframework.context.ApplicationContext;

public class AddAssetSpecFlight extends Flight {

  public AddAssetSpecFlight(FlightMap inputParameters, Object applicationContext) {
    super(inputParameters, applicationContext);
    // get the required daos and services to pass into the steps
    ApplicationContext appContext = (ApplicationContext) applicationContext;
    AssetDao assetDao = appContext.getBean(AssetDao.class);
    ConfigurationService configService = appContext.getBean(ConfigurationService.class);
    DatasetService datasetService = appContext.getBean(DatasetService.class);
    JournalService journalService = appContext.getBean(JournalService.class);

    AuthenticatedUserRequest userReq =
        inputParameters.get(JobMapKeys.AUTH_USER_INFO.getKeyName(), AuthenticatedUserRequest.class);
    UUID datasetId =
        UUID.fromString(inputParameters.get(JobMapKeys.DATASET_ID.getKeyName(), String.class));
    // create job to add the assetspec to the dataset
    addStep(new CreateDatasetAssetStep(assetDao, configService, datasetService));
    addStep(
        new JournalCreateUpdateEntryStep(
            journalService, userReq, datasetId, IamResourceType.DATASET, "Added asset(s)."));
  }
}
