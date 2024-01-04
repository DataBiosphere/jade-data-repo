package bio.terra.service.dataset.flight.delete;

import static bio.terra.common.FlightUtils.getDefaultRandomBackoffRetryRule;

import bio.terra.app.configuration.ApplicationConfiguration;
import bio.terra.service.dataset.AssetDao;
import bio.terra.service.dataset.DatasetService;
import bio.terra.service.dataset.flight.LockDatasetStep;
import bio.terra.service.dataset.flight.UnlockDatasetStep;
import bio.terra.service.dataset.flight.create.DeleteDatasetAssetStep;
import bio.terra.service.job.JobMapKeys;
import bio.terra.stairway.Flight;
import bio.terra.stairway.FlightMap;
import bio.terra.stairway.RetryRule;
import java.util.UUID;
import org.springframework.context.ApplicationContext;

public class RemoveAssetSpecFlight extends Flight {

  public RemoveAssetSpecFlight(FlightMap inputParameters, Object applicationContext) {
    super(inputParameters, applicationContext);

    // get the required daos and services to pass into the steps
    ApplicationContext appContext = (ApplicationContext) applicationContext;
    ApplicationConfiguration appConfig = appContext.getBean(ApplicationConfiguration.class);
    AssetDao assetDao = appContext.getBean(AssetDao.class);
    DatasetService datasetService = appContext.getBean(DatasetService.class);

    UUID datasetId =
        UUID.fromString(inputParameters.get(JobMapKeys.DATASET_ID.getKeyName(), String.class));

    RetryRule lockDatasetRetry =
        getDefaultRandomBackoffRetryRule(appConfig.getMaxStairwayThreads());

    addStep(new LockDatasetStep(datasetService, datasetId, false, true));

    // create job to remove the assetspec from the dataset
    addStep(new DeleteDatasetAssetStep(assetDao, datasetId));

    addStep(new UnlockDatasetStep(datasetService, datasetId, false), lockDatasetRetry);
  }
}
