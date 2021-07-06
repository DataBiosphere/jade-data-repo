package bio.terra.service.dataset.flight.create;

import bio.terra.service.configuration.ConfigurationService;
import bio.terra.service.dataset.AssetDao;
import bio.terra.service.dataset.DatasetService;
import bio.terra.stairway.Flight;
import bio.terra.stairway.FlightMap;
import org.springframework.context.ApplicationContext;

public class AddAssetSpecFlight extends Flight {

  public AddAssetSpecFlight(FlightMap inputParameters, Object applicationContext) {
    super(inputParameters, applicationContext);
    // get the required daos and services to pass into the steps
    ApplicationContext appContext = (ApplicationContext) applicationContext;
    AssetDao assetDao = appContext.getBean(AssetDao.class);
    ConfigurationService configService =
         appContext.getBean(ConfigurationService.class);
    DatasetService datasetService = appContext.getBean(DatasetService.class);

    // create job to add the assetspec to the dataset
    addStep(new CreateDatasetAssetStep(assetDao, configService, datasetService));
  }
}
