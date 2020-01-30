package bio.terra.service.dataset.flight.create;

import bio.terra.service.configuration.ConfigurationService;
import bio.terra.service.dataset.AssetDao;
import bio.terra.service.dataset.DatasetService;
import bio.terra.stairway.Flight;
import bio.terra.stairway.FlightMap;
import bio.terra.stairway.UserRequestInfo;
import org.springframework.context.ApplicationContext;

public class AddAssetSpecFlight extends Flight {

    public AddAssetSpecFlight(FlightMap inputParameters, Object applicationContext, UserRequestInfo userRequestInfo) {
        super(inputParameters, applicationContext, userRequestInfo);
        // get the required daos and services to pass into the steps
        ApplicationContext appContext = (ApplicationContext) applicationContext;
        AssetDao assetDao = (AssetDao) appContext.getBean("assetDao");
        ConfigurationService configService = (ConfigurationService) appContext.getBean("configurationService");
        DatasetService datasetService = (DatasetService) appContext.getBean("datasetService");

        // create job to add the assetspec to the dataset
        addStep(new CreateDatasetAssetStep(assetDao, configService, datasetService));
    }

}
