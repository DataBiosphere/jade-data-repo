package bio.terra.service.dataset.flight.delete;

import bio.terra.service.dataset.AssetDao;
import bio.terra.service.dataset.flight.create.DeleteDatasetAssetStep;
import bio.terra.service.job.JobMapKeys;
import bio.terra.stairway.Flight;
import bio.terra.stairway.FlightMap;
import bio.terra.stairway.UserRequestInfo;
import org.springframework.context.ApplicationContext;

import java.util.UUID;

public class RemoveAssetSpecFlight extends Flight {

    public RemoveAssetSpecFlight(FlightMap inputParameters, Object applicationContext, UserRequestInfo userRequestInfo) {
        super(inputParameters, applicationContext, userRequestInfo);

        // get the required daos and services to pass into the steps
        ApplicationContext appContext = (ApplicationContext) applicationContext;
        AssetDao assetDao = (AssetDao) appContext.getBean("assetDao");

        // get our asset Id
        UUID assetId =  UUID.fromString(
            inputParameters.get(JobMapKeys.ASSET_ID.getKeyName(), String.class)
        );

        // create job to remove the assetspec from the dataset
        addStep(new DeleteDatasetAssetStep(assetDao, assetId));
    }
}
