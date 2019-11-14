package bio.terra.service.dataset.flight.create;

import bio.terra.model.AssetModel;
import bio.terra.service.job.JobMapKeys;
import bio.terra.stairway.Flight;
import bio.terra.stairway.FlightMap;
import bio.terra.stairway.UserRequestInfo;

import java.util.UUID;

public class AddAssetSpecFlight extends Flight {

    public AddAssetSpecFlight(FlightMap inputParameters, Object applicationContext, UserRequestInfo userRequestInfo) {
        super(inputParameters, applicationContext, userRequestInfo);

        // get our dataset Id
        UUID datasetId = UUID.fromString(
            inputParameters.get(JobMapKeys.DATASET_ID.getKeyName(), String.class)
        );

        // validate the assetspec
        AssetModel assetModel = inputParameters.get(
            JobMapKeys.REQUEST.getKeyName(), AssetModel.class);


        // create job to add the assetspec to the dataset
        addStep(new CreateDatasetAssetStep(datasetId, assetModel));

        // on delete asset-- throw an AssetNotFoundException if it's already been deleted
    }

}
