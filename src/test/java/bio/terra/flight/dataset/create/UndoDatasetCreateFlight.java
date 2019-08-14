package bio.terra.flight.dataset.create;

import bio.terra.controller.UserInfo;
import bio.terra.stairway.FlightMap;

public class UndoDatasetCreateFlight extends DatasetCreateFlight {

    public UndoDatasetCreateFlight(FlightMap inputParameters, Object applicationContext, UserInfo userInfo) {
        super(inputParameters, applicationContext, userInfo);
        addStep(new TriggerUndoStep());
    }
}
