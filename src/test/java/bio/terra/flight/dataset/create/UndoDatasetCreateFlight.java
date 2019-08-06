package bio.terra.flight.dataset.create;

import bio.terra.controller.AuthenticatedUser;
import bio.terra.stairway.FlightMap;

public class UndoDatasetCreateFlight extends DatasetCreateFlight {

    public UndoDatasetCreateFlight(FlightMap inputParameters, Object applicationContext, AuthenticatedUser userInfo) {
        super(inputParameters, applicationContext, userInfo);
        addStep(new TriggerUndoStep());
    }
}
