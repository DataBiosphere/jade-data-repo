package bio.terra.flight.dataset.create;

import bio.terra.service.dataset.flight.create.DatasetCreateFlight;
import bio.terra.stairway.UserRequestInfo;
import bio.terra.stairway.FlightMap;

public class UndoDatasetCreateFlight extends DatasetCreateFlight {

    public UndoDatasetCreateFlight(
        FlightMap inputParameters, Object applicationContext, UserRequestInfo userRequestInfo) {
        super(inputParameters, applicationContext, userRequestInfo);
        addStep(new TriggerUndoStep());
    }
}
