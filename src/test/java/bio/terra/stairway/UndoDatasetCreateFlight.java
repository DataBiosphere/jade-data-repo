package bio.terra.stairway;

import bio.terra.service.dataset.flight.create.DatasetCreateFlight;

public class UndoDatasetCreateFlight extends DatasetCreateFlight {

    public UndoDatasetCreateFlight(
        FlightMap inputParameters, Object applicationContext, UserRequestInfo userRequestInfo) {
        super(inputParameters, applicationContext, userRequestInfo);
        addStep(new TriggerUndoStep());
    }
}
