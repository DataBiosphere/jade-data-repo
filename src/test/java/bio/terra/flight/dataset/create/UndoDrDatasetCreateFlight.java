package bio.terra.flight.dataset.create;

import bio.terra.stairway.FlightMap;

public class UndoDrDatasetCreateFlight extends DrDatasetCreateFlight {

    public UndoDrDatasetCreateFlight(FlightMap inputParameters, Object applicationContext) {
        super(inputParameters, applicationContext);
        addStep(new TriggerUndoStep());
    }
}
