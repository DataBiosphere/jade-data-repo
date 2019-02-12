package bio.terra.flight.study.create;

import bio.terra.stairway.FlightMap;

public class UndoStudyCreateFlight extends StudyCreateFlight {

    public UndoStudyCreateFlight(FlightMap inputParameters, Object applicationContext) {
        super(inputParameters, applicationContext);
        addStep(new TriggerUndoStep());
    }
}
