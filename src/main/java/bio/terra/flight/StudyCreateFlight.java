package bio.terra.flight;

import bio.terra.flight.step.CreateStudyMetadataStep;
import bio.terra.stairway.Flight;
import bio.terra.stairway.FlightMap;

// TODO: group steps and flights together

public class StudyCreateFlight extends Flight {

    public StudyCreateFlight(FlightMap inputParameters) {
        super(inputParameters);
        addStep(new CreateStudyMetadataStep());
    }

}
