package bio.terra.flight;

import bio.terra.dao.StudyDAO;
import bio.terra.flight.step.CreateStudyMetadataStep;
import bio.terra.service.AsyncContext;
import bio.terra.stairway.Flight;
import bio.terra.stairway.FlightMap;

// TODO: group steps and flights together

public class StudyCreateFlight extends Flight {

    public StudyCreateFlight(FlightMap inputParameters, Object applicationContext) {
        super(inputParameters, applicationContext);

        AsyncContext asyncContext = (AsyncContext)applicationContext;
        StudyDAO studyDAO = asyncContext.getStudyDAO();
        addStep(new CreateStudyMetadataStep(studyDAO));
    }

}
