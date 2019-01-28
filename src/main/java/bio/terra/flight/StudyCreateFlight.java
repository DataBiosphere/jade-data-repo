package bio.terra.flight;

import bio.terra.dao.StudyDao;
import bio.terra.flight.step.CreateStudyMetadataStep;
import bio.terra.stairway.Flight;
import bio.terra.stairway.FlightMap;
import org.springframework.context.ApplicationContext;

// TODO: group steps and flights together

public class StudyCreateFlight extends Flight {

    public StudyCreateFlight(FlightMap inputParameters, Object applicationContext) {
        super(inputParameters, applicationContext);
        ApplicationContext appContext = (ApplicationContext) applicationContext;
        StudyDao studyDao = (StudyDao)appContext.getBean("studyDao");
        addStep(new CreateStudyMetadataStep(studyDao));
    }

}
