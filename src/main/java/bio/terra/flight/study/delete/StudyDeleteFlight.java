package bio.terra.flight.study.delete;

import bio.terra.dao.StudyDao;
import bio.terra.pdao.bigquery.BigQueryPdao;
import bio.terra.stairway.Flight;
import bio.terra.stairway.FlightMap;
import org.springframework.context.ApplicationContext;

public class StudyDeleteFlight extends Flight {

    public StudyDeleteFlight(FlightMap inputParameters, Object applicationContext) {
        super(inputParameters, applicationContext);

        // get the required daos to pass into the steps
        ApplicationContext appContext = (ApplicationContext) applicationContext;
        StudyDao studyDao = (StudyDao)appContext.getBean("studyDao");
        BigQueryPdao bigQueryPdao = (BigQueryPdao)appContext.getBean("bigQueryPdao");

        addStep(new DeleteStudyPrimaryDataStep(bigQueryPdao, studyDao));
        addStep(new DeleteStudyMetadataStep(studyDao));
    }
}
