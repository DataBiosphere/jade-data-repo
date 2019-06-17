package bio.terra.flight.study.create;

import bio.terra.dao.StudyDao;
import bio.terra.pdao.bigquery.BigQueryPdao;
import bio.terra.service.SamClientService;
import bio.terra.stairway.Flight;
import bio.terra.stairway.FlightMap;
import org.springframework.context.ApplicationContext;

public class StudyCreateFlight extends Flight {

    public StudyCreateFlight(FlightMap inputParameters, Object applicationContext) {
        super(inputParameters, applicationContext);

        // get the required daos and services to pass into the steps
        ApplicationContext appContext = (ApplicationContext) applicationContext;
        StudyDao studyDao = (StudyDao) appContext.getBean("studyDao");
        BigQueryPdao bigQueryPdao = (BigQueryPdao) appContext.getBean("bigQueryPdao");
        SamClientService samClient = (SamClientService) appContext.getBean("samClientService");

        addStep(new CreateStudyMetadataStep(studyDao));
        addStep(new CreateStudyPrimaryDataStep(bigQueryPdao));
        addStep(new CreateStudyAuthzResource(samClient));
    }

}
