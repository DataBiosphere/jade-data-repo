package bio.terra.flight.study.ingest;

import bio.terra.dao.StudyDao;
import bio.terra.pdao.bigquery.BigQueryPdao;
import bio.terra.stairway.Flight;
import bio.terra.stairway.FlightMap;
import org.springframework.context.ApplicationContext;

public class StudyIngestFlight extends Flight {

    public StudyIngestFlight(FlightMap inputParameters, Object applicationContext) {
        super(inputParameters, applicationContext);

        // get the required daos to pass into the steps
        ApplicationContext appContext = (ApplicationContext) applicationContext;
        StudyDao studyDao = (StudyDao)appContext.getBean("studyDao");
        BigQueryPdao bigQueryPdao = (BigQueryPdao)appContext.getBean("bigQueryPdao");

        addStep(new IngestSetupStep(studyDao));
        addStep(new IngestLoadTableStep(studyDao, bigQueryPdao));
        addStep(new IngestRowIdsStep(studyDao, bigQueryPdao));
        addStep(new IngestInsertIntoStudyTableStep(studyDao, bigQueryPdao));
        addStep(new IngestCleanupStep(bigQueryPdao));
    }

}
