package bio.terra.flight.study.ingest;

import bio.terra.filesystem.FireStoreFileDao;
import bio.terra.pdao.bigquery.BigQueryPdao;
import bio.terra.service.StudyService;
import bio.terra.stairway.Flight;
import bio.terra.stairway.FlightMap;
import org.springframework.context.ApplicationContext;

public class StudyIngestFlight extends Flight {

    public StudyIngestFlight(FlightMap inputParameters, Object applicationContext) {
        super(inputParameters, applicationContext);

        // get the required daos to pass into the steps
        ApplicationContext appContext = (ApplicationContext) applicationContext;
        StudyService studyService = (StudyService) appContext.getBean("studyService");
        BigQueryPdao bigQueryPdao = (BigQueryPdao)appContext.getBean("bigQueryPdao");
        FireStoreFileDao fileDao  = (FireStoreFileDao)appContext.getBean("fireStoreFileDao");

        addStep(new IngestSetupStep(studyService));
        addStep(new IngestLoadTableStep(studyService, bigQueryPdao));
        addStep(new IngestRowIdsStep(studyService, bigQueryPdao));
        addStep(new IngestValidateRefsStep(studyService, bigQueryPdao, fileDao));
        addStep(new IngestInsertIntoStudyTableStep(studyService, bigQueryPdao));
        addStep(new IngestCleanupStep(studyService, bigQueryPdao));
    }

}
