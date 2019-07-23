package bio.terra.flight.study.delete;

import bio.terra.dao.DatasetDao;
import bio.terra.dao.StudyDao;
import bio.terra.filesystem.FireStoreDependencyDao;
import bio.terra.filesystem.FireStoreFileDao;
import bio.terra.pdao.bigquery.BigQueryPdao;
import bio.terra.pdao.gcs.GcsPdao;
import bio.terra.service.SamClientService;
import bio.terra.service.StudyService;
import bio.terra.stairway.Flight;
import bio.terra.stairway.FlightMap;
import org.springframework.context.ApplicationContext;

public class StudyDeleteFlight extends Flight {

    public StudyDeleteFlight(FlightMap inputParameters, Object applicationContext) {
        super(inputParameters, applicationContext);

        // get the required daos to pass into the steps
        ApplicationContext appContext = (ApplicationContext) applicationContext;
        StudyDao studyDao = (StudyDao)appContext.getBean("studyDao");
        DatasetDao datasetDao = (DatasetDao)appContext.getBean("datasetDao");
        BigQueryPdao bigQueryPdao = (BigQueryPdao)appContext.getBean("bigQueryPdao");
        GcsPdao gcsPdao = (GcsPdao)appContext.getBean("gcsPdao");
        FireStoreDependencyDao dependencyDao = (FireStoreDependencyDao)appContext.getBean("fireStoreDependencyDao");
        FireStoreFileDao fileDao = (FireStoreFileDao)appContext.getBean("fireStoreFileDao");
        SamClientService samClient = (SamClientService)appContext.getBean("samClientService");
        StudyService studyService = (StudyService) appContext.getBean("studyService");

        addStep(new DeleteStudyValidateStep(datasetDao, dependencyDao, studyService));
        addStep(new DeleteStudyPrimaryDataStep(bigQueryPdao, gcsPdao, fileDao, studyDao));
        addStep(new DeleteStudyMetadataStep(studyDao));
        addStep(new DeleteStudyAuthzResource(samClient));
    }
}
