package bio.terra.flight.dataset.delete;

import bio.terra.dao.SnapshotDao;
import bio.terra.dao.DatasetDao;
import bio.terra.filesystem.FireStoreDependencyDao;
import bio.terra.filesystem.FireStoreDirectoryDao;
import bio.terra.pdao.bigquery.BigQueryPdao;
import bio.terra.pdao.gcs.GcsPdao;
import bio.terra.service.SamClientService;
import bio.terra.service.DatasetService;
import bio.terra.stairway.Flight;
import bio.terra.stairway.FlightMap;
import org.springframework.context.ApplicationContext;

public class DatasetDeleteFlight extends Flight {

    public DatasetDeleteFlight(FlightMap inputParameters, Object applicationContext) {
        super(inputParameters, applicationContext);

        // get the required daos to pass into the steps
        ApplicationContext appContext = (ApplicationContext) applicationContext;
        DatasetDao datasetDao = (DatasetDao)appContext.getBean("datasetDao");
        SnapshotDao snapshotDao = (SnapshotDao)appContext.getBean("snapshotDao");
        BigQueryPdao bigQueryPdao = (BigQueryPdao)appContext.getBean("bigQueryPdao");
        GcsPdao gcsPdao = (GcsPdao)appContext.getBean("gcsPdao");
        FireStoreDependencyDao dependencyDao = (FireStoreDependencyDao)appContext.getBean("fireStoreDependencyDao");
        FireStoreDirectoryDao fileDao = (FireStoreDirectoryDao)appContext.getBean("fireStoreFileDao");
        SamClientService samClient = (SamClientService)appContext.getBean("samClientService");
        DatasetService datasetService = (DatasetService) appContext.getBean("datasetService");

        addStep(new DeleteDatasetValidateStep(snapshotDao, dependencyDao, datasetService));
        addStep(new DeleteDatasetPrimaryDataStep(bigQueryPdao, gcsPdao, fileDao, datasetService));
        addStep(new DeleteDatasetMetadataStep(datasetDao));
        addStep(new DeleteDatasetAuthzResource(samClient));
    }
}
