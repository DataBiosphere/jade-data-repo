package bio.terra.flight.dataset.delete;

import bio.terra.dao.DataSnapshotDao;
import bio.terra.dao.DrDatasetDao;
import bio.terra.filesystem.FireStoreDependencyDao;
import bio.terra.filesystem.FireStoreFileDao;
import bio.terra.pdao.bigquery.BigQueryPdao;
import bio.terra.pdao.gcs.GcsPdao;
import bio.terra.service.SamClientService;
import bio.terra.stairway.Flight;
import bio.terra.stairway.FlightMap;
import org.springframework.context.ApplicationContext;

public class DrDatasetDeleteFlight extends Flight {

    public DrDatasetDeleteFlight(FlightMap inputParameters, Object applicationContext) {
        super(inputParameters, applicationContext);

        // get the required daos to pass into the steps
        ApplicationContext appContext = (ApplicationContext) applicationContext;
        DrDatasetDao datasetDao = (DrDatasetDao)appContext.getBean("drDatasetDao");
        DataSnapshotDao dataSnapshotDao = (DataSnapshotDao)appContext.getBean("dataSnapshotDao");
        BigQueryPdao bigQueryPdao = (BigQueryPdao)appContext.getBean("bigQueryPdao");
        GcsPdao gcsPdao = (GcsPdao)appContext.getBean("gcsPdao");
        FireStoreDependencyDao dependencyDao = (FireStoreDependencyDao)appContext.getBean("fireStoreDependencyDao");
        FireStoreFileDao fileDao = (FireStoreFileDao)appContext.getBean("fireStoreFileDao");
        SamClientService samClient = (SamClientService)appContext.getBean("samClientService");

        addStep(new DeleteDrDatasetValidateStep(dataSnapshotDao, dependencyDao));
        addStep(new DeleteDrDatasetPrimaryDataStep(bigQueryPdao, gcsPdao, fileDao, datasetDao));
        addStep(new DeleteDrDatasetMetadataStep(datasetDao));
        addStep(new DeleteDrDatasetAuthzResource(samClient));
    }
}
