package bio.terra.flight.dataset.create;

import bio.terra.dao.DatasetDao;
import bio.terra.filesystem.FileDao;
import bio.terra.pdao.bigquery.BigQueryPdao;
import bio.terra.service.DatasetService;
import bio.terra.service.SamClientService;
import bio.terra.stairway.Flight;
import bio.terra.stairway.FlightMap;
import org.springframework.context.ApplicationContext;

public class DatasetCreateFlight extends Flight {

    public DatasetCreateFlight(FlightMap inputParameters, Object applicationContext) {
        super(inputParameters, applicationContext);

        // get the required daos to pass into the steps
        ApplicationContext appContext = (ApplicationContext) applicationContext;
        DatasetDao datasetDao = (DatasetDao)appContext.getBean("datasetDao");
        DatasetService datasetService = (DatasetService)appContext.getBean("datasetService");
        BigQueryPdao bigQueryPdao = (BigQueryPdao)appContext.getBean("bigQueryPdao");
        FileDao fileDao = (FileDao)appContext.getBean("fileDao");
        SamClientService samClient = (SamClientService)appContext.getBean("samClientService");


        addStep(new CreateDatasetMetadataStep(datasetDao, datasetService));
        addStep(new CreateDatasetPrimaryDataStep(bigQueryPdao, datasetService, datasetDao, fileDao));
        addStep(new CreateDatasetAuthzResource(samClient));
        addStep(new SetBigQueryAuthorization(bigQueryPdao));
    }
}
