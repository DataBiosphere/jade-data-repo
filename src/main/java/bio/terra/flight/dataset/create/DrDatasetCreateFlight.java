package bio.terra.flight.dataset.create;

import bio.terra.dao.DrDatasetDao;
import bio.terra.pdao.bigquery.BigQueryPdao;
import bio.terra.service.SamClientService;
import bio.terra.stairway.Flight;
import bio.terra.stairway.FlightMap;
import org.springframework.context.ApplicationContext;

public class DrDatasetCreateFlight extends Flight {

    public DrDatasetCreateFlight(FlightMap inputParameters, Object applicationContext) {
        super(inputParameters, applicationContext);

        // get the required daos and services to pass into the steps
        ApplicationContext appContext = (ApplicationContext) applicationContext;
        DrDatasetDao datasetDao = (DrDatasetDao)appContext.getBean("drDatasetDao");
        BigQueryPdao bigQueryPdao = (BigQueryPdao)appContext.getBean("bigQueryPdao");
        SamClientService samClient = (SamClientService)appContext.getBean("samClientService");

        addStep(new CreateDrDatasetMetadataStep(datasetDao));
        addStep(new CreateDrDatasetPrimaryDataStep(bigQueryPdao));
        addStep(new CreateDrDatasetAuthzResource(samClient));
    }

}
