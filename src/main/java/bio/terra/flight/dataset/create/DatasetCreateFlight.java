package bio.terra.flight.dataset.create;

import bio.terra.controller.AuthenticatedUser;
import bio.terra.dao.DatasetDao;
import bio.terra.pdao.bigquery.BigQueryPdao;
import bio.terra.service.SamClientService;
import bio.terra.service.DatasetService;
import bio.terra.stairway.Flight;
import bio.terra.stairway.FlightMap;
import org.springframework.context.ApplicationContext;

public class DatasetCreateFlight extends Flight {

    public DatasetCreateFlight(FlightMap inputParameters, Object applicationContext, AuthenticatedUser userInfo) {
        super(inputParameters, applicationContext, userInfo);

        // get the required daos and services to pass into the steps
        ApplicationContext appContext = (ApplicationContext) applicationContext;
        DatasetDao datasetDao = (DatasetDao) appContext.getBean("datasetDao");
        DatasetService datasetService = (DatasetService) appContext.getBean("datasetService");
        BigQueryPdao bigQueryPdao = (BigQueryPdao) appContext.getBean("bigQueryPdao");
        SamClientService samClient = (SamClientService) appContext.getBean("samClientService");

        addStep(new CreateDatasetMetadataStep(datasetDao));
        // TODO: create dataset data project step
        addStep(new CreateDatasetPrimaryDataStep(bigQueryPdao, datasetService));
        addStep(new CreateDatasetAuthzResource(samClient, bigQueryPdao, datasetService));
    }

}
