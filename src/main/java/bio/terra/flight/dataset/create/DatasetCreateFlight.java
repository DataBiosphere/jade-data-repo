package bio.terra.flight.dataset.create;

import bio.terra.controller.AuthenticatedUserRequest;
import bio.terra.dao.DatasetDao;
import bio.terra.model.DatasetRequestModel;
import bio.terra.pdao.bigquery.BigQueryPdao;
import bio.terra.service.DatasetService;
import bio.terra.service.JobMapKeys;
import bio.terra.service.SamClientService;
import bio.terra.stairway.Flight;
import bio.terra.stairway.FlightMap;
import bio.terra.stairway.UserRequestInfo;
import org.springframework.context.ApplicationContext;

public class DatasetCreateFlight extends Flight {

    public DatasetCreateFlight(FlightMap inputParameters, Object applicationContext, UserRequestInfo userRequestInfo) {
        super(inputParameters, applicationContext, userRequestInfo);

        // get the required daos and services to pass into the steps
        ApplicationContext appContext = (ApplicationContext) applicationContext;
        DatasetDao datasetDao = (DatasetDao) appContext.getBean("datasetDao");
        DatasetService datasetService = (DatasetService) appContext.getBean("datasetService");
        BigQueryPdao bigQueryPdao = (BigQueryPdao) appContext.getBean("bigQueryPdao");
        SamClientService samClient = (SamClientService) appContext.getBean("samClientService");

        // get data from inputs that steps need
        AuthenticatedUserRequest userReq = inputParameters.get(
            JobMapKeys.AUTH_USER_INFO.getKeyName(), AuthenticatedUserRequest.class);
        DatasetRequestModel datasetRequest =
            inputParameters.get(JobMapKeys.REQUEST.getKeyName(), DatasetRequestModel.class);

        addStep(new CreateDatasetMetadataStep(datasetDao, datasetRequest));
        // TODO: create dataset data project step
        addStep(new CreateDatasetPrimaryDataStep(bigQueryPdao, datasetService));
        addStep(new CreateDatasetAuthzResource(samClient, bigQueryPdao, datasetService, userReq));
    }

}
