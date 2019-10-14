package bio.terra.service.dataset.flight.create;

import bio.terra.service.iam.AuthenticatedUserRequest;
import bio.terra.service.dataset.DatasetDao;
import bio.terra.model.DatasetRequestModel;
import bio.terra.service.tabulardata.google.BigQueryPdao;
import bio.terra.service.dataset.DatasetService;
import bio.terra.service.job.JobMapKeys;
import bio.terra.service.iam.SamClientService;
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
