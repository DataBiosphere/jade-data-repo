package bio.terra.service.dataset.flight.create;

import bio.terra.model.AssetModel;
import bio.terra.service.dataset.AssetSpecification;
import bio.terra.service.dataset.DatasetDao;
import bio.terra.service.dataset.DatasetService;
import bio.terra.service.iam.AuthenticatedUserRequest;
import bio.terra.service.iam.SamClientService;
import bio.terra.service.job.JobMapKeys;
import bio.terra.service.tabulardata.google.BigQueryPdao;
import bio.terra.stairway.Flight;
import bio.terra.stairway.FlightMap;
import bio.terra.stairway.UserRequestInfo;
import org.springframework.context.ApplicationContext;

public class AddAssetSpecFlight extends Flight {

    public AddAssetSpecFlight(FlightMap inputParameters, Object applicationContext, UserRequestInfo userRequestInfo) {
        super(inputParameters, applicationContext, userRequestInfo);

        // get our dataset Id
        String datasetId = inputParameters.get(
            JobMapKeys.DATASET_ID.getKeyName(), String.class);

        // check that the user has dataset edit access TODO don't need to here
        AuthenticatedUserRequest userReq = inputParameters.get(
            JobMapKeys.AUTH_USER_INFO.getKeyName(), AuthenticatedUserRequest.class);

        ApplicationContext appContext = (ApplicationContext) applicationContext;
        BigQueryPdao bigQueryPdao = (BigQueryPdao) appContext.getBean("bigQueryPdao");
        SamClientService samClient = (SamClientService) appContext.getBean("samClientService");

        // validate the assetspec
        AssetModel assetModel = inputParameters.get(
            JobMapKeys.REQUEST.getKeyName(), AssetModel.class);

        AssetSpecification assetSpecification = new AssetSpecification();


        // create job to add the assetspec to the dataset
        addStep(new CreateDatasetMetadataStep(datasetDao, datasetRequest));




        // on delete asset-- throw an AssetNotFoundException if it's already been deleted
    }

}
