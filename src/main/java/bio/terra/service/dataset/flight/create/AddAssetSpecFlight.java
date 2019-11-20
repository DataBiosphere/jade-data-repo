package bio.terra.service.dataset.flight.create;

import bio.terra.app.controller.exception.ValidationException;
import bio.terra.service.dataset.AssetDao;
import bio.terra.service.dataset.AssetSpecification;
import bio.terra.service.dataset.Dataset;
import bio.terra.service.dataset.DatasetService;
import bio.terra.service.job.JobMapKeys;
import bio.terra.stairway.Flight;
import bio.terra.stairway.FlightMap;
import bio.terra.stairway.UserRequestInfo;
import org.springframework.context.ApplicationContext;

import java.util.List;
import java.util.UUID;

public class AddAssetSpecFlight extends Flight {

    public AddAssetSpecFlight(FlightMap inputParameters, Object applicationContext, UserRequestInfo userRequestInfo) {
        super(inputParameters, applicationContext, userRequestInfo);

        // get the required daos and services to pass into the steps
        ApplicationContext appContext = (ApplicationContext) applicationContext;
        AssetDao assetDao = (AssetDao) appContext.getBean("assetDao");
        DatasetService datasetService = (DatasetService) appContext.getBean("datasetService");

        // get our dataset Id and dataset
        UUID datasetId = UUID.fromString(
            inputParameters.get(JobMapKeys.DATASET_ID.getKeyName(), String.class)
        );
        Dataset dataset = datasetService.retrieve(datasetId);


        // validate the assetspec
        AssetSpecification assetSpecification = inputParameters.get(
            JobMapKeys.REQUEST.getKeyName(), AssetSpecification.class);
        String assetName = assetSpecification.getName();

        // get the dataset assets that already exist --asset name needs to be unique
        List<AssetSpecification> datasetAssetSpecificationList = dataset.getAssetSpecifications();
        // could have used AssetDao.retrieve(dataset)
        if(datasetAssetSpecificationList.stream()
            .anyMatch(asset -> asset.getName().equalsIgnoreCase(assetName))) {
            throw new ValidationException("Can not add an asset to a dataset with a duplicate name");
        }

        //Asset columns and tables need to match things in the dataset schema
        // get the dataset schema
        // can I use DatasetRequestValidator.validateAsset ?



        // create job to add the assetspec to the dataset
        addStep(new CreateDatasetAssetStep(datasetId, assetDao, assetSpecification));
    }

}
