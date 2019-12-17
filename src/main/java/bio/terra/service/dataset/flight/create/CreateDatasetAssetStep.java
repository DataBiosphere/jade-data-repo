package bio.terra.service.dataset.flight.create;

import bio.terra.app.controller.exception.ValidationException;
import bio.terra.service.dataset.AssetDao;
import bio.terra.service.dataset.AssetSpecification;
import bio.terra.service.dataset.Dataset;
import bio.terra.service.dataset.DatasetService;
import bio.terra.service.job.JobMapKeys;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.Step;
import bio.terra.stairway.StepResult;

import java.util.List;
import java.util.Optional;
import java.util.UUID;

public class CreateDatasetAssetStep implements Step {

    private DatasetService datasetService;
    private AssetDao assetDao;

    public CreateDatasetAssetStep(DatasetService datasetService, AssetDao assetDao) {
        this.datasetService = datasetService;
        this.assetDao = assetDao;
    }

    private Dataset getDataset(FlightContext context) {
        // Use the dataset id to fetch the Dataset object
        UUID datasetId = UUID.fromString(
            context.getInputParameters().get(JobMapKeys.DATASET_ID.getKeyName(), String.class)
        );
        return datasetService.retrieve(datasetId);
    }

    private AssetSpecification getNewAssetSpec(FlightContext context) {
        // validate the assetspec
        AssetSpecification assetSpecification = context.getInputParameters().get(
            JobMapKeys.REQUEST.getKeyName(), AssetSpecification.class);
        return assetSpecification;
    }

    @Override
    public StepResult doStep(FlightContext context) {
        // TODO: Asset columns and tables need to match things in the dataset schema
        Dataset dataset = getDataset(context);
        // get the dataset assets that already exist --asset name needs to be unique
        AssetSpecification newAssetSpecification = getNewAssetSpec(context);
        List<AssetSpecification> datasetAssetSpecificationList = dataset.getAssetSpecifications();
        if (datasetAssetSpecificationList.stream()
            .anyMatch(asset -> asset.getName().equalsIgnoreCase(newAssetSpecification.getName()))) {
            throw new ValidationException("Can not add an asset to a dataset with a duplicate name");
        }
        assetDao.create(newAssetSpecification, getDataset(context).getId());
        return StepResult.getStepResultSuccess();
    }

    @Override
    public StepResult undoStep(FlightContext context) {
        Dataset dataset = getDataset(context);
        // Search the Asset list in the dataset object to see if the asset you were trying to create got created.
        AssetSpecification newAssetSpecification = getNewAssetSpec(context);
        Optional<AssetSpecification> assetSpecificationToDelete =
            dataset.getAssetSpecificationByName(newAssetSpecification.getName());
        // This only works if we are sure asset names are unique.
        // You cannot assume that the flight object created when the doStep was run is the same flight object
        // when the undoStep is run.
        if (assetSpecificationToDelete.isPresent()) {
            // If the asset is found, then you get its id and call delete.
            assetDao.delete(assetSpecificationToDelete.get().getId());
        }
        // Else, if the asset is not found, then you are done. It never got created.
        return StepResult.getStepResultSuccess();
    }
}

