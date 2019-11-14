package bio.terra.service.dataset.flight.create;

import bio.terra.model.AssetModel;
import bio.terra.service.dataset.AssetDao;
import bio.terra.service.dataset.AssetSpecification;
import bio.terra.service.dataset.DatasetJsonConversion;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.Step;
import bio.terra.stairway.StepResult;

import java.util.UUID;

public class CreateDatasetAssetStep implements Step {

    private UUID datasetId;
    private AssetDao assetDao;
    private AssetModel assetModel;

    public CreateDatasetAssetStep(UUID datasetId, AssetDao assetDao, AssetModel assetModel) {
        this.datasetId = datasetId;
        this.assetDao = assetDao;
        this.assetModel = assetModel;
    }

    @Override
    public StepResult doStep(FlightContext context) {
        // TODO this needs the dataset tables and relationships added in
        AssetSpecification assetSpecification = DatasetJsonConversion.assetModelToAssetSpecification(assetModel);
        assetDao.create(assetSpecification, datasetId);
        return StepResult.getStepResultSuccess();
    }

    @Override
    public StepResult undoStep(FlightContext context) {
        // TODO is there a asset deletion method, or is that going to need to get added as well?
        return StepResult.getStepResultSuccess();
    }
}

