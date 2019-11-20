package bio.terra.service.dataset.flight.create;

import bio.terra.service.dataset.AssetDao;
import bio.terra.service.dataset.AssetSpecification;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.Step;
import bio.terra.stairway.StepResult;

import java.util.UUID;

public class CreateDatasetAssetStep implements Step {

    private UUID datasetId;
    private AssetDao assetDao;
    private AssetSpecification assetSpecification;

    public CreateDatasetAssetStep(UUID datasetId, AssetDao assetDao, AssetSpecification assetSpecification) {
        this.datasetId = datasetId;
        this.assetDao = assetDao;
        this.assetSpecification = assetSpecification;
    }

    @Override
    public StepResult doStep(FlightContext context) {
        assetDao.create(assetSpecification, datasetId);
        return StepResult.getStepResultSuccess();
    }

    @Override
    public StepResult undoStep(FlightContext context) {
        assetDao.delete(assetSpecification.getId());
        return StepResult.getStepResultSuccess();
    }
}

