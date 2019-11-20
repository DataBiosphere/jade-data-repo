package bio.terra.service.dataset.flight.create;

import bio.terra.service.dataset.AssetDao;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.Step;
import bio.terra.stairway.StepResult;

import java.util.UUID;

public class DeleteDatasetAssetStep implements Step {

    private AssetDao assetDao;
    private UUID assetId;

    public DeleteDatasetAssetStep(AssetDao assetDao, UUID assetId) {
        this.assetDao = assetDao;
        this.assetId = assetId;
    }

    @Override
    public StepResult doStep(FlightContext context) {
        assetDao.delete(assetId);
        return StepResult.getStepResultSuccess();
    }

    @Override
    public StepResult undoStep(FlightContext context) {
        // can't undo delete ?!?!??!
        return StepResult.getStepResultSuccess();
    }
}

