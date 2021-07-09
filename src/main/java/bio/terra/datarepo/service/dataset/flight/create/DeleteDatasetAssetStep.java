package bio.terra.datarepo.service.dataset.flight.create;

import bio.terra.datarepo.service.dataset.AssetDao;
import bio.terra.datarepo.service.job.JobMapKeys;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.FlightMap;
import bio.terra.stairway.Step;
import bio.terra.stairway.StepResult;
import java.util.UUID;
import org.springframework.http.HttpStatus;

public class DeleteDatasetAssetStep implements Step {

  private AssetDao assetDao;

  public DeleteDatasetAssetStep(AssetDao assetDao) {
    this.assetDao = assetDao;
  }

  private UUID getAssetId(FlightContext context) {
    // get asset Id
    UUID assetId =
        UUID.fromString(
            context.getInputParameters().get(JobMapKeys.ASSET_ID.getKeyName(), String.class));
    return assetId;
  }

  @Override
  public StepResult doStep(FlightContext context) {
    assetDao.delete(getAssetId(context));
    FlightMap map = context.getWorkingMap();
    map.put(JobMapKeys.STATUS_CODE.getKeyName(), HttpStatus.NO_CONTENT);
    return StepResult.getStepResultSuccess();
  }

  @Override
  public StepResult undoStep(FlightContext context) {
    // can't undo delete
    return StepResult.getStepResultSuccess();
  }
}
