package bio.terra.service.dataset.flight.create;

import bio.terra.service.dataset.AssetDao;
import bio.terra.service.job.JobMapKeys;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.FlightMap;
import bio.terra.stairway.Step;
import bio.terra.stairway.StepResult;
import java.util.UUID;
import org.springframework.http.HttpStatus;

public class DeleteDatasetAssetStep implements Step {

  private final AssetDao assetDao;

  public DeleteDatasetAssetStep(AssetDao assetDao) {
    this.assetDao = assetDao;
  }

  private UUID getAssetId(FlightContext context) {
    return JobMapKeys.ASSET_ID.get(context.getInputParameters());
  }

  @Override
  public StepResult doStep(FlightContext context) {
    assetDao.delete(getAssetId(context));
    FlightMap map = context.getWorkingMap();
    JobMapKeys.STATUS_CODE.put(map, HttpStatus.NO_CONTENT);
    return StepResult.getStepResultSuccess();
  }

  @Override
  public StepResult undoStep(FlightContext context) {
    // can't undo delete
    return StepResult.getStepResultSuccess();
  }
}
