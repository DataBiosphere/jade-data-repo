package bio.terra.service.dataset.flight.create;

import bio.terra.app.controller.exception.ValidationException;
import bio.terra.service.dataset.AssetDao;
import bio.terra.service.job.JobMapKeys;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.FlightMap;
import bio.terra.stairway.Step;
import bio.terra.stairway.StepResult;
import java.util.List;
import java.util.UUID;
import org.apache.commons.lang3.StringUtils;
import org.springframework.http.HttpStatus;

public class DeleteDatasetAssetStep implements Step {

  private AssetDao assetDao;

  public DeleteDatasetAssetStep(AssetDao assetDao) {
    this.assetDao = assetDao;
  }

  @Override
  public StepResult doStep(FlightContext context) {
    FlightMap inputs = context.getInputParameters();
    UUID datasetId = UUID.fromString(inputs.get(JobMapKeys.DATASET_ID.getKeyName(), String.class));
    UUID assetId = UUID.fromString(inputs.get(JobMapKeys.ASSET_ID.getKeyName(), String.class));
    List<UUID> snapshotIds = assetDao.retrieveSnapshotsForAsset(datasetId, assetId);
    if (snapshotIds.size() != 0) {
      throw new ValidationException(
          "The asset is being used by snapshots: " + StringUtils.join(snapshotIds, ","));
    }
    assetDao.delete(assetId);
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
