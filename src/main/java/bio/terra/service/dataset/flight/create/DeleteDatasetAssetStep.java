package bio.terra.service.dataset.flight.create;

import bio.terra.service.dataset.dao.DatasetDao;
import bio.terra.service.job.JobMapKeys;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.FlightMap;
import bio.terra.stairway.Step;
import bio.terra.stairway.StepResult;
import org.springframework.http.HttpStatus;

import java.util.UUID;

public class DeleteDatasetAssetStep implements Step {

    private DatasetDao datasetDao;

    public DeleteDatasetAssetStep(DatasetDao datasetDao) {
        this.datasetDao = datasetDao;
    }

    private UUID getAssetId(FlightContext context) {
        // get asset Id
        UUID assetId = UUID.fromString(
            context.getInputParameters().get(JobMapKeys.ASSET_ID.getKeyName(), String.class)
        );
        return assetId;
    }

    @Override
    public StepResult doStep(FlightContext context) {
        datasetDao.deleteAsset(getAssetId(context));
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

