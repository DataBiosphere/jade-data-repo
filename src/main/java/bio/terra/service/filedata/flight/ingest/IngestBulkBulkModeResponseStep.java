package bio.terra.service.filedata.flight.ingest;

import bio.terra.model.BulkLoadArrayResultModel;
import bio.terra.service.dataset.flight.ingest.IngestMapKeys;
import bio.terra.service.job.DefaultUndoStep;
import bio.terra.service.job.JobMapKeys;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.FlightMap;
import bio.terra.stairway.StepResult;
import org.apache.commons.collections4.CollectionUtils;

// It expects the following working map data:
// - BULK_LOAD_RESULT - a BulkLoadArrayResultModel object
//
public record IngestBulkBulkModeResponseStep(boolean isArrayMode) implements DefaultUndoStep {

  private static final int MAX_FILE_RESULTS = 1000;

  @Override
  public StepResult doStep(FlightContext context) {
    FlightMap workingMap = context.getWorkingMap();
    BulkLoadArrayResultModel result =
        workingMap.get(IngestMapKeys.BULK_LOAD_RESULT, BulkLoadArrayResultModel.class);

    if (isArrayMode) {
      workingMap.put(JobMapKeys.RESPONSE.getKeyName(), result);
    } else {
      // Truncate file results if needed.  Note: not doing this for arrays since that API takes in
      // the values so presumably can return them back.  This is explicitly for the case where the
      // file data is loaded from a cloud file
      if (CollectionUtils.size(result.getLoadFileResults()) > MAX_FILE_RESULTS) {
        result.loadFileResults(result.getLoadFileResults().subList(0, MAX_FILE_RESULTS));
      }
      workingMap.put(JobMapKeys.RESPONSE.getKeyName(), result.getLoadSummary());
    }

    return StepResult.getStepResultSuccess();
  }
}
