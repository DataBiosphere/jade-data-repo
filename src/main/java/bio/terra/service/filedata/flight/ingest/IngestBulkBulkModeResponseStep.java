package bio.terra.service.filedata.flight.ingest;

import bio.terra.model.BulkLoadArrayResultModel;
import bio.terra.service.dataset.flight.ingest.IngestMapKeys;
import bio.terra.service.job.JobMapKeys;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.FlightMap;
import bio.terra.stairway.Step;
import bio.terra.stairway.StepResult;

// It expects the following working map data:
// - BULK_LOAD_RESULT - a BulkLoadArrayResultModel object
//
public class IngestBulkBulkModeResponseStep implements Step {

  private final boolean isArrayMode;

  public IngestBulkBulkModeResponseStep(boolean isArrayMode) {
    this.isArrayMode = isArrayMode;
  }

  @Override
  public StepResult doStep(FlightContext context) {
    FlightMap workingMap = context.getWorkingMap();
    BulkLoadArrayResultModel result =
        workingMap.get(IngestMapKeys.BULK_LOAD_RESULT, BulkLoadArrayResultModel.class);

    if (isArrayMode) {
      workingMap.put(JobMapKeys.RESPONSE.getKeyName(), result);
    } else {
      workingMap.put(JobMapKeys.RESPONSE.getKeyName(), result.getLoadSummary());
    }

    return StepResult.getStepResultSuccess();
  }

  @Override
  public StepResult undoStep(FlightContext context) {
    return StepResult.getStepResultSuccess();
  }
}
