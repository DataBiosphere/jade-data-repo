package bio.terra.service.dataset.flight.ingest;

import bio.terra.model.BulkLoadArrayResultModel;
import bio.terra.model.BulkLoadFileResultModel;
import bio.terra.model.BulkLoadResultModel;
import bio.terra.service.job.DefaultUndoStep;
import bio.terra.service.load.LoadService;
import bio.terra.service.load.flight.LoadMapKeys;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.FlightMap;
import bio.terra.stairway.StepResult;
import java.util.List;
import java.util.UUID;

// It expects the following working map data:
// - LOAD_ID - load id we are working on
//
public class IngestBulkMapResponseStep extends DefaultUndoStep {

  private final LoadService loadService;
  private final String loadTag;

  public IngestBulkMapResponseStep(LoadService loadService, String loadTag) {
    this.loadService = loadService;
    this.loadTag = loadTag;
  }

  @Override
  public StepResult doStep(FlightContext context) {
    FlightMap workingMap = context.getWorkingMap();
    String loadIdString = workingMap.get(LoadMapKeys.LOAD_ID, String.class);
    UUID loadId = UUID.fromString(loadIdString);

    BulkLoadArrayResultModel result = makeLoadResult(loadId, context);
    workingMap.put(IngestMapKeys.BULK_LOAD_RESULT, result);

    return StepResult.getStepResultSuccess();
  }

  private BulkLoadArrayResultModel makeLoadResult(UUID loadId, FlightContext context) {
    // Get the summary stats and fill in our specific information
    BulkLoadResultModel summary = loadService.makeBulkLoadResult(loadId);
    summary.loadTag(loadTag).jobId(context.getFlightId());

    // Get the file load results
    List<BulkLoadFileResultModel> fileResults = loadService.makeBulkLoadFileArray(loadId);

    return new BulkLoadArrayResultModel().loadSummary(summary).loadFileResults(fileResults);
  }
}
