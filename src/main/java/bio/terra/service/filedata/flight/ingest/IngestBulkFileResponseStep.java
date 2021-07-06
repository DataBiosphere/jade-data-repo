package bio.terra.service.filedata.flight.ingest;

import bio.terra.model.BulkLoadResultModel;
import bio.terra.service.job.JobMapKeys;
import bio.terra.service.load.LoadService;
import bio.terra.service.load.flight.LoadMapKeys;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.FlightMap;
import bio.terra.stairway.Step;
import bio.terra.stairway.StepResult;
import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

// It expects the following working map data:
// - LOAD_ID - load id we are working on
//
public class IngestBulkFileResponseStep implements Step {
  private final Logger logger = LoggerFactory.getLogger(IngestBulkFileResponseStep.class);

  private final LoadService loadService;
  private final String loadTag;

  public IngestBulkFileResponseStep(LoadService loadService, String loadTag) {
    this.loadService = loadService;
    this.loadTag = loadTag;
  }

  @Override
  public StepResult doStep(FlightContext context) {
    FlightMap workingMap = context.getWorkingMap();
    String loadIdString = workingMap.get(LoadMapKeys.LOAD_ID, String.class);
    UUID loadId = UUID.fromString(loadIdString);

    // Get the summary stats and fill in our specific information
    BulkLoadResultModel summary = loadService.makeBulkLoadResult(loadId);
    summary.loadTag(loadTag).jobId(context.getFlightId());
    workingMap.put(JobMapKeys.RESPONSE.getKeyName(), summary);

    return StepResult.getStepResultSuccess();
  }

  @Override
  public StepResult undoStep(FlightContext context) {
    return StepResult.getStepResultSuccess();
  }
}
