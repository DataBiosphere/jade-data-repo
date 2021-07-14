package bio.terra.service.dataset.flight.ingest;

import bio.terra.model.BulkLoadFileModel;
import bio.terra.service.load.LoadService;
import bio.terra.service.load.flight.LoadMapKeys;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.FlightMap;
import bio.terra.stairway.Step;
import bio.terra.stairway.StepResult;
import java.util.List;
import java.util.Set;
import java.util.UUID;

// Populate the files to be loaded from the incoming array
public class IngestPopulateFileStateFromFlightMapStep implements Step {

  private final LoadService loadService;

  public IngestPopulateFileStateFromFlightMapStep(LoadService loadService) {
    this.loadService = loadService;
  }

  @Override
  public StepResult doStep(FlightContext context) {
    FlightMap inputParameters = context.getInputParameters();

    FlightMap workingMap = context.getWorkingMap();
    UUID loadId = UUID.fromString(workingMap.get(LoadMapKeys.LOAD_ID, String.class));
    Set<BulkLoadFileModel> models = workingMap.get(IngestMapKeys.BULK_LOAD_FILE_MODELS, Set.class);

    loadService.populateFiles(loadId, List.copyOf(models));

    return StepResult.getStepResultSuccess();
  }

  @Override
  public StepResult undoStep(FlightContext context) {
    FlightMap workingMap = context.getWorkingMap();
    UUID loadId = UUID.fromString(workingMap.get(LoadMapKeys.LOAD_ID, String.class));

    loadService.cleanFiles(loadId);
    return StepResult.getStepResultSuccess();
  }
}
