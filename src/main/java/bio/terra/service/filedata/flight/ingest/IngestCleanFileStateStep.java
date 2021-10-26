package bio.terra.service.filedata.flight.ingest;

import bio.terra.service.dataset.flight.ingest.OptionalStep;
import bio.terra.service.load.LoadService;
import bio.terra.service.load.flight.LoadMapKeys;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.FlightMap;
import bio.terra.stairway.StepResult;
import java.util.UUID;
import java.util.function.Predicate;

// Populate the files to be loaded from the incoming array
public class IngestCleanFileStateStep extends OptionalStep {

  private final LoadService loadService;

  public IngestCleanFileStateStep(LoadService loadService, Predicate<FlightContext> doCondition) {
    super(doCondition);
    this.loadService = loadService;
  }

  public IngestCleanFileStateStep(LoadService loadService) {
    this(loadService, OptionalStep::alwaysDo);
  }

  @Override
  public StepResult doOptionalStep(FlightContext context) {
    FlightMap workingMap = context.getWorkingMap();
    UUID loadId = UUID.fromString(workingMap.get(LoadMapKeys.LOAD_ID, String.class));
    loadService.cleanFiles(loadId);
    return StepResult.getStepResultSuccess();
  }
}
