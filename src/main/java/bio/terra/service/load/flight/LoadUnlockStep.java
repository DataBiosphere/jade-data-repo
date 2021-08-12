package bio.terra.service.load.flight;

import bio.terra.service.dataset.flight.ingest.SkippableStep;
import bio.terra.service.load.LoadService;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.StepResult;
import java.util.function.Predicate;

// This step is meant to be shared by dataset and filesystem flights for locking the load tag.
// It expects to find LoadMapKeys.LOAD_TAG in the working map.

public class LoadUnlockStep extends SkippableStep {
  private final LoadService loadService;

  public LoadUnlockStep(LoadService loadService, Predicate<FlightContext> skipCondition) {
    super(skipCondition);
    this.loadService = loadService;
  }

  public LoadUnlockStep(LoadService loadService) {
    this.loadService = loadService;
  }

  @Override
  public StepResult doSkippableStep(FlightContext context) {
    String loadTag = loadService.getLoadTag(context);
    loadService.unlockLoad(loadTag, context.getFlightId());
    return StepResult.getStepResultSuccess();
  }
}
