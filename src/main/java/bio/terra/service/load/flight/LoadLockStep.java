package bio.terra.service.load.flight;

import bio.terra.service.load.LoadService;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.Step;
import bio.terra.stairway.StepResult;

// This step is meant to be shared by dataset and filesystem flights for locking the load tag.
// It expects to find LoadMapKeys.LOAD_TAG in the working map.

public class LoadLockStep implements Step {
    private final LoadService loadService;

    public LoadLockStep(LoadService loadService) {
        this.loadService = loadService;
    }

    @Override
    public StepResult doStep(FlightContext context) {
        String loadTag = loadService.getLoadTag(context);
        loadService.lockLoad(loadTag, context.getFlightId());
        return StepResult.getStepResultSuccess();
    }

    @Override
    public StepResult undoStep(FlightContext context) {
        String loadTag = loadService.getLoadTag(context);
        loadService.unlockLoad(loadTag, context.getFlightId());
        return StepResult.getStepResultSuccess();
    }
}
