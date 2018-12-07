package bio.terra.stairway;


/**
 * Step must implement a do and an undo method. The do performs the step and the undo
 * removes the effects of the step. Care must be taken in their implementation to
 * make sure that a failure at any point can be either continued or undone. The best
 * way to do that is to make each step do only one thing.
 */
public interface Step {
    /**
     * Called by the Flight controller when running "forward" on the success path
     *
     * @param context The sequencer context
     * @returns step result
     */
     StepResult doStep(FlightContext context);

    /**
     * Called by the sequencer when running "backward" on the failure/rollback path
     *
     * @param context The sequencer context
     * @returns step result
     */
    StepResult undoStep(FlightContext context);
}
