package bio.terra.stairway;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TestStepIncrement implements Step {
    private Logger logger = LoggerFactory.getLogger("bio.terra.stairway");

    @Override
    public StepResult doStep(FlightContext context) {
        FlightMap inputs = context.getInputParameters();
        FlightMap workingMap = context.getWorkingMap();


        Integer value = workingMap.get("value", Integer.class);
        logger.debug("TestStepIncrement - do - start value is: " + value);

        if (value == null) {
            // Value hasn't been set yet, so we set it to initial value
            value = inputs.get("initialValue", Integer.class);
        }
        value++;
        workingMap.put("value", value);
        logger.debug("TestStepIncrement - do - end value is: " + value);

        return StepResult.getStepResultSuccess();
    }

    @Override
    public StepResult undoStep(FlightContext context) {
        FlightMap workingMap = context.getWorkingMap();
        Integer value = workingMap.get("value", Integer.class);
        logger.debug("TestStepIncrement - undo - start value is: " + value);

        if (value != null) {
            value--;
            workingMap.put("value", value);
            logger.debug("TestStepIncrement - undo - end value is: " + value);
        }

        return StepResult.getStepResultSuccess();
    }
}
