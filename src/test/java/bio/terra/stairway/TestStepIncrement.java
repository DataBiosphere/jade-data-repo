package bio.terra.stairway;

import static bio.terra.stairway.TestUtil.debugWrite;

public class TestStepIncrement implements Step {

    @Override
    public StepResult doStep(FlightContext context) {
        FlightMap inputs = context.getInputParameters();
        FlightMap workingMap = context.getWorkingMap();


        Integer value = workingMap.get("value", Integer.class);
        debugWrite("TestStepIncrement - do - start value is: " + value);

        if (value == null) {
            // Value hasn't been set yet, so we set it to initial value
            value = inputs.get("initialValue", Integer.class);
        }
        value++;
        workingMap.put("value", value);
        debugWrite("TestStepIncrement - do - end value is: " + value);

        return StepResult.getStepResultSuccess();
    }

    @Override
    public StepResult undoStep(FlightContext context) {
        FlightMap workingMap = context.getWorkingMap();
        Integer value = workingMap.get("value", Integer.class);
        debugWrite("TestStepIncrement - undo - start value is: " + value);

        if (value != null) {
            value--;
            workingMap.put("value", value);
            debugWrite("TestStepIncrement - undo - end value is: " + value);
        }

        return StepResult.getStepResultSuccess();
    }
}
