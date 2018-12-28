package bio.terra.stairway;

public class TestStepIncrement implements Step {

    @Override
    public StepResult doStep(FlightContext context) {
        SafeHashMap inputs = context.getInputParameters();
        SafeHashMap workingMap = context.getWorkingMap();


        Integer value = workingMap.get("value", Integer.class);
        System.out.println("TestStepIncrement - do - start value is: " + value);

        if (value == null) {
            // Value hasn't been set yet, so we set it to initial value
            value = inputs.get("initialValue", Integer.class);
        }
        value++;
        workingMap.put("value", value);
        System.out.println("TestStepIncrement - do - end value is: " + value);

        return StepResult.getStepResultSuccess();
    }

    @Override
    public StepResult undoStep(FlightContext context) {
        SafeHashMap workingMap = context.getWorkingMap();
        Integer value = workingMap.get("value", Integer.class);
        System.out.println("TestStepIncrement - undo - start value is: " + value);

        if (value != null) {
            value--;
            workingMap.put("value", value);
            System.out.println("TestStepIncrement - undo - end value is: " + value);
        }

        return StepResult.getStepResultSuccess();
    }
}
