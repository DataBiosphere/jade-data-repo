package bio.terra.stairway;

public class TestStopController {
    private volatile int control;
    private static TestStopController singleton = new TestStopController();

    public TestStopController() {
        control = 0;
    }

    public static int getControl() {
        return singleton.control;
    }

    public static void setControl(int control) {
        singleton.control = control;
    }
}
