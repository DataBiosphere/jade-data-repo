package bio.terra.stairway;

public class TestStopController {
    private volatile int control;
    private static TestStopController singleton;

    public TestStopController() {
        control = 0;
    }

    public static int getControl() {
        if (singleton == null) {
            singleton = new TestStopController();
        }
        return singleton.control;
    }

    public static void setControl(int control) {
        if (singleton == null) {
            singleton = new TestStopController();
        }
        singleton.control = control;
    }
}
