package runner;

public class TestScript {

    /**
     * Public constructor so that this class can be instantiated via reflection.
     */
    public TestScript() { }

    /**
     * The test script setup contains the API call(s) that we do not want to profile and will not be scaled to run
     * multiple in parallel. setup() is called once at the beginning of the test run.
     */
    public void setup() throws Exception { }

    /**
     * The test script userJourney contains the API call(s) that we want to profile and it may be scaled to run
     * multiple journeys in parallel.
     */
    public void userJourney() throws Exception {
        throw new UnsupportedOperationException("userJourney must be overridden by sub-classes");
    }

    /**
     * The test script cleanup contains the API call(s) that we do not want to profile and will not be scaled to run
     * multiple in parallel. cleanup() is called once at the end of the test run.
     */
    public void cleanup() throws Exception { }

    /**
     * This method runs the test script methods in the expected order: setup, userJourney, cleanup.
     * It is intended for easier debugging (e.g. API calls, building request models) when writing a new script.
     * @param args
     */
    public static void main(String[] args) throws Exception {
        // get an instance of the script
        TestScript testScript = new TestScript();

        testScript.setup();
        testScript.userJourney();
        testScript.cleanup();
    }
}

