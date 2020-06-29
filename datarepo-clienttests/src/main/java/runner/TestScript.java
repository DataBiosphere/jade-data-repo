package runner;

import bio.terra.datarepo.client.ApiClient;
import bio.terra.datarepo.client.Configuration;

import java.util.HashMap;
import java.util.Map;

public class TestScript {

    /**
     * Public constructor so that this class can be instantiated via reflection.
     */
    public TestScript() { }

    /**
     * The test script setup contains the API call(s) that we do not want to profile and will not be scaled to run
     * multiple in parallel. setup() is called once at the beginning of the test run.
     */
    public void setup(Map<String, ApiClient> apiClients) throws Exception { }

    /**
     * The test script userJourney contains the API call(s) that we want to profile and it may be scaled to run
     * multiple journeys in parallel.
     */
    public void userJourney(ApiClient apiClient) throws Exception {
        throw new UnsupportedOperationException("userJourney must be overridden by sub-classes");
    }

    /**
     * The test script cleanup contains the API call(s) that we do not want to profile and will not be scaled to run
     * multiple in parallel. cleanup() is called once at the end of the test run.
     */
    public void cleanup(Map<String, ApiClient> apiClients) throws Exception { }

    /**
     * This method runs the test script methods in the expected order: setup, userJourney, cleanup.
     * It is intended for easier debugging (e.g. API calls, building request models) when writing a new script.
     * @param args
     */
    public static void main(String[] args) throws Exception {
        // get an instance of the script and the API client
        TestScript testScript = new TestScript();

        ApiClient apiClient = Configuration.getDefaultApiClient();
        Map<String, ApiClient> apiClients = new HashMap<>();
        apiClients.put("default", apiClient);

        testScript.setup(apiClients);
        testScript.userJourney(apiClient);
        testScript.cleanup(apiClients);
    }
}

