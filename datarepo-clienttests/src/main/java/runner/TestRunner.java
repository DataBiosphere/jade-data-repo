package runner;

import bio.terra.datarepo.client.ApiClient;
import bio.terra.datarepo.client.Configuration;
import runner.config.TestConfiguration;
import runner.config.TestScriptSpecification;

import javax.ws.rs.client.Client;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;

class TestRunner {

    TestConfiguration config;
    List<TestScript> scripts;
    Client httpClient; // TODO: singleton ok or change to have one per ApiClient?

    TestRunner(TestConfiguration config) {
        this.config = config;
        this.scripts = new ArrayList<>();
    }

    void executeTestConfiguration() {
        // get an instance of each test script class
        for (TestScriptSpecification testScriptSpecification : config.testScripts) {
            try {
                TestScript testScriptInstance = testScriptSpecification.scriptClass.newInstance();
                scripts.add(testScriptInstance);
            } catch (IllegalAccessException|InstantiationException niEx) {
                throw new IllegalArgumentException("Error calling constructor of TestScript class: "
                    + testScriptSpecification.scriptClass.getName(), niEx);
            }
        }

        // setup the default API client for the setup/cleanup methods to use
        ApiClient apiClient = new ApiClient();
        apiClient.setBasePath(config.server.uri);
        Configuration.setDefaultApiClient(apiClient);

        // call the setup method of each test script
        for (TestScript testScript : scripts) {
            testScript.setup();
        }

        // TODO: create the client thread pool

        // TODO: kick off the user journey(s), one per thread
        for (TestScript testScript : scripts) {
            testScript.userJourney();
        }

        // TODO: wait until all threads either finish or time out

        // call the cleanup method of each test script
        for (TestScript testScript : scripts) {
            testScript.cleanup();
        }
    }

    static void printHelp() {
        System.out.println("Specify test configuration file as first argument.");
        System.out.println("  e.g. ../gradlew :run --args=\"BasicConfig.json\"");
    }

    public static void main(String[] args) throws Exception {
        // if no args specified, print help and list of available test configurations
        if (args.length < 1) {
            printHelp();
        }

        // read in configuration, validate it, and print to stdout
        TestConfiguration testConfiguration = TestConfiguration.fromJSONFile(args[0]);
        testConfiguration.validate();
        testConfiguration.display();

        // get an instance of a runner and tell it to execute the configuration
        TestRunner runner = new TestRunner(testConfiguration);
        runner.executeTestConfiguration();
    }

}
