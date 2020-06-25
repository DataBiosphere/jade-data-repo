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
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

class TestRunner {

    TestConfiguration config;
    List<TestScript> scripts;
    List<ThreadPoolExecutor> threadPools;
    List<List<Future<UserJourneyResult>>> userJourneyFutureLists;

    Client httpClient; // TODO: singleton ok or change to have one per ApiClient?
    static long secondsToWaitForPoolShutdown = 60;

    TestRunner(TestConfiguration config) {
        this.config = config;
        this.scripts = new ArrayList<>();
        this.threadPools = new ArrayList<>();
        this.userJourneyFutureLists = new ArrayList<>();
    }

    List<UserJourneyResult> executeTestConfiguration() throws InterruptedException {
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

        // for each test script
        for (int ctr = 0; ctr < scripts.size(); ctr++) {
            TestScript testScript = scripts.get(ctr);
            TestScriptSpecification testScriptSpecification = config.testScripts.get(ctr);

            // create a thread pool for running its user journeys
            ThreadPoolExecutor threadPool =
                (ThreadPoolExecutor) Executors.newFixedThreadPool(testScriptSpecification.numberToRunInParallel);
            threadPools.add(threadPool);

            // kick off the user journey(s), one per thread
            List<UserJourneyThread> userJourneyThreads = Collections.nCopies(testScriptSpecification.totalNumberToRun,
                new UserJourneyThread(testScript, testScriptSpecification.name));

            // TODO: support different patterns of kicking off user journeys. here they're all queued at once
            List<Future<UserJourneyResult>> userJourneyFutures = threadPool.invokeAll(userJourneyThreads);
            userJourneyFutureLists.add(userJourneyFutures);
        }

        // wait until all threads either finish or time out
        for (int ctr = 0; ctr < scripts.size(); ctr++) {
            TestScriptSpecification testScriptSpecification = config.testScripts.get(ctr);
            ThreadPoolExecutor threadPool = threadPools.get(ctr);

            threadPool.shutdown();
            long totalTerminationTime =
                testScriptSpecification.expectedTimeForEach * testScriptSpecification.totalNumberToRun;
            boolean terminatedByItself =
                threadPool.awaitTermination(totalTerminationTime, testScriptSpecification.expectedTimeForEachUnitObj);

            // if the threads didn't finish in the expected time, then send them interrupts
            if (!terminatedByItself) {
                threadPool.shutdownNow();
            }
            if (!threadPool.awaitTermination(secondsToWaitForPoolShutdown, TimeUnit.SECONDS)) {
                System.out.println("Thread pool for test script " + ctr
                    + " (" + testScriptSpecification.name + ") failed to terminate.");
            }
        }

        // call the cleanup method of each test script
        for (TestScript testScript : scripts) {
            testScript.cleanup();
        }

        // compile and return the results
        List<UserJourneyResult> results = new ArrayList<>();
        for (int ctr = 0; ctr < scripts.size(); ctr++) {
            List<Future<UserJourneyResult>> userJourneyFutureList = userJourneyFutureLists.get(ctr);
            TestScriptSpecification testScriptSpecification = config.testScripts.get(ctr);

            for (Future<UserJourneyResult> userJourneyFuture : userJourneyFutureList) {
                UserJourneyResult result = null;
                if (userJourneyFuture.isDone()) try {
                    result = userJourneyFuture.get();
                    result.completed = true;
                } catch (ExecutionException execEx) {
                    result = new UserJourneyResult(testScriptSpecification.name, "");
                    result.completed = true;
                    result.exceptionThrown = execEx;
                } else {
                    result = new UserJourneyResult(testScriptSpecification.name, "");
                    result.completed = false;
                }
                results.add(result);
            }
        }
        return results;
    }

    static class UserJourneyThread implements Callable<UserJourneyResult> {
        TestScript testScript;
        String testScriptName;

        public UserJourneyThread(TestScript testScript, String testScriptName) {
            this.testScript = testScript;
            this.testScriptName = testScriptName;
        }

        public UserJourneyResult call() {
            UserJourneyResult result = new UserJourneyResult(testScriptName, Thread.currentThread().getName());

            try {
                testScript.userJourney();
            } catch (Exception ex) {
                result.exceptionThrown = ex;
            }
            return result;
        }
    }

    static void printHelp() {
        System.out.println("Specify test configuration file as first argument.");
        System.out.println("  e.g. ../gradlew :run --args=\"BasicConfig.json\"");
    }

    public static void main(String[] args) throws Exception {
        // if no args specified, print help
        if (args.length < 1) {
            printHelp();
            // TODO: also print list of available test configurations
        }

        // read in configuration, validate it, and print to stdout
        TestConfiguration testConfiguration = TestConfiguration.fromJSONFile(args[0]);
        testConfiguration.validate();
        testConfiguration.display();

        // get an instance of a runner and tell it to execute the configuration
        TestRunner runner = new TestRunner(testConfiguration);
        List<UserJourneyResult> results = runner.executeTestConfiguration();

        // print the results to stdout
        for (UserJourneyResult result : results) {
            result.display();
        }
    }

}
