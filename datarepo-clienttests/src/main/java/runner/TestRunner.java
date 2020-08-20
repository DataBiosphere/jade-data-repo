package runner;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import measurementcollectionscripts.CPUCoreUsageTimeMetric;
import measurementcollectionscripts.LoggerInterceptorHttpStatus;
import measurementcollectionscripts.MemoryUsedBytesMetric;
import measurementcollectionscripts.PerformanceLoggerElapsedTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import runner.config.TestConfiguration;
import runner.config.TestScriptSpecification;
import runner.config.TestSuite;
import runner.config.TestUserSpecification;
import utils.FileUtils;
import utils.KubernetesClientUtils;

class TestRunner {

  private static final Logger logger = LoggerFactory.getLogger(TestRunner.class);

  TestConfiguration config;
  List<TestScript> scripts;
  DeploymentScript deploymentScript;
  List<ThreadPoolExecutor> threadPools;
  ThreadPoolExecutor disruptionThreadPool;
  List<List<Future<UserJourneyResult>>> userJourneyFutureLists;
  List<TestScriptResult> testScriptResults;
  long startTime;
  long endTime;
  List<MeasurementCollectionScript> measurementCollectionScripts;

  static long secondsToWaitForPoolShutdown = 60;

  TestRunner(TestConfiguration config) {
    this.config = config;
    this.scripts = new ArrayList<>();
    this.threadPools = new ArrayList<>();
    this.disruptionThreadPool = null;
    this.userJourneyFutureLists = new ArrayList<>();
    this.testScriptResults = new ArrayList<>();
    this.startTime = -1;
    this.endTime = -1;
    this.measurementCollectionScripts = new ArrayList<>();
  }

  void executeTestConfiguration() throws Exception {
    try {
      executeTestConfigurationNoGuaranteedCleanup();
    } catch (Exception originalEx) {
      // cleanup deployment (i.e. run teardown method)
      try {
        if (!config.server.skipDeployment) {
          logger.info(
              "Deployment: Calling {}.teardown() after failure",
              deploymentScript.getClass().getName());
          deploymentScript.teardown();
        }
      } catch (Exception deploymentTeardownEx) {
        logger.error(
            "Deployment: Exception during forced deployment teardown", deploymentTeardownEx);
      }

      // cleanup test scripts (i.e. run cleanup methods)
      logger.info("Test Scripts: Calling the cleanup methods after failure");
      try {
        callTestScriptCleanups();
      } catch (Exception testScriptCleanupEx) {
        logger.error(
            "Test Scripts: Exception during forced test script cleanups", testScriptCleanupEx);
      }

      // cleanup data project (i.e. run cloud cleanup scripts)
      logger.info("Data Project: Cleaning up the data project after failure");
      try {
        cleanupDataProject();
      } catch (Exception leftoverTestDataEx) {
        logger.error(
            "Data Project: Exception during forced data project cleanup", leftoverTestDataEx);
      }

      throw originalEx;
    }
  }

  void executeTestConfigurationNoGuaranteedCleanup() throws Exception {
    // specify any value overrides in the Helm chart, then deploy
    if (!config.server.skipDeployment) {
      // get an instance of the deployment script class
      try {
        deploymentScript = config.server.deploymentScript.scriptClass.newInstance();
      } catch (IllegalAccessException | InstantiationException niEx) {
        logger.error(
            "Deployment: Error calling constructor of DeploymentScript class: {}",
            config.server.deploymentScript.name,
            niEx);
        throw new IllegalArgumentException(
            "Error calling constructor of DeploymentScript class: "
                + config.server.deploymentScript.name,
            niEx);
      }

      // set any parameters specified by the configuration
      deploymentScript.setParameters(config.server.deploymentScript.parameters);

      // call the deploy and waitForDeployToFinish methods to do the deployment
      logger.info("Deployment: Calling {}.deploy()", deploymentScript.getClass().getName());
      deploymentScript.deploy(config.server, config.application);

      logger.info(
          "Deployment: Calling {}.waitForDeployToFinish()", deploymentScript.getClass().getName());
      deploymentScript.waitForDeployToFinish();
    } else {
      logger.info("Deployment: Skipping deployment");
    }

    // update any Kubernetes properties specified by the test configuration
    if (!config.server.skipKubernetes) {
      KubernetesClientUtils.buildKubernetesClientObject(config.server);
      modifyKubernetesPostDeployment();
    } else {
      logger.info("Kubernetes: Skipping Kubernetes configuration post-deployment");
    }

    // setup the instance of each test script class
    logger.info(
        "Test Scripts: Fetching instance of each class, setting billing account and parameters");
    for (TestScriptSpecification testScriptSpecification : config.testScripts) {
      TestScript testScriptInstance = testScriptSpecification.scriptClassInstance();

      // set the billing account for the test script to use
      testScriptInstance.setBillingAccount(config.billingAccount);

      // set the server specification for the test script to run against
      testScriptInstance.setServer(config.server);

      // set any parameters specified by the configuration
      testScriptInstance.setParameters(testScriptSpecification.parameters);

      scripts.add(testScriptInstance);
    }

    // call the setup method of each test script
    logger.info("Test Scripts: Calling the setup methods");
    Exception setupExceptionThrown = callTestScriptSetups();
    if (setupExceptionThrown != null) {
      logger.error("Test Scripts: Error calling test script setup methods", setupExceptionThrown);
      throw new RuntimeException("Error calling test script setup methods.", setupExceptionThrown);
    }

    // Disruptive Thread: fetch script specification if config is defined
    if (config.disruptiveScript != null) {
      logger.debug("Creating thread pool for disruptive script.");
      DisruptiveScript disruptiveScript = config.disruptiveScript.disruptiveScriptClassInstance();
      disruptiveScript.setParameters(config.disruptiveScript.parameters);

      // create a thread pool for running its disrupt method
      disruptionThreadPool = (ThreadPoolExecutor) Executors.newFixedThreadPool(1);

      DisruptiveThread disruptiveThread = new DisruptiveThread(disruptiveScript, config.testUsers);
      disruptionThreadPool.submit(disruptiveThread);
      logger.debug("Successfully submitted disruptive thread.");
    }

    // set the start time for this test run
    startTime = System.currentTimeMillis();

    // for each test script
    logger.info(
        "Test Scripts: Creating a thread pool for each TestScript and kicking off the user journeys");
    for (int tsCtr = 0; tsCtr < scripts.size(); tsCtr++) {
      TestScript testScript = scripts.get(tsCtr);
      TestScriptSpecification testScriptSpecification = config.testScripts.get(tsCtr);

      // create a thread pool for running its user journeys
      ThreadPoolExecutor threadPool =
          (ThreadPoolExecutor)
              Executors.newFixedThreadPool(testScriptSpecification.numberToRunInParallel);
      threadPools.add(threadPool);

      // kick off the user journey(s), one per thread
      List<Future<UserJourneyResult>> userJourneyFutures = new ArrayList<>();
      for (int ujCtr = 0; ujCtr < testScriptSpecification.totalNumberToRun; ujCtr++) {
        TestUserSpecification testUser = config.testUsers.get(ujCtr % config.testUsers.size());
        // add a description to the user journey threads/results that includes any test script
        // parameters
        Future<UserJourneyResult> userJourneyFuture =
            threadPool.submit(
                new UserJourneyThread(testScript, testScriptSpecification.description, testUser));
        userJourneyFutures.add(userJourneyFuture);
      }

      // TODO: support different patterns of kicking off user journeys. here they're all queued at
      // once
      userJourneyFutureLists.add(userJourneyFutures);
    }

    // wait until all threads either finish or time out
    logger.info("Test Scripts: Waiting until all threads either finish or time out");
    for (int ctr = 0; ctr < scripts.size(); ctr++) {
      TestScriptSpecification testScriptSpecification = config.testScripts.get(ctr);
      ThreadPoolExecutor threadPool = threadPools.get(ctr);

      threadPool.shutdown();
      long totalTerminationTime =
          testScriptSpecification.expectedTimeForEach * testScriptSpecification.totalNumberToRun;
      boolean terminatedByItself =
          threadPool.awaitTermination(
              totalTerminationTime, testScriptSpecification.expectedTimeForEachUnitObj);

      // if the threads didn't finish in the expected time, then send them interrupts
      if (!terminatedByItself) {
        threadPool.shutdownNow();
      }
      if (!threadPool.awaitTermination(secondsToWaitForPoolShutdown, TimeUnit.SECONDS)) {
        logger.error(
            "Test Scripts: Thread pool for test script failed to terminate: {}",
            testScriptSpecification.description);
      }
    }

    // set the end time for this test run
    endTime = System.currentTimeMillis();

    // shutdown the disrupt thread pool
    if (disruptionThreadPool != null) {
      logger.debug("Force shut down of the disruption thread pool");
      disruptionThreadPool.shutdownNow();
      if (!disruptionThreadPool.awaitTermination(secondsToWaitForPoolShutdown, TimeUnit.SECONDS)) {
        logger.error("Disruption Script: Thread pool for disruption script failed to terminate");
      }
    }

    // compile the results from all thread pools
    logger.info("Test Scripts: Compiling the results from all thread pools");
    for (int ctr = 0; ctr < scripts.size(); ctr++) {
      List<Future<UserJourneyResult>> userJourneyFutureList = userJourneyFutureLists.get(ctr);
      TestScriptSpecification testScriptSpecification = config.testScripts.get(ctr);

      List<UserJourneyResult> userJourneyResults = new ArrayList<>();
      for (Future<UserJourneyResult> userJourneyFuture : userJourneyFutureList) {
        UserJourneyResult result = null;
        if (userJourneyFuture.isDone())
          try {
            // user journey thread completed and populated its own return object, which may include
            // an exception
            result = userJourneyFuture.get();
            result.completed = true;
          } catch (ExecutionException execEx) {
            // user journey thread threw an exception and didn't populate its own return object
            result = new UserJourneyResult(testScriptSpecification.name, "");
            result.completed = false;
            result.exceptionThrown = execEx;
          }
        else {
          // user journey either was never started or got cancelled before it finished
          result = new UserJourneyResult(testScriptSpecification.name, "");
          result.completed = false;
        }
        userJourneyResults.add(result);
      }
      testScriptResults.add(new TestScriptResult(testScriptSpecification, userJourneyResults));
    }

    // call the cleanup method of each test script
    logger.info("Test Scripts: Calling the cleanup methods");
    Exception cleanupExceptionThrown = callTestScriptCleanups();
    if (cleanupExceptionThrown != null) {
      logger.error(
          "Test Scripts: Error calling test script cleanup methods", cleanupExceptionThrown);
      throw new RuntimeException(
          "Error calling test script cleanup methods.", cleanupExceptionThrown);
    }

    // no need to restore any Kubernetes settings. they are always set again at the beginning of a
    // test run, which is more important from a reproducibility standpoint. probably more useful to
    // leave the deployment as is, for debugging after a test run
    if (!config.server.skipDeployment) {
      deploymentScript.teardown();
    } else {
      logger.info("Deployment: Skipping deployment teardown");
    }

    // cleanup data project
    logger.info("Data Project: Cleaning up data project");
    cleanupDataProject();
  }

  /**
   * Call the setup() method of each TestScript class. If one of the classes throws an exception,
   * stop looping through the remaining setup methods and return the exception.
   *
   * @return the exception thrown, null if none
   */
  Exception callTestScriptSetups() {
    for (TestScript testScript : scripts) {
      try {
        testScript.setup(config.testUsers);
      } catch (Exception setupEx) {
        // return the first exception thrown and stop looping through the setup methods
        return setupEx;
      }
    }
    return null;
  }

  /**
   * Call the cleanup() method of each TestScript class. If any of the classes throws an exception,
   * keep looping through the remaining cleanup methods before returning. Save the first exception
   * thrown and return it.
   *
   * @return the first exception thrown, null if none
   */
  Exception callTestScriptCleanups() {
    Exception exceptionThrown = null;
    for (TestScript testScript : scripts) {
      try {
        testScript.cleanup(config.testUsers);
      } catch (Exception cleanupEx) {
        // save the first exception thrown, keep looping through the remaining cleanup methods
        // before returning
        if (exceptionThrown == null) {
          exceptionThrown = cleanupEx;
        }
      }
    }
    return exceptionThrown;
  }

  static class UserJourneyThread implements Callable<UserJourneyResult> {
    TestScript testScript;
    String userJourneyDescription;
    TestUserSpecification testUser;

    public UserJourneyThread(
        TestScript testScript, String userJourneyDescription, TestUserSpecification testUser) {
      this.testScript = testScript;
      this.userJourneyDescription = userJourneyDescription;
      this.testUser = testUser;
    }

    public UserJourneyResult call() {
      UserJourneyResult result =
          new UserJourneyResult(userJourneyDescription, Thread.currentThread().getName());

      long startTime = System.nanoTime();
      try {
        testScript.userJourney(testUser);
      } catch (Exception ex) {
        result.exceptionThrown = ex;
      }
      result.elapsedTimeNS = System.nanoTime() - startTime;

      return result;
    }
  }

  static class DisruptiveThread implements Callable {
    DisruptiveScript disruptiveScript;
    List<TestUserSpecification> testUsers;

    public DisruptiveThread(
        DisruptiveScript disruptiveScript, List<TestUserSpecification> testUsers) {
      this.disruptiveScript = disruptiveScript;
      this.testUsers = testUsers;
    }

    public Object call() {
      try {
        disruptiveScript.disrupt(testUsers);
      } catch (Exception ex) {
        logger.info("Disruptive thread threw exception: {}", ex.getMessage());
      }
      return null;
    }
  }

  void modifyKubernetesPostDeployment() throws Exception {
    logger.info(
        "Kubernetes: Setting the initial number of pods in the API deployment replica set to {}",
        config.kubernetes.numberOfInitialPods);
    KubernetesClientUtils.changeReplicaSetSizeAndWait(config.kubernetes.numberOfInitialPods);
  }

  void cleanupDataProject() {
    // TODO: cleanup any cloud resources/permissions generated by the test
    // no need to cleanup any DR Manager metadata because each test run re-deploys with a clean
    // database, but reporting that it was left hanging around would be helpful
  }

  void collectMetrics() throws Exception {
    // don't try to collect metrics for a server running locally
    if (config.server.skipKubernetes) {
      logger.info(
          "Skipping metrics collection because the server specification has disabled Kubernetes (see server.skipKubernetes flag).");
      return;
    }

    // TODO: these should come from the test configuration instead
    // build a list of measurements to collect
    measurementCollectionScripts.add(new CPUCoreUsageTimeMetric()); // container cpu usage
    measurementCollectionScripts.add(new MemoryUsedBytesMetric()); // container memory usage

    PerformanceLoggerElapsedTime perfloggerScript = new PerformanceLoggerElapsedTime();
    perfloggerScript.setParameters(
        Arrays.asList(
            "bio.terra.service.resourcemanagement.ResourcesApiController", "enumerateProfiles"));
    measurementCollectionScripts.add(perfloggerScript);

    LoggerInterceptorHttpStatus httpStatusScript = new LoggerInterceptorHttpStatus();
    httpStatusScript.setParameters(Arrays.asList("500"));
    measurementCollectionScripts.add(httpStatusScript);

    // loop through the measurement scripts, downloading raw data points and processing them
    for (MeasurementCollectionScript measurementCollectionScript : measurementCollectionScripts) {
      measurementCollectionScript.setServer(config.server);
      measurementCollectionScript.downloadDataPoints(startTime, endTime);
      measurementCollectionScript.calculateSummaryStatistics();
    }
  }

  void printResults() {
    try {
      // TODO: print the full set of results to a file instead of to debug

      // use Jackson to map the object to a JSON-formatted text block
      ObjectMapper objectMapper = new ObjectMapper();

      // print the full set of user journey results to debug
      logger.debug(
          objectMapper.writerWithDefaultPrettyPrinter().writeValueAsString(testScriptResults));

      // print the summaries to info
      List<TestScriptResult.TestScriptResultSummary> testScriptResultSummaries =
          testScriptResults.stream().map(TestScriptResult::getSummary).collect(Collectors.toList());
      logger.info(
          objectMapper
              .writerWithDefaultPrettyPrinter()
              .writeValueAsString(testScriptResultSummaries));

      // print the full set of measurement data points to debug
      measurementCollectionScripts.stream()
          .forEach(
              mcs ->
                  logger.debug(
                      "Measurement {}: {}",
                      mcs.getSummaryStatistics().description,
                      mcs.writeDataPointsToString()));

      // print the metrics summary to info
      List<MeasurementCollectionScript.MeasurementResultSummary> measurementResultSummaries =
          measurementCollectionScripts.stream()
              .map(MeasurementCollectionScript::getSummaryStatistics)
              .collect(Collectors.toList());
      logger.info(
          objectMapper
              .writerWithDefaultPrettyPrinter()
              .writeValueAsString(measurementResultSummaries));

      // TODO: point user to dashboards with the correct time range
    } catch (JsonProcessingException jpEx) {
      throw new RuntimeException("Error converting object to a JSON-formatted string", jpEx);
    }
  }

  static void printHelp() throws IOException {
    logger.info("Specify test configuration file as first argument.");
    logger.info("  e.g. ./gradlew run --args=\"configs/basicexamples/BasicUnauthenticated.json\"");
    logger.info("  e.g. ./gradlew run --args=\"suites/BasicSmoke.json\"");

    // print out the available test configurations found in the resources directory
    logger.info("The following test configuration files were found:");
    List<String> availableTestConfigs =
        FileUtils.getResourcesInDirectory(TestConfiguration.resourceDirectory);
    for (String testConfigFilePath : availableTestConfigs) {
      logger.info("  {}", testConfigFilePath);
    }

    // print out the available test suites found in the resources directory
    logger.info("The following test suite files were found:");
    List<String> availableTestSuites =
        FileUtils.getResourcesInDirectory(TestSuite.resourceDirectory);
    for (String testSuiteFilePath : availableTestSuites) {
      logger.info("  {}", testSuiteFilePath);
    }
  }

  public static void main(String[] args) throws Exception {
    // if no args specified, print help
    if (args.length < 1) {
      printHelp();
      return;
    }

    logger.info("==== READING IN TEST SUITE/CONFIGURATION(S) ====");
    // read in test suite and validate it
    TestSuite testSuite;
    boolean isSuite = args[0].startsWith(TestSuite.resourceDirectory + "/");
    boolean isSingleConfig = args[0].startsWith(TestConfiguration.resourceDirectory + "/");
    if (isSuite) {
      testSuite = TestSuite.fromJSONFile(args[0].split(TestSuite.resourceDirectory + "/")[1]);
      logger.info("Found a test suite: {}", testSuite.name);
    } else if (isSingleConfig) {
      TestConfiguration testConfiguration =
          TestConfiguration.fromJSONFile(
              args[0].split(TestConfiguration.resourceDirectory + "/")[1]);
      testSuite = TestSuite.fromSingleTestConfiguration(testConfiguration);
      logger.info("Found a single test configuration: {}", testConfiguration.name);
    } else {
      throw new RuntimeException("Invalid file reference to test suite or configuration.");
    }
    testSuite.validate();

    for (int ctr = 0; ctr < testSuite.testConfigurations.size(); ctr++) {
      TestConfiguration testConfiguration = testSuite.testConfigurations.get(ctr);
      logger.info(
          "==== EXECUTING TEST CONFIGURATION ({}) {} ====", ctr + 1, testConfiguration.name);
      logger.info(testConfiguration.display());

      // get an instance of a runner and tell it to execute the configuration and collect metrics
      TestRunner runner = new TestRunner(testConfiguration);
      Exception runnerEx = null;
      try {
        runner.executeTestConfiguration();
        runner.collectMetrics();
      } catch (Exception ex) {
        runnerEx = ex; // save exception to display after printing the results
      }

      logger.info("==== TEST RUN RESULTS ({}) {} ====", ctr + 1, testConfiguration.name);
      runner.printResults();

      if (runnerEx != null) {
        logger.error("Test Runner threw an exception", runnerEx);
      }
    }
  }
}
