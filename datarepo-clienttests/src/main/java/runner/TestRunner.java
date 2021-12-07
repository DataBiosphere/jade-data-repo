package runner;

import bio.terra.datarepo.model.CloudPlatform;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import common.utils.FileUtils;
import common.utils.KubernetesClientUtils;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Optional;
import java.util.TimeZone;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import runner.config.TestConfiguration;
import runner.config.TestScriptSpecification;
import runner.config.TestSuite;
import runner.config.TestUserSpecification;

public class TestRunner {
  private static final Logger logger = LoggerFactory.getLogger(TestRunner.class);

  private TestConfiguration config;
  private List<TestScript> scripts;
  private DeploymentScript deploymentScript;
  private List<ThreadPoolExecutor> threadPools;
  private ThreadPoolExecutor disruptionThreadPool;
  private List<List<Future<UserJourneyResult>>> userJourneyFutureLists;

  private List<TestScriptResult> testScriptResults;
  protected TestRunSummary summary;

  private static long secondsToWaitForPoolShutdown = 60;

  public static class TestRunSummary {
    public String id;

    public long startTime = -1;
    public long startUserJourneyTime = -1;
    public long endUserJourneyTime = -1;
    public long endTime = -1;
    public List<TestScriptResult.TestScriptResultSummary> testScriptResultSummaries;

    public TestRunSummary() {}

    public TestRunSummary(String id) {
      this.id = id;
    }

    private String startTimestamp;
    private String startUserJourneyTimestamp;
    private String endUserJourneyTimestamp;
    private String endTimestamp;

    public String getStartTimestamp() {
      return millisecondsToTimestampString(startTime);
    }

    public String getStartUserJourneyTimestamp() {
      return millisecondsToTimestampString(startUserJourneyTime);
    }

    public String getEndUserJourneyTimestamp() {
      return millisecondsToTimestampString(endUserJourneyTime);
    }

    public String getEndTimestamp() {
      return millisecondsToTimestampString(endTime);
    }

    private static String millisecondsToTimestampString(long milliseconds) {
      DateFormat dateFormat =
          new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSSSSS'Z'"); // Quoted Z to indicate UTC
      dateFormat.setTimeZone(TimeZone.getTimeZone("UTC"));
      return dateFormat.format(new Date(milliseconds));
    }
  }

  protected TestRunner(TestConfiguration config) {
    this.config = config;
    this.scripts = new ArrayList<>();
    this.threadPools = new ArrayList<>();
    this.disruptionThreadPool = null;
    this.userJourneyFutureLists = new ArrayList<>();
    this.testScriptResults = new ArrayList<>();

    this.summary = new TestRunSummary(UUID.randomUUID().toString());
  }

  protected void executeTestConfiguration() throws Exception {
    try {
      // set the start time for this test run
      summary.startTime = System.currentTimeMillis();

      executeTestConfigurationNoGuaranteedCleanup();

      // set the end time for this test run
      summary.endTime = System.currentTimeMillis();
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

      throw originalEx;
    }
  }

  private void executeTestConfigurationNoGuaranteedCleanup() throws Exception {
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

      testScriptInstance.setCloudPlatform(
          Optional.ofNullable(CloudPlatform.fromValue(config.cloudPlatform))
              .orElse(CloudPlatform.GCP));

      // set the billing account for the test script to use
      testScriptInstance.setBillingAccount(config.billingAccount);

      // set Azure billing profile info
      testScriptInstance.setTenantId(config.tenantId);
      testScriptInstance.setSubscriptionId(config.subscriptionId);
      testScriptInstance.setResourceGroupName(config.resourceGroupName);
      testScriptInstance.setApplicationDeploymentName(config.applicationDeploymentName);

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
      DisruptiveScript disruptiveScriptInstance =
          config.disruptiveScript.disruptiveScriptClassInstance();
      disruptiveScriptInstance.setBillingAccount(config.billingAccount);
      disruptiveScriptInstance.setServer(config.server);
      disruptiveScriptInstance.setParameters(config.disruptiveScript.parameters);

      // create a thread pool for running its disrupt method
      disruptionThreadPool = (ThreadPoolExecutor) Executors.newFixedThreadPool(1);

      DisruptiveThread disruptiveThread =
          new DisruptiveThread(disruptiveScriptInstance, config.testUsers);
      disruptionThreadPool.execute(disruptiveThread);
      logger.debug("Successfully submitted disruptive thread.");
      TimeUnit.SECONDS.sleep(
          1); // give the disruptive script thread some time to start before kicking off the user
      // journeys.
    }

    // set the start time for the user journey portion this test run
    summary.startUserJourneyTime = System.currentTimeMillis();

    // for each test script
    logger.info(
        "Test Scripts: Creating a thread pool for each TestScript and kicking off the user journeys");
    for (int tsCtr = 0; tsCtr < scripts.size(); tsCtr++) {
      TestScript testScript = scripts.get(tsCtr);
      TestScriptSpecification testScriptSpecification = config.testScripts.get(tsCtr);

      // create a thread pool for running its user journeys
      ThreadPoolExecutor threadPool =
          (ThreadPoolExecutor)
              Executors.newFixedThreadPool(testScriptSpecification.userJourneyThreadPoolSize);
      threadPools.add(threadPool);

      // kick off the user journey(s), one per thread
      List<Future<UserJourneyResult>> userJourneyFutures = new ArrayList<>();
      for (int ujCtr = 0;
          ujCtr < testScriptSpecification.numberOfUserJourneyThreadsToRun;
          ujCtr++) {
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
          testScriptSpecification.expectedTimeForEach
              * testScriptSpecification.numberOfUserJourneyThreadsToRun;
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

    // set the end time for the user journey portion this test run
    summary.endUserJourneyTime = System.currentTimeMillis();

    // shutdown the disrupt thread pool
    if (disruptionThreadPool != null) {
      logger.debug("Tell the disruption thread pool to shutdown");
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

    // pull out the test script summary information into the summary object
    summary.testScriptResultSummaries =
        testScriptResults.stream().map(TestScriptResult::getSummary).collect(Collectors.toList());

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
  }

  /**
   * Call the setup() method of each TestScript class. If one of the classes throws an exception,
   * stop looping through the remaining setup methods and return the exception.
   *
   * @return the exception thrown, null if none
   */
  private Exception callTestScriptSetups() {
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
  private Exception callTestScriptCleanups() {
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

  private static class UserJourneyThread implements Callable<UserJourneyResult> {
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

  private static class DisruptiveThread implements Runnable {
    DisruptiveScript disruptiveScript;
    List<TestUserSpecification> testUsers;

    public DisruptiveThread(
        DisruptiveScript disruptiveScript, List<TestUserSpecification> testUsers) {
      this.disruptiveScript = disruptiveScript;
      this.testUsers = testUsers;
    }

    public void run() {
      try {
        disruptiveScript.disrupt(testUsers);
      } catch (Exception ex) {
        logger.info("Disruptive thread threw exception: {}", ex.getMessage());
      }
    }
  }

  private void modifyKubernetesPostDeployment() throws Exception {
    logger.info(
        "Kubernetes: Setting the initial number of pods in the API deployment replica set to {}",
        config.kubernetes.numberOfInitialPods);
    KubernetesClientUtils.changeReplicaSetSizeAndWait(config.kubernetes.numberOfInitialPods);
  }

  private static final String renderedConfigFileName = "RENDERED_testConfiguration.json";
  private static final String userJourneyResultsFileName = "RAWDATA_userJourneyResults.json";
  private static final String runSummaryFileName = "SUMMARY_testRun.json";

  protected void writeOutResults(String outputParentDirName) throws IOException {
    // use Jackson to map the object to a JSON-formatted text block
    ObjectMapper objectMapper = new ObjectMapper();
    ObjectWriter objectWriter = objectMapper.writerWithDefaultPrettyPrinter();

    // print the summary results to info
    logger.info(objectWriter.writeValueAsString(summary));

    // create the output directory if it doesn't already exist
    Path outputDirectory =
        Paths.get(outputParentDirName); // .resolve(config.name + "_" + summary.id);
    File outputDirectoryFile = outputDirectory.toFile();
    if (outputDirectoryFile.exists() && !outputDirectoryFile.isDirectory()) {
      throw new IllegalArgumentException(
          "Output directory already exists as a file: " + outputDirectoryFile.getAbsolutePath());
    }
    boolean outputDirectoryCreated = outputDirectoryFile.mkdirs();
    logger.debug(
        "outputDirectoryCreated {}: {}",
        outputDirectoryFile.getAbsolutePath(),
        outputDirectoryCreated);
    logger.info("Test run results written to directory: {}", outputDirectoryFile.getAbsolutePath());

    // create the output files if they don't already exist
    File renderedConfigFile = outputDirectory.resolve(renderedConfigFileName).toFile();
    File userJourneyResultsFile =
        FileUtils.createNewFile(outputDirectory.resolve(userJourneyResultsFileName).toFile());
    File runSummaryFile = outputDirectory.resolve(runSummaryFileName).toFile();

    // write the rendered test configuration that was run to a file
    objectWriter.writeValue(renderedConfigFile, config);
    logger.info("Rendered test configuration written to file: {}", renderedConfigFile.getName());

    // write the full set of user journey results to a file
    objectWriter.writeValue(userJourneyResultsFile, testScriptResults);
    logger.info("All user journey results written to file: {}", userJourneyResultsFile.getName());

    // write the test run summary to a file
    objectWriter.writeValue(runSummaryFile, summary);
    logger.info("Test run summary written to file: {}", runSummaryFile.getName());
  }

  /**
   * Read in the rendered test configuration from the output directory and return the
   * TestConfiguration Java object.
   */
  public static TestConfiguration getRenderedTestConfiguration(Path outputDirectory)
      throws Exception {
    return FileUtils.readOutputFileIntoJavaObject(
        outputDirectory, TestRunner.renderedConfigFileName, TestConfiguration.class);
  }

  /**
   * Read in the test run summary from the output directory and return the TestRunner.TestRunSummary
   * Java object.
   */
  public static TestRunner.TestRunSummary getTestRunSummary(Path outputDirectory) throws Exception {
    return FileUtils.readOutputFileIntoJavaObject(
        outputDirectory, TestRunner.runSummaryFileName, TestRunner.TestRunSummary.class);
  }

  /**
   * Build a list of output directories that contain test run results. - For a single test config,
   * this is just the provided output directory - For a test suite, this is all the immediate
   * sub-directories of the provided output directory
   *
   * @return a list of test run output directories
   */
  public static List<Path> getTestRunOutputDirectories(Path outputDirectory) throws Exception {
    // check that the output directory exists
    if (!outputDirectory.toFile().exists()) {
      throw new FileNotFoundException(
          "Output directory not found: " + outputDirectory.toAbsolutePath());
    }

    // build a list of output directories that contain test run results
    List<Path> testRunOutputDirectories = new ArrayList<>();
    TestRunner.TestRunSummary testRunSummary = null;
    try {
      testRunSummary = getTestRunSummary(outputDirectory);
    } catch (Exception ex) {
    }

    if (testRunSummary != null) { // single test config
      testRunOutputDirectories.add(outputDirectory);
    } else { // test suite
      File[] subdirectories = outputDirectory.toFile().listFiles(File::isDirectory);
      if (subdirectories == null) {
        throw new RuntimeException("Unexpected output directory format, no test runs found.");
      }
      for (int ctr = 0; ctr < subdirectories.length; ctr++) {
        testRunOutputDirectories.add(subdirectories[ctr].toPath());
      }
    }
    return testRunOutputDirectories;
  }

  /** Returns a boolean indicating whether any test runs failed or not. */
  public static boolean runTest(String configFileName, String outputParentDirName)
      throws Exception {
    logger.info("==== READING IN TEST SUITE/CONFIGURATION(S) ====");
    // read in test suite and validate it
    TestSuite testSuite;
    boolean isSuite = configFileName.startsWith(TestSuite.resourceDirectory + "/");
    boolean isSingleConfig = configFileName.startsWith(TestConfiguration.resourceDirectory + "/");
    if (isSuite) {
      testSuite =
          TestSuite.fromJSONFile(configFileName.split(TestSuite.resourceDirectory + "/")[1]);
      logger.info("Found a test suite: {}", testSuite.name);
    } else if (isSingleConfig) {
      TestConfiguration testConfiguration =
          TestConfiguration.fromJSONFile(
              configFileName.split(TestConfiguration.resourceDirectory + "/")[1]);
      testSuite = TestSuite.fromSingleTestConfiguration(testConfiguration);
      logger.info("Found a single test configuration: {}", testConfiguration.name);
    } else {
      throw new RuntimeException(
          "File reference "
              + configFileName
              + " is not found as a test suite (in "
              + TestSuite.resourceDirectory
              + "/) or as a test config (in "
              + TestConfiguration.resourceDirectory
              + "/)");
    }
    testSuite.validate();

    boolean isFailure = false;
    for (int ctr = 0; ctr < testSuite.testConfigurations.size(); ctr++) {
      TestConfiguration testConfiguration = testSuite.testConfigurations.get(ctr);

      logger.info(
          "==== EXECUTING TEST CONFIGURATION ({}) {} ====", ctr + 1, testConfiguration.name);
      logger.info(testConfiguration.display());

      // get an instance of a runner and tell it to execute the configuration
      TestRunner runner = new TestRunner(testConfiguration);
      try {
        runner.executeTestConfiguration();

        // update the failure flag if it's not already been set
        if (!isFailure) {
          for (TestScriptResult.TestScriptResultSummary testScriptResultSummary :
              runner.summary.testScriptResultSummaries) {
            if (testScriptResultSummary.isFailure) {
              isFailure = true;
              break;
            }
          }
        }
      } catch (Exception runnerEx) {
        logger.error("Test Runner threw an exception", runnerEx);
        isFailure = true;
      }

      logger.info("==== TEST RUN RESULTS ({}) {} ====", ctr + 1, testConfiguration.name);
      String outputDirName =
          outputParentDirName; // if running a single config, put the results in the given directory
      if (isSuite) { // if running a suite, put each config results in a separate sub-directory
        outputDirName =
            Paths.get(outputParentDirName)
                .resolve(testConfiguration.name + "_" + runner.summary.id)
                .toAbsolutePath()
                .toString();
      }
      runner.writeOutResults(outputDirName);

      TimeUnit.SECONDS.sleep(5);
    }

    return isFailure;
  }
}
