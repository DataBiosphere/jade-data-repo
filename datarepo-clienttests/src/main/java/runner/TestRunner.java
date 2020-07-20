package runner;

import bio.terra.datarepo.client.ApiClient;
import com.google.auth.oauth2.AccessToken;
import com.google.auth.oauth2.GoogleCredentials;
import io.kubernetes.client.openapi.models.V1Deployment;
import io.kubernetes.client.openapi.models.V1Pod;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import runner.config.TestConfiguration;
import runner.config.TestScriptSpecification;
import runner.config.TestUserSpecification;
import utils.AuthenticationUtils;
import utils.FileUtils;
import utils.KubernetesClientUtils;

class TestRunner {

  private static final Logger LOG = LoggerFactory.getLogger(TestRunner.class);

  TestConfiguration config;
  List<TestScript> scripts;
  DeploymentScript deploymentScript;
  List<ThreadPoolExecutor> threadPools;
  List<List<Future<UserJourneyResult>>> userJourneyFutureLists;
  List<UserJourneyResult> userJourneyResults;
  Map<String, ApiClient> apiClientsForUsers; // testUser -> apiClient
  // TODO: have ApiClients share an HTTP client, or one per each is ok?

  static long secondsToWaitForPoolShutdown = 60;

  TestRunner(TestConfiguration config) {
    this.config = config;
    this.scripts = new ArrayList<>();
    this.threadPools = new ArrayList<>();
    this.userJourneyFutureLists = new ArrayList<>();
    this.userJourneyResults = new ArrayList<>();
    this.apiClientsForUsers = new HashMap<>();
  }

  void executeTestConfiguration() throws Exception {
    try {
      executeTestConfigurationNoGuaranteedCleanup();
    } catch (Exception originalEx) {
      LOG.info("Cleanup After Failure =========================================================");

      // cleanup deployment (i.e. run teardown method)
      LOG.info("Deployment: Calling {}.teardown()", deploymentScript.getClass().getName());
      try {
        if (!config.server.skipDeployment) {
          deploymentScript.teardown();
        }
      } catch (Exception deploymentTeardownEx) {
        LOG.error("Deployment: Exception during forced deployment teardown", deploymentTeardownEx);
      }

      // cleanup test scripts (i.e. run cleanup methods)
      LOG.info("Test Scripts: Calling the cleanup methods");
      try {
        callTestScriptCleanups();
      } catch (Exception testScriptCleanupEx) {
        LOG.error(
            "Test Scripts: Exception during forced test script cleanups", testScriptCleanupEx);
      }

      // cleanup data project (i.e. run cloud cleanup scripts)
      LOG.info("Data Project: Cleaning up the data project");
      try {
        cleanupDataProject();
      } catch (Exception leftoverTestDataEx) {
        LOG.error("Data Project: Exception during forced data project cleanup", leftoverTestDataEx);
      }

      throw originalEx;
    }
  }

  void executeTestConfigurationNoGuaranteedCleanup() throws Exception {
    LOG.info("Deployment =========================================================");

    // specify any value overrides in the Helm chart, then deploy
    if (!config.server.skipDeployment) {
      // get an instance of the deployment script class
      try {
        deploymentScript = config.server.deploymentScript.scriptClass.newInstance();
      } catch (IllegalAccessException | InstantiationException niEx) {
        LOG.error(
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
      LOG.info("Deployment: Calling {}.deploy()", deploymentScript.getClass().getName());
      deploymentScript.deploy(config.server, config.application);

      LOG.info(
          "Deployment: Calling {}.waitForDeployToFinish()", deploymentScript.getClass().getName());
      deploymentScript.waitForDeployToFinish();
    } else {
      LOG.info("Deployment: Skipping deployment");
    }

    LOG.info("Kubernetes =========================================================");

    // update any Kubernetes properties specified by the test configuration
    if (!config.server.skipKubernetes) {
      KubernetesClientUtils.buildKubernetesClientObject(config.server);
      modifyKubernetesPostDeployment();
    } else {
      LOG.info("Kubernetes: Skipping Kubernetes configuration post-deployment");
    }

    LOG.info("Test Users =========================================================");

    // get an instance of the API client per test user
    LOG.info("Test Users: Fetching credentials and building ApiClient objects");
    for (TestUserSpecification testUser : config.testUsers) {
      ApiClient apiClient = new ApiClient();
      apiClient.setBasePath(config.server.uri);
      GoogleCredentials userCredential = AuthenticationUtils.getDelegatedUserCredential(testUser);
      AccessToken userAccessToken = AuthenticationUtils.getAccessToken(userCredential);
      apiClient.setAccessToken(userAccessToken.getTokenValue());

      apiClientsForUsers.put(testUser.name, apiClient);
    }

    LOG.info("Test Scripts =========================================================");

    // get an instance of each test script class
    LOG.info(
        "Test Scripts: Fetching instance of each class, setting billing account and parameters");
    for (TestScriptSpecification testScriptSpecification : config.testScripts) {
      try {
        TestScript testScriptInstance = testScriptSpecification.scriptClass.newInstance();

        // set the billing account for the test script to use
        testScriptInstance.setBillingAccount(config.billingAccount);

        // set any parameters specified by the configuration
        testScriptInstance.setParameters(testScriptSpecification.parameters);

        scripts.add(testScriptInstance);
      } catch (IllegalAccessException | InstantiationException niEx) {
        LOG.error(
            "Test Scripts: Error calling constructor of TestScript class: {}",
            testScriptSpecification.name,
            niEx);
        throw new IllegalArgumentException(
            "Error calling constructor of TestScript class: " + testScriptSpecification.name, niEx);
      }
    }

    // call the setup method of each test script
    LOG.info("Test Scripts: Calling the setup methods");
    Exception setupExceptionThrown = callTestScriptSetups();
    if (setupExceptionThrown != null) {
      LOG.error("Test Scripts: Error calling test script setup methods", setupExceptionThrown);
      throw new RuntimeException("Error calling test script setup methods.", setupExceptionThrown);
    }

    // for each test script
    LOG.info(
        "Test Scripts: Creating a thread pool for each TestScript and kicking off the user journeys");
    List<ApiClient> apiClientList = new ArrayList<>(apiClientsForUsers.values());
    for (int tsCtr = 0; tsCtr < scripts.size(); tsCtr++) {
      TestScript testScript = scripts.get(tsCtr);
      TestScriptSpecification testScriptSpecification = config.testScripts.get(tsCtr);

      // add a description to the user journey threads/results that includes any parameters
      String userJourneyDescription = testScriptSpecification.name;
      if (testScriptSpecification.parameters != null) {
        userJourneyDescription += ": " + String.join(",", testScriptSpecification.parameters);
      }

      // create a thread pool for running its user journeys
      ThreadPoolExecutor threadPool =
          (ThreadPoolExecutor)
              Executors.newFixedThreadPool(testScriptSpecification.numberToRunInParallel);
      threadPools.add(threadPool);

      // kick off the user journey(s), one per thread
      List<UserJourneyThread> userJourneyThreads = new ArrayList<>();
      for (int ujCtr = 0; ujCtr < testScriptSpecification.totalNumberToRun; ujCtr++) {
        ApiClient apiClient = apiClientList.get(ujCtr % apiClientList.size());
        userJourneyThreads.add(
            new UserJourneyThread(testScript, userJourneyDescription, apiClient));
      }

      // TODO: support different patterns of kicking off user journeys. here they're all queued at
      // once
      List<Future<UserJourneyResult>> userJourneyFutures = threadPool.invokeAll(userJourneyThreads);
      userJourneyFutureLists.add(userJourneyFutures);
    }

    // wait until all threads either finish or time out
    LOG.info("Test Scripts: Waiting until all threads either finish or time out");
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
        LOG.error(
            "Test Scripts: Thread pool for test script {} with parameters {} failed to terminate",
            testScriptSpecification.name,
            testScriptSpecification.parameters == null
                ? ""
                : String.join(",", testScriptSpecification.parameters));
      }
    }

    // compile the results from all thread pools
    LOG.info("Test Scripts: Compiling the results from all thread pools");
    for (int ctr = 0; ctr < scripts.size(); ctr++) {
      List<Future<UserJourneyResult>> userJourneyFutureList = userJourneyFutureLists.get(ctr);
      TestScriptSpecification testScriptSpecification = config.testScripts.get(ctr);

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
    }

    // call the cleanup method of each test script
    LOG.info("Test Scripts: Calling the cleanup methods");
    Exception cleanupExceptionThrown = callTestScriptCleanups();
    if (cleanupExceptionThrown != null) {
      LOG.error("Test Scripts: Error calling test script cleanup methods", cleanupExceptionThrown);
      throw new RuntimeException(
          "Error calling test script cleanup methods.", cleanupExceptionThrown);
    }

    LOG.info("Deployment =========================================================");

    // TODO: also restore any Kubernetes settings? they are always set again at the beginning of a
    // test run, which is more important from a reproducibility standpoint. might be useful to leave
    // the deployment as is, for debugging after a test run
    if (!config.server.skipDeployment) {
      deploymentScript.teardown();
    } else {
      LOG.info("Deployment: Skipping deployment teardown");
    }

    LOG.info("Data Project =========================================================");

    // cleanup data project
    LOG.info("Cleaning up data project");
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
        testScript.setup(apiClientsForUsers);
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
        testScript.cleanup(apiClientsForUsers);
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
    ApiClient apiClient;

    public UserJourneyThread(
        TestScript testScript, String userJourneyDescription, ApiClient apiClient) {
      this.testScript = testScript;
      this.userJourneyDescription = userJourneyDescription;
      this.apiClient = apiClient;
    }

    public UserJourneyResult call() {
      UserJourneyResult result =
          new UserJourneyResult(userJourneyDescription, Thread.currentThread().getName());

      long startTime = System.nanoTime();
      try {
        testScript.userJourney(apiClient);
      } catch (Exception ex) {
        result.exceptionThrown = ex;
      }
      result.elapsedTime = System.nanoTime() - startTime;

      return result;
    }
  }

  void modifyKubernetesPostDeployment() throws Exception {
    // set the initial number of pods in the API deployment replica set
    V1Deployment apiDeployment = KubernetesClientUtils.getApiDeployment(config.server.namespace);
    if (apiDeployment == null) {
      LOG.error("Kubernetes: API deployment not found");
      throw new RuntimeException("API deployment not found.");
    }
    LOG.info(
        "Kubernetes: Pod count before set initial replica set size: {}",
        KubernetesClientUtils.listPods(config.server.namespace).size());
    apiDeployment =
        KubernetesClientUtils.changeReplicaSetSize(
            apiDeployment, config.kubernetes.numberOfInitialPods);
    KubernetesClientUtils.waitForReplicaSetSizeChange(
        apiDeployment, config.kubernetes.numberOfInitialPods);

    // print out the current pods
    List<V1Pod> pods = KubernetesClientUtils.listPods(config.server.namespace);
    LOG.info("Kubernetes: Initial number of pods: {}", pods.size());
    for (V1Pod pod : pods) {
      LOG.debug("  pod: {}", pod.getMetadata().getName());
    }
  }

  void cleanupDataProject() {
    // TODO: cleanup any cloud resources/permissions generated by the test
    // no need to cleanup any DR Manager metadata because each test run re-deploys with a clean
    // database, but reporting that it was left hanging around would be helpful
  }

  void calculateResultStatistics() {
    // TODO: calculate mean/median response time for all completed user journey threads, group by
    // userjourney description
  }

  static void printHelp() {
    LOG.info("Specify test configuration file as first argument.");
    LOG.info("  e.g. ./gradlew :run --args=\"BasicUnauthenticated.json\"");
    LOG.info("The following test configuration files were found:");

    // print out the available test configuration found in the resources directory
    List<String> availableTestConfigs =
        FileUtils.getResourcesInDirectory(TestConfiguration.resourceDirectory + "/");
    for (String testConfigFileName : availableTestConfigs) {
      LOG.info("  {}", testConfigFileName);
    }
  }

  public static void main(String[] args) throws Exception {
    // if no args specified, print help
    if (args.length < 1) {
      printHelp();
      return;
    }

    LOG.info("TEST CONFIGURATION =========================================================");

    // read in configuration, validate it, and print to stdout
    TestConfiguration testConfiguration = TestConfiguration.fromJSONFile(args[0]);
    testConfiguration.validate();
    testConfiguration.display();

    LOG.info("TEST RUN =========================================================");

    // get an instance of a runner and tell it to execute the configuration
    TestRunner runner = new TestRunner(testConfiguration);
    Exception runnerEx = null;
    try {
      runner.executeTestConfiguration();
    } catch (Exception ex) {
      runnerEx = ex; // save exception to display after printing the results
    }

    LOG.info("USER JOURNEY RESULTS =========================================================");

    // print the results to stdout
    for (UserJourneyResult result : runner.userJourneyResults) {
      result.display();
    }
    if (runnerEx != null) {
        LOG.error("Test Runner threw an exception", runnerEx);
    }

    // calculate any relevant statistics about the user journeys and print them out
    runner.calculateResultStatistics();
  }
}
